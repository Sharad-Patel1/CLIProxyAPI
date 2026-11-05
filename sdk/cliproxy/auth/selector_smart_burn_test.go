package auth

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func smartBurnClaude(id string, now time.Time, weekly, short time.Duration) *Auth {
	signals := make(map[string]string)
	for name, delta := range map[string]time.Duration{"7d": weekly, "5h": short} {
		if delta != 0 {
			signals[http.CanonicalHeaderKey("Anthropic-Ratelimit-Unified-"+name+"-Reset")] = strconv.FormatInt(now.Add(delta).Unix(), 10)
		}
	}
	return &Auth{ID: id, Provider: "claude", Quota: QuotaState{ObservedAt: now, Signals: signals}}
}

func TestSmartBurnWeeklyResetBeforeShortWindow(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	later := smartBurnClaude("a-later", now, 4*24*time.Hour, time.Hour)
	sooner := smartBurnClaude("z-sooner", now, 12*time.Hour, 4*time.Hour)
	selector := &SmartBurnSelector{}
	for range 3 {
		got, err := selector.Pick(context.Background(), "claude", "opus", cliproxyexecutor.Options{}, []*Auth{later, sooner})
		if err != nil || got.ID != sooner.ID {
			t.Fatalf("pick = %v, %v; want the account resetting in 12h", got, err)
		}
	}
	// Expired weekly data cannot win; use that account's still-live short window.
	sooner.Quota = smartBurnClaude("", now, -time.Hour, 3*time.Hour).Quota
	if got := smartBurnReset(sooner, now); !got.Equal(now.Add(3 * time.Hour)) {
		t.Fatalf("reset = %v, want short-window fallback", got)
	}
}

func TestSmartBurnCodexWindows(t *testing.T) {
	now := time.Unix(1800000000, 0)
	for _, weekly := range []string{"Primary", "Secondary"} {
		t.Run(weekly, func(t *testing.T) {
			short := "Secondary"
			if weekly == short {
				short = "Primary"
			}
			auth := &Auth{Provider: "codex", Quota: QuotaState{ObservedAt: now.Add(-time.Hour), Signals: map[string]string{
				"X-Codex-" + weekly + "-Window-Minutes":      "10080",
				"X-Codex-" + weekly + "-Reset-After-Seconds": "46800",
				"X-Codex-" + short + "-Window-Minutes":       "300",
				"X-Codex-" + short + "-Reset-At":             strconv.FormatInt(now.Add(2*time.Hour).Unix(), 10),
				"X-Codex-Code-Review-Primary-Reset-At":       strconv.FormatInt(now.Add(time.Minute).Unix(), 10),
			}}}
			if got := smartBurnReset(auth, now); !got.Equal(now.Add(12 * time.Hour)) {
				t.Fatalf("reset = %v, want observation-anchored weekly reset", got)
			}
			auth.Quota.Signals["X-Codex-"+weekly+"-Reset-At"] = strconv.FormatInt(now.Add(-time.Minute).Unix(), 10)
			if got := smartBurnReset(auth, now); !got.Equal(now.Add(2 * time.Hour)) {
				t.Fatalf("reset = %v, expired absolute deadline was revived", got)
			}
		})
	}
	for _, bad := range []string{"", "NaN", "-1", "9223372036854775807", "1801000000", "1800000000"} {
		if got := smartBurnUnixReset(bad, now); !got.IsZero() {
			t.Errorf("reset(%q) = %v", bad, got)
		}
	}
}

func TestSmartBurnDiscoveryDoesNotStarveKnownAccounts(t *testing.T) {
	now := time.Unix(1800000000, 0)
	known := smartBurnClaude("known", now, time.Hour, 0)
	a := &Auth{ID: "unknown-a", Provider: "claude"}
	b := &Auth{ID: "unknown-b", Provider: "claude"}
	selector := &SmartBurnSelector{}
	for _, want := range []string{a.ID, b.ID, known.ID, known.ID} {
		got, err := selector.pick(context.Background(), "claude", "opus", cliproxyexecutor.Options{}, []*Auth{known, a, b}, now)
		if err != nil || got.ID != want {
			t.Fatalf("pick = %v, %v, want %s", got, err, want)
		}
	}
	got, err := selector.pick(context.Background(), "claude", "opus", cliproxyexecutor.Options{}, []*Auth{known, a, b}, now.Add(smartBurnDiscoveryInterval))
	if err != nil || got.ID != a.ID {
		t.Fatalf("expired discovery lease: %v, %v", got, err)
	}
	// Providers without supported quota observations retain round-robin behavior.
	xai := []*Auth{{ID: "xai-a", Provider: "xai"}, {ID: "xai-b", Provider: "xai"}}
	for _, want := range []string{"xai-a", "xai-b", "xai-a"} {
		got, err := selector.Pick(context.Background(), "xai", "grok", cliproxyexecutor.Options{}, xai)
		if err != nil || got.ID != want {
			t.Fatalf("fallback: %v, %v, want %s", got, err, want)
		}
	}
}

func TestSmartBurnEligibilityAndWebsockets(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	soon := smartBurnClaude("soon", now, time.Hour, 0)
	later := smartBurnClaude("later", now, 4*24*time.Hour, 0)
	selector := &SmartBurnSelector{}
	soon.Disabled = true
	got, err := selector.Pick(context.Background(), "claude", "opus", cliproxyexecutor.Options{}, []*Auth{soon, later})
	if err != nil || got.ID != later.ID {
		t.Fatalf("disabled account selected: %v, %v", got, err)
	}
	soon.Disabled = false
	soon.Quota.Exceeded, soon.Quota.Reason, soon.Quota.NextRecoverAt = true, "credential_quota", now.Add(time.Hour)
	got, err = selector.Pick(context.Background(), "claude", "opus", cliproxyexecutor.Options{}, []*Auth{soon, later})
	if err != nil || got.ID != later.ID {
		t.Fatalf("cooling account selected: %v, %v", got, err)
	}
	soon.Quota.Exceeded = false
	later.Attributes = map[string]string{"priority": "5"}
	got, err = selector.Pick(context.Background(), "claude", "opus", cliproxyexecutor.Options{}, []*Auth{soon, later})
	if err != nil || got.ID != later.ID {
		t.Fatalf("explicit priority ignored: %v, %v", got, err)
	}
	soon.Provider, later.Provider = "codex", "codex"
	later.Attributes = map[string]string{"websockets": "true"}
	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())
	got, err = selector.Pick(ctx, "codex", "gpt", cliproxyexecutor.Options{}, []*Auth{soon, later})
	if err != nil || got.ID != later.ID {
		t.Fatalf("websocket preference ignored: %v, %v", got, err)
	}
}

func TestSmartBurnAffinitySurvivesRankingChangeAndFailsOver(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	a := smartBurnClaude("a", now, 12*time.Hour, 0)
	b := smartBurnClaude("b", now, 4*24*time.Hour, 0)
	selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: &SmartBurnSelector{}, RebindOn: SessionAffinityRebindOnQuota})
	t.Cleanup(selector.Stop)
	pick := func(session, want string) {
		t.Helper()
		got, err := selector.Pick(context.Background(), "claude", "opus", cliproxyexecutor.Options{Headers: http.Header{"X-Session-Id": {session}}}, []*Auth{a, b})
		if err != nil || got.ID != want {
			t.Fatalf("session %s: %v, %v, want %s", session, got, err, want)
		}
	}
	pick("thread-1", "a")
	b.Quota = smartBurnClaude("", now, time.Hour, 0).Quota
	pick("thread-1", "a")
	pick("thread-2", "b")
	a.Quota.Exceeded, a.Quota.Reason, a.Quota.NextRecoverAt = true, "credential_quota", now.Add(time.Hour)
	pick("thread-1", "b")
	a.Quota.Exceeded = false
	pick("thread-1", "b")
}

func TestSmartBurnManagerObservesUpdatedQuotas(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	manager := NewManager(nil, &SmartBurnSelector{}, nil)
	manager.RegisterExecutor(&customStreamMockExecutor{identifier: "claude"})
	for _, id := range []string{"smart-a", "smart-b"} {
		registry.GetGlobalRegistry().RegisterClient(id, "claude", []*registry.ModelInfo{{ID: "model"}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
		a := smartBurnClaude(id, now, 4*24*time.Hour, 0)
		_, err := manager.Register(context.Background(), a)
		if err != nil {
			t.Fatal(err)
		}
	}
	// This exercises the manager's quota cloning path, not direct mutation of a
	// candidate handed to the selector.
	ctx := logging.WithResponseHeadersHolder(context.Background())
	logging.SetResponseHeaders(ctx, http.Header{
		"Anthropic-Ratelimit-Unified-7d-Reset": {strconv.FormatInt(now.Add(time.Hour).Unix(), 10)},
	})
	manager.MarkResult(ctx, Result{AuthID: "smart-b", Provider: "claude", Model: "model", Success: true})
	got, err := manager.SelectAuth(context.Background(), "claude", "model", cliproxyexecutor.Options{})
	if err != nil || got.ID != "smart-b" {
		t.Fatalf("updated quota not used: %v, %v", got, err)
	}
}

func TestSmartBurnConcurrentDiscovery(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	selector := &SmartBurnSelector{}
	auths := []*Auth{smartBurnClaude("known", now, time.Hour, 0), {ID: "unknown", Provider: "claude"}}
	var wg sync.WaitGroup
	results := make(chan string, 32)
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			a, err := selector.Pick(context.Background(), "claude", "opus", cliproxyexecutor.Options{}, auths)
			if err != nil {
				results <- fmt.Sprint(err)
			} else {
				results <- a.ID
			}
		}()
	}
	wg.Wait()
	close(results)
	discoveries := 0
	for id := range results {
		if id == "unknown" {
			discoveries++
		} else if id != "known" {
			t.Fatal(id)
		}
	}
	if discoveries != 1 {
		t.Fatalf("discovery selections = %d, want 1", discoveries)
	}
}
