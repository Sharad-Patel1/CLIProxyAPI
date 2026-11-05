package auth

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

const smartBurnDiscoveryInterval = 5 * time.Minute

// SmartBurnSelector spends the eligible account whose weekly quota resets first.
// Short windows are used only when that account has no future weekly reset.
// Wrap it in SessionAffinitySelector to apply this policy only to new bindings
// and genuine failovers. Availability and explicit priority remain authoritative.
type SmartBurnSelector struct {
	mu            sync.Mutex
	lastDiscovery map[string]time.Time
	fallback      RoundRobinSelector
}

func (s *SmartBurnSelector) Pick(ctx context.Context, provider, model string, opts cliproxyexecutor.Options, auths []*Auth) (*Auth, error) {
	now := time.Now()
	available, err := getSelectorAvailableAuths(ctx, auths, provider, model, now)
	if err != nil {
		return nil, err
	}
	available = preferCodexWebsocketAuths(ctx, provider, available)
	return s.pick(ctx, provider, model, opts, available, now)
}

func (s *SmartBurnSelector) pick(ctx context.Context, provider, model string, opts cliproxyexecutor.Options, available []*Auth, now time.Time) (*Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastDiscovery == nil {
		s.lastDiscovery = make(map[string]time.Time)
	}
	// Removed credentials must not leave unbounded state behind. Discovery leases
	// expire; clearing a full map only permits another ordinary request to explore.
	if len(s.lastDiscovery) >= 4096 {
		clear(s.lastDiscovery)
	}
	var best, discover *Auth
	var bestReset, oldestDiscovery time.Time
	for _, candidate := range available {
		reset := smartBurnReset(candidate, now)
		if !reset.IsZero() {
			if best == nil || reset.Before(bestReset) || (reset.Equal(bestReset) && candidate.ID < best.ID) {
				best, bestReset = candidate, reset
			}
			continue
		}
		if candidate.Provider != "claude" && candidate.Provider != "codex" {
			continue
		}
		last := s.lastDiscovery[candidate.ID]
		if candidate.Quota.ObservedAt.After(last) {
			last = candidate.Quota.ObservedAt
		}
		if now.Sub(last) >= smartBurnDiscoveryInterval &&
			(discover == nil || last.Before(oldestDiscovery) || (last.Equal(oldestDiscovery) && candidate.ID < discover.ID)) {
			discover, oldestDiscovery = candidate, last
		}
	}
	if discover != nil {
		// A cold account receives at most one discovery selection per interval,
		// even while its first response is in flight or carries no quota headers.
		// This sends a normal requested inference, never a background upstream call.
		s.lastDiscovery[discover.ID] = now
		return discover, nil
	}
	if best != nil {
		return best, nil
	}
	return s.fallback.Pick(ctx, provider, model, opts, available)
}

// smartBurnReset reads only the main account limits. Code-review and named
// model-specific limits must not change the ordering of unrelated model traffic.
// Signals are advisory ordering data; only the existing scheduler can block an
// account. In particular, a rounded utilization of 100 is not a new cooldown.
func smartBurnReset(auth *Auth, now time.Time) time.Time {
	q := auth.Quota
	signal := func(key string) string { return strings.TrimSpace(q.Signals[http.CanonicalHeaderKey(key)]) }
	switch auth.Provider {
	case "claude":
		for _, window := range []string{"7d", "5h"} {
			if reset := smartBurnUnixReset(signal("Anthropic-Ratelimit-Unified-"+window+"-Reset"), now); !reset.IsZero() {
				return reset
			}
		}
	case "codex":
		var weekly, short time.Time
		for _, window := range []string{"Primary", "Secondary"} {
			prefix := "X-Codex-" + window + "-"
			minutes, err := strconv.ParseInt(signal(prefix+"Window-Minutes"), 10, 64)
			if err != nil || minutes <= 0 || minutes > 7*24*60 {
				continue
			}
			reset := smartBurnUnixReset(signal(prefix+"Reset-At"), now)
			// Relative resets are anchored to observation time, never request time.
			// An expired absolute reset must not be revived by a relative fallback.
			if signal(prefix+"Reset-At") == "" && !q.ObservedAt.IsZero() && !q.ObservedAt.After(now) {
				seconds, errSeconds := strconv.ParseInt(signal(prefix+"Reset-After-Seconds"), 10, 64)
				if errSeconds == nil && seconds > 0 && seconds <= minutes*60 {
					candidate := q.ObservedAt.Add(time.Duration(seconds) * time.Second)
					if candidate.After(now) {
						reset = candidate
					}
				}
			}
			if reset.IsZero() {
				continue
			}
			if minutes == 7*24*60 {
				if weekly.IsZero() || reset.Before(weekly) {
					weekly = reset
				}
			} else if short.IsZero() || reset.Before(short) {
				short = reset
			}
		}
		if !weekly.IsZero() {
			return weekly
		}
		return short
	}
	return time.Time{}
}

func smartBurnUnixReset(value string, now time.Time) time.Time {
	seconds, err := strconv.ParseInt(value, 10, 64)
	if err != nil || seconds <= 0 {
		return time.Time{}
	}
	reset := time.Unix(seconds, 0)
	if !reset.After(now) || reset.After(now.Add(7*24*time.Hour)) {
		return time.Time{}
	}
	return reset
}
