package openai

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MoeForge/moego/ojson"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	runtimeexecutor "github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

type fallbackRequest struct {
	transport string
	body      []byte
	key       string
}

// Exercise the actual handler, account selection, transport and SSE-to-WS bridge.
func newHTTPFallbackFixture(t *testing.T, steering bool) (*websocket.Conn, <-chan fallbackRequest, string) {
	t.Helper()
	requests := make(chan fallbackRequest, 10)
	var responses atomic.Int32
	completion := func() string {
		return fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp-%d","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"summary"}]}]}}`, responses.Add(1))
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if websocket.IsWebSocketUpgrade(r) {
			conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
			if err != nil {
				t.Error(err)
				return
			}
			defer func() { _ = conn.Close() }()
			for {
				_, body, err := conn.ReadMessage()
				if err != nil {
					return
				}
				requests <- fallbackRequest{"websocket", body, r.Header.Get("Authorization")}
				if len(body) >= coreexecutor.CodexHTTPFallbackThreshold {
					_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseMessageTooBig, "message too big"), time.Now().Add(time.Second))
					return
				}
				completed := completion()
				created := ojson.NewDocString(`{"type":"response.created","response":{"status":"in_progress","output":[]}}`)
				_ = created.Set("response.id", ojson.Get(completed, "response.id").String())
				if err = conn.WriteMessage(websocket.TextMessage, created.Bytes()); err != nil {
					return
				}
				if err = conn.WriteMessage(websocket.TextMessage, []byte(completed)); err != nil {
					return
				}
			}
		}
		if r.Method != http.MethodPost || r.URL.Path != "/responses" {
			t.Errorf("unexpected HTTP request: %s %s", r.Method, r.URL.Path)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		requests <- fallbackRequest{"http", body, r.Header.Get("Authorization")}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprintf(w, "data: %s\n\n", completion())
	}))
	t.Cleanup(upstream.Close)
	cfg := &config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll, CodexResponseSteering: steering}}
	cfg.Codex.ResponseSteering = steering
	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetConfig(cfg)
	manager.RegisterExecutor(runtimeexecutor.NewCodexAutoExecutor(cfg))
	for _, key := range []string{"key-a", "key-b"} {
		id := t.Name() + key
		_, err := manager.Register(context.Background(), &coreauth.Auth{ID: id, Provider: "codex", Status: coreauth.StatusActive, Attributes: map[string]string{"api_key": key, "base_url": upstream.URL, "websockets": "true"}})
		if err != nil {
			t.Fatal(err)
		}
		registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: "fallback-model"}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
	}
	handler := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&cfg.SDKConfig, manager))
	router := gin.New()
	router.GET("/v1/responses", handler.ResponsesWebsocket)
	downstream := httptest.NewServer(router)
	t.Cleanup(downstream.Close)
	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(downstream.URL, "http")+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client, requests, "ws" + strings.TrimPrefix(downstream.URL, "http") + "/v1/responses"
}

func fallbackExchange(t *testing.T, client *websocket.Conn, payload []byte, event string) []byte {
	t.Helper()
	_ = client.SetWriteDeadline(time.Now().Add(15 * time.Second))
	_ = client.SetReadDeadline(time.Now().Add(15 * time.Second))
	if err := client.WriteMessage(websocket.TextMessage, payload); err != nil {
		t.Fatal(err)
	}
	for {
		_, body, err := client.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		typ := ojson.GetBytesNoCopy(body, "type").String()
		if typ == event {
			return body
		}
		if typ == "error" {
			t.Fatalf("unexpected error: %s", body)
		}
	}
}

func fallbackPayload(size int, previous string) []byte {
	doc := ojson.NewDocString(`{"type":"response.create","model":"fallback-model","input":[{"type":"message","role":"user","content":"full context sentinel"}],"instructions":""}`)
	if previous != "" {
		_ = doc.Set("previous_response_id", previous)
	}
	_ = doc.Set("instructions", strings.Repeat("x", size-len(doc.Bytes())))
	return doc.Bytes()
}

func TestResponsesWebsocketLargeContextUsesHTTP(t *testing.T) {
	for _, steering := range []bool{false, true} {
		t.Run(fmt.Sprintf("steering=%t", steering), func(t *testing.T) {
			client, requests, _ := newHTTPFallbackFixture(t, steering)
			// Exact byte count captured during the production compaction failure.
			payload := fallbackPayload(25102631, "")
			if len(payload) != 25102631 {
				t.Fatal("incorrect reproduction size")
			}
			fallbackExchange(t, client, payload, "response.completed")
			first := <-requests
			if first.transport != "http" {
				t.Fatalf("transport = %s, want HTTP", first.transport)
			}
			if got := ojson.GetBytesNoCopy(first.body, "input.0.content").String(); got != "full context sentinel" {
				t.Fatalf("input lost: %s", got)
			}
			fallbackExchange(t, client, []byte(`{"type":"response.create","previous_response_id":"resp-1","input":[{"type":"message","role":"user","content":"continue sentinel"}]}`), "response.completed")
			second := <-requests
			if second.transport != "http" || second.key != first.key {
				t.Fatalf("continuation changed transport/account: %s %s", second.transport, second.key)
			}
			if ojson.GetBytesNoCopy(second.body, "previous_response_id").Exists() {
				t.Fatal("HTTP received a socket-scoped continuation")
			}
			input := ojson.GetBytesNoCopy(second.body, "input").Raw
			for _, text := range []string{"full context sentinel", "summary", "continue sentinel"} {
				if !strings.Contains(input, text) {
					t.Fatalf("continuation lost %q", text)
				}
			}
			compacted := ojson.NewDocString(`{"type":"response.create","instructions":"brief instructions","input":[{"type":"message","role":"user","content":""}]}`)
			_ = compacted.Set("input.0.content", codexLocalCompactionSummaryPrefix+"\ncompact sentinel")
			fallbackExchange(t, client, compacted.Bytes(), "response.completed")
			third := <-requests
			if third.transport != "http" || third.key != first.key {
				t.Fatal("compaction changed transport or account")
			}
			input = ojson.GetBytesNoCopy(third.body, "input").Raw
			if strings.Contains(input, "full context sentinel") || strings.Contains(input, "continue sentinel") || !strings.Contains(input, "compact sentinel") {
				t.Fatal("compaction failed to replace the previous transcript")
			}
		})
	}
}

func TestResponsesWebsocketLargeDeltaRequiresFullReplay(t *testing.T) {
	client, requests, _ := newHTTPFallbackFixture(t, false)
	fallbackExchange(t, client, []byte(`{"type":"response.create","model":"fallback-model","input":[]}`), "response.completed")
	first := <-requests
	if first.transport != "websocket" {
		t.Fatalf("small request transport = %s", first.transport)
	}
	errBody := fallbackExchange(t, client, fallbackPayload(coreexecutor.CodexHTTPFallbackThreshold, "resp-1"), "error")
	if got := ojson.GetBytesNoCopy(errBody, "error.code").String(); got != "previous_response_not_found" {
		t.Fatalf("error = %s", errBody)
	}
	fallbackExchange(t, client, fallbackPayload(coreexecutor.CodexHTTPFallbackThreshold, ""), "response.completed")
	second := <-requests
	if second.transport != "http" || second.key != first.key {
		t.Fatalf("full replay changed account or failed HTTP fallback: %s %s", second.transport, second.key)
	}
	select {
	case <-requests:
		t.Fatal("rejected delta was sent upstream")
	default:
	}
}

func TestResponsesWebsocketDuplexLargeContextReconnectsToHTTP(t *testing.T) {
	client, requests, url := newHTTPFallbackFixture(t, true)
	fallbackExchange(t, client, []byte(`{"type":"response.create","model":"fallback-model","input":[]}`), "response.completed")
	if first := <-requests; first.transport != "websocket" {
		t.Fatal("small create did not use websocket")
	}
	payload := fallbackPayload(coreexecutor.CodexHTTPFallbackThreshold, "")
	if err := client.WriteMessage(websocket.TextMessage, payload); err != nil {
		t.Fatal(err)
	}
	_, _, err := client.ReadMessage()
	var closeErr *websocket.CloseError
	if !errors.As(err, &closeErr) || closeErr.Code != websocket.CloseServiceRestart {
		t.Fatalf("expected replay close, got %v", err)
	}
	retry, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = retry.Close() }()
	fallbackExchange(t, retry, payload, "response.completed")
	if second := <-requests; second.transport != "http" {
		t.Fatal("reconnect did not select HTTP")
	}
	select {
	case <-requests:
		t.Fatal("oversized create was sent over websocket")
	default:
	}
}
