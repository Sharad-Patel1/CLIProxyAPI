package capture

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/MoeForge/moego/base64"
	"github.com/MoeForge/moego/ojson"
	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/httptransport"
)

func records(t *testing.T, dir string) []ojson.Result {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "*.sealed"))
	if err != nil {
		t.Fatal(err)
	}
	var result []ojson.Result
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte("\n")) {
			if !ojson.ValidBytes(line) {
				t.Fatalf("invalid spool record")
			}
			result = append(result, ojson.ParseBytes(line))
		}
		info, _ := os.Stat(path)
		if info.Mode().Perm() != 0600 {
			t.Fatalf("spool permissions: %o", info.Mode().Perm())
		}
	}
	return result
}

func TestRecorderSealsBodyWithOffsetsAndCleanWatermark(t *testing.T) {
	dir := t.TempDir()
	r := NewRecorder(dir, 1<<20)
	ctx := context.WithValue(context.Background(), scopeKey{}, &scope{recorder: r, request: "client-a", invocation: "execution-a"})
	data := bytes.Repeat([]byte("payload"), 12000)
	stream := Open(ctx, map[string]string{"boundary": "provider.response"})
	stream.Bytes(data)
	stream.End(true, nil)
	r.Close()
	var reconstructed []byte
	closed := false
	for _, record := range records(t, dir) {
		if record.Get("kind").String() == "stream.data" {
			if record.Get("metadata.offset").Uint() != uint64(len(reconstructed)) {
				t.Fatal("non-contiguous offset")
			}
			raw, err := base64.StdEncoding.DecodeString(record.Get("data_b64").String())
			if err != nil {
				t.Fatal(err)
			}
			reconstructed = append(reconstructed, raw...)
		}
		if record.Get("kind").String() == "stream.close" {
			closed = record.Get("metadata.complete").String() == "true"
		}
	}
	if !bytes.Equal(data, reconstructed) || !closed {
		t.Fatal("body was changed or truncated")
	}
	status, err := os.ReadFile(filepath.Join(dir, "status.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !ojson.GetBytes(status, "clean_shutdown").Bool() {
		t.Fatal("shutdown not clean")
	}
	if ojson.GetBytes(status, "last_sealed_sequence").Uint() != ojson.GetBytes(status, "contiguous_durable_sequence").Uint() {
		t.Fatal("watermark differs without failure")
	}
}

func TestOverflowNeverBlocksAndCannotClaimDurability(t *testing.T) {
	r := &Recorder{maxQueued: 2048, queue: make(chan observation, 1)}
	for i := 0; i < 100; i++ {
		r.submit(observation{data: []byte("body")})
	}
	if r.dropped.Load() != 99 || len(r.queue) != 1 {
		t.Fatalf("queue=%d drops=%d", len(r.queue), r.dropped.Load())
	}
	if r.queued.Load() > r.maxQueued {
		t.Fatal("queue byte limit exceeded")
	}
}

func TestSpoolCeilingPreservesUnacknowledgedFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "previous.sealed")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(8 << 30); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	r := NewRecorder(dir, 1<<20)
	r.submit(observation{kind: "test"})
	r.Close()
	if r.failed.Load() == 0 {
		t.Fatal("spool ceiling was not reported")
	}
	info, err := os.Stat(path)
	if err != nil || info.Size() != 8<<30 {
		t.Fatal("unacknowledged spool changed")
	}
}

func TestPersistenceFailureDoesNotBlockSubmitOrOverwriteCrashRemainder(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "file")
	if err := os.WriteFile(file, []byte("existing"), 0600); err != nil {
		t.Fatal(err)
	}
	r := NewRecorder(filepath.Join(file, "spool"), 1<<20)
	r.submit(observation{kind: "test"})
	r.Close()
	if r.failed.Load() == 0 {
		t.Fatal("persistence failure was hidden")
	}
	raw, _ := os.ReadFile(file)
	if string(raw) != "existing" {
		t.Fatal("existing file changed")
	}
	dir := filepath.Join(root, "valid")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	tail := []byte("incomplete crash tail")
	path := filepath.Join(dir, "old.open")
	if err := os.WriteFile(path, tail, 0600); err != nil {
		t.Fatal(err)
	}
	restarted := NewRecorder(dir, 1<<20)
	restarted.Close()
	preserved, err := os.ReadFile(path + ".recovered")
	if err != nil || !bytes.Equal(tail, preserved) {
		t.Fatal("crash evidence was discarded")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestDecoratorPreservesFinalBodiesPolicyAndCredentialsExclusion(t *testing.T) {
	dir := t.TempDir()
	r := NewRecorder(dir, 1<<20)
	selected := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		raw, _ := io.ReadAll(req.Body)
		_ = req.Body.Close()
		if string(raw) != `{"final_payload":true}` || req.Header.Get("Authorization") != "Bearer do-not-archive-header" {
			t.Fatal("outbound request changed")
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader("data: {\"answer\":\"complete\"}\n\n"))}, nil
	})
	engine := gin.New()
	engine.Use(r.Middleware())
	engine.POST("/v1/responses", func(c *gin.Context) {
		ctx := Invocation(Inherit(context.Background(), c.Request.Context()), "client-model")
		ctx = httptransport.InheritDecorator(ctx, c.Request.Context())
		client := httptransport.Decorate(ctx, &http.Client{Transport: selected, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }})
		if client.CheckRedirect == nil {
			t.Fatal("client policy lost")
		}
		req, _ := http.NewRequestWithContext(ctx, "POST", "https://provider.example/responses?token=secret-query", strings.NewReader(`{"final_payload":true}`))
		req.Header.Set("Authorization", "Bearer do-not-archive-header")
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = response.Body.Close() }()
		_, _ = io.Copy(c.Writer, response.Body)
	})
	writer := httptest.NewRecorder()
	request := httptest.NewRequest("POST", "/v1/responses", strings.NewReader("original client request"))
	engine.ServeHTTP(writer, request)
	r.Close()
	if writer.Code != 200 || writer.Body.String() != "data: {\"answer\":\"complete\"}\n\n" {
		t.Fatal("client response changed")
	}
	var boundaries []string
	for _, record := range records(t, dir) {
		if strings.Contains(record.Raw, "do-not-archive-header") || strings.Contains(record.Raw, "secret-query") {
			t.Fatal("transport credential leaked")
		}
		if record.Get("kind").String() == "stream.open" {
			boundaries = append(boundaries, record.Get("metadata.boundary").String())
		}
	}
	if len(boundaries) != 4 {
		t.Fatalf("missing boundaries: %v", boundaries)
	}
}
