package capture

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"strconv"
	"sync"

	"github.com/MoeForge/moego/ojson"
	"github.com/google/uuid"
)

type scopeKey struct{}
type scope struct {
	recorder                        *Recorder
	request, invocation             string
	model                           string
	provider, credential, selection string
	session, parentSession          string
}

func from(ctx context.Context) *scope {
	if ctx == nil {
		return nil
	}
	value, _ := ctx.Value(scopeKey{}).(*scope)
	return value
}

// Inherit preserves capture identity when a handler creates its execution context.
func Inherit(ctx, fallback context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if from(ctx) == nil {
		if value := from(fallback); value != nil {
			return context.WithValue(ctx, scopeKey{}, value)
		}
	}
	return ctx
}

// Invocation distinguishes a model execution from its client HTTP/WS connection.
// A duplex execution can contain multiple provider response IDs, preserved in bodies.
func Invocation(ctx context.Context, model string) context.Context {
	parent := from(ctx)
	if parent == nil {
		return ctx
	}
	child := *parent
	child.invocation = uuid.NewString()
	child.model = model
	child.recorder.submit(observation{kind: "invocation.start", request: child.request, invocation: child.invocation,
		metadata: map[string]string{"model": model}})
	return context.WithValue(ctx, scopeKey{}, &child)
}

// Selection records a pseudonymous credential identity, never its token or email.
func Selection(ctx context.Context, provider, credential string) context.Context {
	parent := from(ctx)
	if parent == nil {
		return ctx
	}
	child := *parent
	digest := sha256.Sum256([]byte(credential))
	child.provider, child.credential, child.selection = provider, hex.EncodeToString(digest[:]), uuid.NewString()
	child.recorder.submit(observation{kind: "invocation.selection", request: child.request, invocation: child.invocation,
		metadata: map[string]string{"provider": provider, "credential_id_sha256": child.credential, "selection_id": child.selection}})
	return context.WithValue(ctx, scopeKey{}, &child)
}

func Workflow(ctx context.Context, session, parentSession string) context.Context {
	parent := from(ctx)
	if parent == nil || parent.session == session && parent.parentSession == parentSession {
		return ctx
	}
	child := *parent
	child.session, child.parentSession = session, parentSession
	child.recorder.submit(observation{kind: "invocation.workflow", request: child.request, invocation: child.invocation,
		metadata: map[string]string{"session_id": session, "parent_session_id": parentSession}})
	return context.WithValue(ctx, scopeKey{}, &child)
}

// Stream identifies one observed body or complete WebSocket message. Its framing
// lets the uploader detect missing chunks before reconstructing and redacting it.
type Stream struct {
	scope  *scope
	id     string
	mu     sync.Mutex
	offset uint64
	closed bool
}

func Open(ctx context.Context, metadata map[string]string) *Stream {
	value := from(ctx)
	if value == nil {
		return nil
	}
	stream := &Stream{scope: value, id: uuid.NewString()}
	metadata["model"] = value.model
	if value.provider != "" {
		metadata["provider"] = value.provider
	}
	metadata["credential_id_sha256"] = value.credential
	metadata["selection_id"] = value.selection
	metadata["session_id"], metadata["parent_session_id"] = value.session, value.parentSession
	// Metadata ownership transfers to this observation. Callers must not mutate it.
	stream.emit("stream.open", metadata, nil)
	return stream
}

func (s *Stream) emit(kind string, metadata map[string]string, data []byte) {
	s.scope.recorder.submit(observation{kind: kind, request: s.scope.request, invocation: s.scope.invocation,
		stream: s.id, metadata: metadata, data: data})
}

func (s *Stream) Bytes(data []byte) {
	if s == nil || len(data) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	for len(data) > 0 {
		n := min(len(data), chunkBytes)
		s.emit("stream.data", map[string]string{"offset": strconv.FormatUint(s.offset, 10)}, data[:n])
		s.offset += uint64(n)
		data = data[n:]
	}
}

func (s *Stream) End(complete bool, err error) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	metadata := map[string]string{"bytes": strconv.FormatUint(s.offset, 10), "complete": strconv.FormatBool(complete)}
	if err != nil {
		metadata["error_type"] = "TransportError"
	}
	s.emit("stream.close", metadata, nil)
}

// Frame observes exactly one complete message and whether its write succeeded.
// The raw provider response ID remains the authority for invocation attribution.
func Frame(ctx context.Context, boundary string, provider string, payload []byte, err error) {
	if len(payload) == 0 && err != nil {
		if value := from(ctx); value != nil {
			value.recorder.submit(observation{kind: "websocket.end", request: value.request, invocation: value.invocation,
				metadata: map[string]string{"boundary": boundary, "provider": provider, "error_type": "TransportError"}})
			value.recorder.submit(observation{kind: "websocket.checkpoint", request: value.request, invocation: value.invocation,
				metadata: map[string]string{"boundary": boundary}})
		}
		return
	}
	stream := Open(ctx, map[string]string{"boundary": boundary, "transport": "websocket", "provider": provider})
	stream.Bytes(payload)
	stream.End(err == nil, err)
	if value := from(ctx); value != nil && (err != nil || boundary == "provider.response" && terminal(payload) || boundary == "client.response" && terminal(payload)) {
		value.recorder.submit(observation{kind: "websocket.checkpoint", request: value.request, invocation: value.invocation,
			metadata: map[string]string{"boundary": boundary}})
	}
}

func terminal(payload []byte) bool {
	switch ojson.GetBytesNoCopy(payload, "type").String() {
	case "response.completed", "response.done", "response.failed", "response.incomplete", "error":
		return true
	}
	return false
}

type bodyReader struct {
	io.ReadCloser
	stream *Stream
}

func (b *bodyReader) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.stream.Bytes(p[:n])
	if err != nil {
		b.stream.End(err == io.EOF, nonEOF(err))
	}
	return n, err
}

func (b *bodyReader) Close() error {
	err := b.ReadCloser.Close()
	// End is idempotent, so closing after EOF preserves completeness.
	b.stream.End(false, err)
	return err
}

func nonEOF(err error) error {
	if err == io.EOF {
		return nil
	}
	return err
}
