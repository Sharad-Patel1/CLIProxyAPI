package capture

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/httptransport"
)

// Middleware observes model routes only. Management/auth endpoints and URL query
// strings are excluded. Authorization and cookie headers never enter the spool.
func (r *Recorder) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		path := c.Request.URL.Path
		if !(strings.HasPrefix(path, "/v1/") || strings.HasPrefix(path, "/v1beta/")) {
			c.Next()
			return
		}
		value := &scope{recorder: r, request: uuid.NewString()}
		ctx := context.WithValue(c.Request.Context(), scopeKey{}, value)
		ctx = httptransport.WithDecorator(ctx, func(next http.RoundTripper) http.RoundTripper { return &transport{next: next} })
		c.Request = c.Request.WithContext(ctx)
		input := Open(ctx, map[string]string{"boundary": "client.request", "transport": "http", "method": c.Request.Method, "path": path,
			"content_type": c.Request.Header.Get("Content-Type"), "client_request_id": c.Request.Header.Get("X-Request-ID"),
			"user_agent": c.Request.Header.Get("User-Agent")})
		if c.Request.Body != nil && c.Request.Body != http.NoBody {
			c.Request.Body = &bodyReader{ReadCloser: c.Request.Body, stream: input}
		} else {
			input.End(true, nil)
		}
		output := Open(ctx, map[string]string{"boundary": "client.response", "transport": "http"})
		writer := &responseWriter{ResponseWriter: c.Writer, stream: output}
		c.Writer = writer
		defer func() {
			input.End(false, nil)
			output.End(true, nil)
			r.submit(observation{kind: "request.close", request: value.request,
				metadata: map[string]string{"status": strconv.Itoa(writer.Status())}})
		}()
		c.Next()
	}
}

type responseWriter struct {
	gin.ResponseWriter
	stream *Stream
}

func (w *responseWriter) Write(data []byte) (int, error) {
	n, err := w.ResponseWriter.Write(data)
	w.stream.Bytes(data[:n])
	if err != nil {
		w.stream.End(false, err)
	}
	return n, err
}
func (w *responseWriter) WriteString(data string) (int, error) { return w.Write([]byte(data)) }

// transport wraps the final selected RoundTripper without changing its policy.
// Bodies are observed only as the selected transport/consumer reads them.
type transport struct{ next http.RoundTripper }

func (t *transport) RoundTrip(request *http.Request) (*http.Response, error) {
	ctx := request.Context()
	value := from(ctx)
	if value == nil {
		return t.next.RoundTrip(request)
	}
	attempt := uuid.NewString()
	metadata := func(boundary string) map[string]string {
		return map[string]string{
			"boundary": boundary, "transport": "http", "attempt_id": attempt,
			"host": request.URL.Host, "path": request.URL.Path, "method": request.Method, "model": value.model}
	}
	input := Open(ctx, metadata("provider.request"))
	clone := request.Clone(ctx)
	if request.Body != nil && request.Body != http.NoBody {
		clone.Body = &bodyReader{ReadCloser: request.Body, stream: input}
	} else {
		input.End(true, nil)
	}
	response, err := t.next.RoundTrip(clone)
	if err != nil {
		input.End(false, err)
		stream := Open(ctx, map[string]string{"boundary": "provider.response", "transport": "http", "attempt_id": attempt})
		stream.End(false, err)
		return response, err
	}
	info := metadata("provider.response")
	info["status"] = strconv.Itoa(response.StatusCode)
	info["content_type"] = response.Header.Get("Content-Type")
	info["content_encoding"] = response.Header.Get("Content-Encoding")
	info["uncompressed"] = strconv.FormatBool(response.Uncompressed)
	output := Open(ctx, info)
	if response.Body != nil {
		response.Body = &bodyReader{ReadCloser: response.Body, stream: output}
	} else {
		output.End(true, nil)
	}
	return response, nil
}

// No WriterTo method: optimized copies must still pass through observed Read.
var _ io.ReadCloser = (*bodyReader)(nil)
