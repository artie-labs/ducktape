package ducktape

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type stopError struct{}

func (stopError) Error() string { return "stop" }

func newTestServer(t *testing.T) *httptest.Server {
	server := httptest.NewServer(h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_ = json.NewEncoder(w).Encode(AppendResponse{RowsAppended: 1})
	}), &http2.Server{}))
	t.Cleanup(server.Close)
	return server
}

func appendWith(ctx context.Context, server *httptest.Server, iterator func(yield func(RowMessageResult) bool)) error {
	_, err := NewClient(server.URL).Append(ctx, "dsn", "db", "schema", "table", iterator,
		func(r RowMessage) ([]byte, error) { return json.Marshal(r) },
		func(b []byte) (*AppendResponse, error) {
			var resp AppendResponse
			return &resp, json.Unmarshal(b, &resp)
		},
	)
	return err
}

func TestAppend(t *testing.T) {
	server := newTestServer(t)
	{
		// An error from the iterator comes back from Append, so callers can match it.
		err := appendWith(t.Context(), server, func(yield func(RowMessageResult) bool) {
			if !yield(RowMessageResult{Row: RowMessage{Values: []any{1}}}) {
				return
			}
			yield(RowMessageResult{Err: stopError{}})
		})
		var stopErr stopError
		if !errors.As(err, &stopErr) {
			t.Fatalf("expected Append to return the iterator's stopError, got %v", err)
		}
	}
	{
		// An iterator blocked inside itself doesn't keep Append waiting past ctx.
		unblock := make(chan struct{})
		defer close(unblock)
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()
		err := appendWith(ctx, server, func(yield func(RowMessageResult) bool) { <-unblock })
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context.DeadlineExceeded, got %v", err)
		}
	}
}
