package ducktape

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type stopError struct{}

func (stopError) Error() string { return "stop" }

func TestAppendReturnsIteratorError(t *testing.T) {
	server := httptest.NewServer(h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_ = json.NewEncoder(w).Encode(AppendResponse{RowsAppended: 1})
	}), &http2.Server{}))
	defer server.Close()

	iterator := func(yield func(RowMessageResult) bool) {
		if !yield(RowMessageResult{Row: RowMessage{Values: []any{1}}}) {
			return
		}
		yield(RowMessageResult{Err: stopError{}})
	}

	_, err := NewClient(server.URL).Append(t.Context(), "dsn", "db", "schema", "table", iterator,
		func(r RowMessage) ([]byte, error) { return json.Marshal(r) },
		func(b []byte) (*AppendResponse, error) {
			var resp AppendResponse
			return &resp, json.Unmarshal(b, &resp)
		},
	)
	if _, ok := errors.AsType[stopError](err); !ok {
		t.Fatalf("expected Append to return the iterator's stopError, got %v", err)
	}
}
