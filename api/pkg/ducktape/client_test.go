package ducktape

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type stopError struct{}

func (stopError) Error() string { return "stop" }

func newTestServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	server := httptest.NewServer(h2c.NewHandler(handler, &http2.Server{}))
	t.Cleanup(server.Close)
	return server
}

// readAllHandler reads the whole body before answering, like a successful append.
func readAllHandler(w http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
	_ = json.NewEncoder(w).Encode(AppendResponse{RowsAppended: 1})
}

// earlyErrorHandler answers with a server error without reading the body, like a failed DuckDB connect.
func earlyErrorHandler(w http.ResponseWriter, _ *http.Request) {
	errMsg := "boom"
	_ = json.NewEncoder(w).Encode(AppendResponse{Error: &errMsg})
	w.(http.Flusher).Flush()
}

func marshalRow(r RowMessage) ([]byte, error) { return json.Marshal(r) }

// appendWith runs Append against server. onResponse, if set, runs when Append unmarshals the server's response.
func appendWith(t *testing.T, server *httptest.Server, iterator func(yield func(RowMessageResult) bool), marshalFunc func(RowMessage) ([]byte, error), onResponse func()) (*AppendResponse, error) {
	return NewClient(server.URL).Append(t.Context(), "dsn", "db", "schema", "table", iterator, marshalFunc,
		func(b []byte) (*AppendResponse, error) {
			if onResponse != nil {
				onResponse()
			}
			var resp AppendResponse
			return &resp, json.Unmarshal(b, &resp)
		},
	)
}

func TestAppend(t *testing.T) {
	{
		// An error from the iterator comes back from Append, so callers can match it.
		server := newTestServer(t, readAllHandler)
		_, err := appendWith(t, server, func(yield func(RowMessageResult) bool) {
			if !yield(RowMessageResult{Row: RowMessage{Values: []any{1}}}) {
				return
			}
			yield(RowMessageResult{Err: stopError{}})
		}, marshalRow, nil)
		var stopErr stopError
		if !errors.As(err, &stopErr) {
			t.Fatalf("expected Append to return the iterator's stopError, got %v", err)
		}
	}
	{
		// A marshal error comes back from Append too.
		server := newTestServer(t, readAllHandler)
		_, err := appendWith(t, server, func(yield func(RowMessageResult) bool) {
			yield(RowMessageResult{Row: RowMessage{Values: []any{1}}})
		}, func(RowMessage) ([]byte, error) { return nil, stopError{} }, nil)
		var stopErr stopError
		if !errors.As(err, &stopErr) {
			t.Fatalf("expected Append to return the marshal stopError, got %v", err)
		}
	}
	{
		// When the server answers with an error first, that error wins over an iterator error that comes after it.
		server := newTestServer(t, earlyErrorHandler)
		gotResponse, writerFailed := make(chan struct{}), make(chan struct{})
		resp, err := appendWith(t, server, func(yield func(RowMessageResult) bool) {
			defer close(writerFailed)
			if !yield(RowMessageResult{Row: RowMessage{Values: []any{1}}}) {
				return
			}
			<-gotResponse
			yield(RowMessageResult{Err: stopError{}})
		}, marshalRow, func() {
			// Hold Append here until the writer has recorded the iterator's error, so both errors are present.
			close(gotResponse)
			<-writerFailed
		})
		if err != nil {
			t.Fatalf("expected no client error, got %v", err)
		}
		if resp.Error == nil || *resp.Error != "boom" {
			t.Fatalf("expected the server's error, got %+v", resp)
		}
	}
	{
		// Once the server has answered, the writer stops pulling rows instead of filling its buffer first.
		server := newTestServer(t, earlyErrorHandler)
		var responded atomic.Bool
		var rowsAfterResponse atomic.Int64
		_, err := appendWith(t, server, func(yield func(RowMessageResult) bool) {
			for {
				if responded.Load() {
					rowsAfterResponse.Add(1)
				}
				if !yield(RowMessageResult{Row: RowMessage{Values: []any{1}}}) {
					return
				}
			}
		}, marshalRow, func() { responded.Store(true) })
		if err != nil {
			t.Fatalf("expected no client error, got %v", err)
		}
		// Without the stop, the writer keeps going until its 1 MB buffer flushes: about 95k of these rows.
		if n := rowsAfterResponse.Load(); n > 1000 {
			t.Fatalf("expected the writer to stop soon after the response, but it pulled %d more rows", n)
		}
	}
}
