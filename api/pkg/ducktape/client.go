package ducktape

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"iter"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2"
)

type Client struct {
	baseURL    string
	httpClient *http.Client
}

func NewClient(baseURL string) *Client {
	tr := &http2.Transport{
		AllowHTTP: true,
		// Below the server's 2m idle timeout, so the client closes idle connections
		// first and an Append (non-retryable pipe body) doesn't race the server's idle GOAWAY.
		IdleConnTimeout: 90 * time.Second,
		DialTLSContext: func(ctx context.Context, network, addr string, cfg *tls.Config) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, addr)
		},
	}
	return &Client{baseURL: baseURL, httpClient: &http.Client{Transport: tr}}
}

func (c *Client) Ping(
	ctx context.Context,
	connectionString string,
) error {
	url := fmt.Sprintf("%s%s", c.baseURL, PingRoute)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	req.Header.Set(DuckDBConnectionStringHeader, connectionString)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to ping duckdb: %s", resp.Status)
	}

	return nil
}

func (c *Client) Execute(
	ctx context.Context,
	request ExecuteRequest,
	connectionString string,
	marshalFunc func(r ExecuteRequest) ([]byte, error),
	unmarshalFunc func(r []byte) (*ExecuteResponse, error),
) (*ExecuteResponse, error) {
	url := fmt.Sprintf("%s%s", c.baseURL, ExecuteRoute)
	body, err := marshalFunc(request)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(DuckDBConnectionStringHeader, connectionString)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return unmarshalFunc(responseBody)
}

func (c *Client) Query(
	ctx context.Context,
	request QueryRequest,
	connectionString string,
	marshalFunc func(r QueryRequest) ([]byte, error),
	unmarshalFunc func(r []byte) (*QueryResponse, error),
) (*QueryResponse, error) {
	url := fmt.Sprintf("%s%s", c.baseURL, QueryRoute)
	body, err := marshalFunc(request)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(DuckDBConnectionStringHeader, connectionString)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return unmarshalFunc(responseBody)
}

// Append streams rows from streamIterator to the server. It waits for streamIterator to return, so the iterator
// shouldn't block.
func (c *Client) Append(
	ctx context.Context,
	connectionString string,
	database string,
	schema string,
	table string,
	streamIterator iter.Seq[RowMessageResult],
	marshalFunc func(r RowMessage) ([]byte, error),
	unmarshalFunc func(r []byte) (*AppendResponse, error),
) (*AppendResponse, error) {
	headers := map[string]string{
		DuckDBConnectionStringHeader: connectionString,
		DuckDBDatabaseHeader:         database,
		DuckDBSchemaHeader:           schema,
		DuckDBTableHeader:            table,
	}
	encoded := func(yield func([]byte, error) bool) {
		for result := range streamIterator {
			if result.Err != nil {
				yield(nil, fmt.Errorf("error in row message result: %w", result.Err))
				return
			}
			bytes, err := marshalFunc(result.Row)
			if !yield(bytes, err) {
				return
			}
			if err != nil {
				return
			}
		}
	}
	return streamAppend(ctx, c, AppendRoute, headers, encoded, true, unmarshalFunc)
}

// ZerobusTarget is the Databricks table a Zerobus append writes to, and the service principal it writes as.
type ZerobusTarget struct {
	// Endpoint is the workspace's Zerobus endpoint, such as https://<id>.zerobus.<region>.cloud.databricks.com.
	Endpoint string
	// WorkspaceURL is the workspace URL, used to get OAuth tokens.
	WorkspaceURL string
	// Table is catalog.schema.table.
	Table        string
	ClientID     string
	ClientSecret string
}

// AppendZerobus streams rows into a Databricks table through Zerobus Ingest. Each row is one JSON object keyed by
// column name, with no newline in it; an empty row or one with a newline stops the append, as does an error from rows.
// It returns once Zerobus has acknowledged every row. Delivery is at-least-once: a failed append may have written some
// rows, so a retry can write them twice. A failure caused by the rows themselves, such as invalid JSON, fails the same
// way on every retry and writes the rows before it again each time.
func (c *Client) AppendZerobus(
	ctx context.Context,
	target ZerobusTarget,
	rows iter.Seq2[[]byte, error],
	unmarshalFunc func(r []byte) (*AppendResponse, error),
) (*AppendResponse, error) {
	checked := func(yield func([]byte, error) bool) {
		var n int
		for row, err := range rows {
			n++
			if err == nil {
				err = checkNDJSONRow(row, n)
			}
			if !yield(row, err) {
				return
			}
			if err != nil {
				return
			}
		}
	}
	return streamAppend(ctx, c, ZerobusAppendRoute, target.headers(), checked, true, unmarshalFunc)
}

// AppendZerobusArrow streams an Arrow IPC stream into a Databricks table through Zerobus's Arrow Flight path. chunks
// are written back to back and must form one IPC stream: a schema matching the table's columns, then record batches.
// Ducktape sends the batches to Zerobus with zstd compression. It returns once Zerobus has acknowledged every row, and
// delivery is at-least-once, like AppendZerobus.
func (c *Client) AppendZerobusArrow(
	ctx context.Context,
	target ZerobusTarget,
	chunks iter.Seq2[[]byte, error],
	unmarshalFunc func(r []byte) (*AppendResponse, error),
) (*AppendResponse, error) {
	return streamAppend(ctx, c, ZerobusAppendArrowRoute, target.headers(), chunks, false, unmarshalFunc)
}

func (t ZerobusTarget) headers() map[string]string {
	return map[string]string{
		ZerobusEndpointHeader:     t.Endpoint,
		ZerobusWorkspaceURLHeader: t.WorkspaceURL,
		ZerobusTableHeader:        t.Table,
		ZerobusClientIDHeader:     t.ClientID,
		ZerobusClientSecretHeader: t.ClientSecret,
	}
}

// checkNDJSONRow returns an error if row can't be sent as one NDJSON line: the server would skip an empty row and
// split one with a newline into two broken records. n is the row's 1-based position, for the error.
func checkNDJSONRow(row []byte, n int) error {
	if len(row) == 0 {
		return fmt.Errorf("row %d is empty", n)
	}
	if bytes.IndexByte(row, '\n') >= 0 {
		return fmt.Errorf("row %d contains a newline", n)
	}
	return nil
}

// streamAppend streams rows to route back to back, with a newline after each one when newline is set (NDJSON). An
// error from rows stops the append. It waits for rows to return, so the iterator shouldn't block.
func streamAppend(
	ctx context.Context,
	c *Client,
	route string,
	headers map[string]string,
	rows iter.Seq2[[]byte, error],
	newline bool,
	unmarshalFunc func(r []byte) (*AppendResponse, error),
) (*AppendResponse, error) {
	url := fmt.Sprintf("%s%s", c.baseURL, route)
	req, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/octet-stream")
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	pr, pw := io.Pipe()

	// streamErr is an error from the iterator. It's only read after done is closed.
	var streamErr error
	// stopped tells the writer the request is over, so it stops pulling rows instead of filling its buffer first.
	var stopped atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		bw := bufio.NewWriterSize(pw, RecommendedBufferSize)
		for bytes, err := range rows {
			if stopped.Load() {
				return
			}
			if err != nil {
				streamErr = err
				pw.CloseWithError(err)
				return
			}
			if _, err = bw.Write(bytes); err != nil {
				pw.CloseWithError(err)
				return
			}
			if !newline {
				continue
			}
			if err = bw.WriteByte('\n'); err != nil {
				pw.CloseWithError(err)
				return
			}
		}
		if err := bw.Flush(); err != nil {
			pw.CloseWithError(err)
			return
		}
		pw.Close()
	}()

	req.Body = pr

	appendResp, err := func() (*AppendResponse, error) {
		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		responseBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return unmarshalFunc(responseBody)
	}()

	// The request is over. Stop the writer before its next row and unblock a write in progress, then wait for it, so
	// streamErr is safe to read and the iterator isn't running once Append returns.
	stopped.Store(true)
	pr.Close()
	<-done

	// The iterator's own error explains the failure better than whatever the HTTP request saw, unless the server
	// answered with its own error first.
	if streamErr != nil && (appendResp == nil || appendResp.Error == nil) {
		return nil, streamErr
	}
	return appendResp, err
}
