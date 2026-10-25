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

	"golang.org/x/net/http2"
)

type Client struct {
	baseURL    string
	httpClient *http.Client
}

func NewClient(baseURL string) *Client {
	tr := &http2.Transport{
		AllowHTTP: true,
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
	url := fmt.Sprintf("%s%s", c.baseURL, AppendRoute)
	req, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set(DuckDBConnectionStringHeader, connectionString)
	req.Header.Set(DuckDBDatabaseHeader, database)
	req.Header.Set(DuckDBSchemaHeader, schema)
	req.Header.Set(DuckDBTableHeader, table)

	pr, pw := io.Pipe()

	// streamErr is an error from the iterator or from marshalling a row. It's only read after done is closed.
	var streamErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		bw := bufio.NewWriterSize(pw, RecommendedBufferSize)
		for rowMessageResult := range streamIterator {
			if rowMessageResult.Err != nil {
				streamErr = fmt.Errorf("error in row message result: %w", rowMessageResult.Err)
				pw.CloseWithError(streamErr)
				return
			}
			bytes, err := marshalFunc(rowMessageResult.Row)
			if err != nil {
				streamErr = err
				pw.CloseWithError(err)
				return
			}
			if _, err = bw.Write(bytes); err != nil {
				pw.CloseWithError(err)
				return
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

	var responseBody []byte
	resp, err := c.httpClient.Do(req)
	if err == nil {
		responseBody, err = io.ReadAll(resp.Body)
		resp.Body.Close()
	}

	// Unblock the writer if the request ended before it sent every row, then wait for it so streamErr is safe to read.
	pr.Close()
	select {
	case <-done:
	case <-ctx.Done():
		// The iterator is blocked inside itself; leave the goroutine to finish on its own rather than hang past ctx.
		return nil, ctx.Err()
	}

	// The iterator's own error explains the failure better than whatever the HTTP request saw.
	if streamErr != nil {
		return nil, streamErr
	}
	if err != nil {
		return nil, err
	}
	return unmarshalFunc(responseBody)
}
