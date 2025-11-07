package ducktape

import (
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

	go func() {
		for rowMessageResult := range streamIterator {
			if rowMessageResult.Error != nil {
				pw.CloseWithError(fmt.Errorf("error in row message result: %s", *rowMessageResult.Error))
				return
			}
			bytes, err := marshalFunc(rowMessageResult.Row)
			if err != nil {
				pw.CloseWithError(err)
				return
			}
			_, err = pw.Write(append(bytes, '\n'))
			if err != nil {
				pw.CloseWithError(err)
				return
			}
		}
		pw.Close()
	}()

	req.Body = pr

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
