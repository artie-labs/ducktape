package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/artie-labs/ducktape/api/pkg/ducktape"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

func TestHandleZerobusAppendArrowMissingHeader(t *testing.T) {
	mux := http.NewServeMux()
	RegisterApiRoutes(mux)
	server := httptest.NewServer(h2c.NewHandler(mux, &http2.Server{}))
	t.Cleanup(server.Close)

	target := ducktape.ZerobusTarget{
		Endpoint:     "https://zerobus.example.com",
		WorkspaceURL: "https://workspace.example.com",
		ClientID:     "id",
		ClientSecret: "secret",
	}
	chunks := func(yield func([]byte, error) bool) {}
	resp, err := ducktape.NewClient(server.URL).AppendZerobusArrow(t.Context(), target, chunks, func(b []byte) (*ducktape.AppendResponse, error) {
		var resp ducktape.AppendResponse
		return &resp, json.Unmarshal(b, &resp)
	})
	if err != nil {
		t.Fatalf("unexpected client error: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected the server to reject the request")
	}
	if !strings.Contains(*resp.Error, ducktape.ZerobusTableHeader) {
		t.Fatalf("expected the error to name the missing table header, got %q", *resp.Error)
	}
}

func TestCheckDatabricksURL(t *testing.T) {
	{
		// Workspace and Zerobus URLs on Databricks domains pass.
		for _, raw := range []string{
			"https://dbc-5c81740c-483b.cloud.databricks.com",
			"https://4379012206025330.zerobus.us-east-1.cloud.databricks.com",
			"https://adb-123.4.azuredatabricks.net",
			"https://DBC-1.Cloud.Databricks.com:443",
		} {
			if err := checkDatabricksURL(raw); err != nil {
				t.Fatalf("expected %q to pass, got %v", raw, err)
			}
		}
	}
	{
		// Plain http is refused.
		if err := checkDatabricksURL("http://dbc-1.cloud.databricks.com"); err == nil {
			t.Fatal("expected http to be refused")
		}
	}
	{
		// Other hosts are refused, including ones that only contain a Databricks domain.
		for _, raw := range []string{
			"https://example.com",
			"https://databricks.com.evil.example",
			"https://evildatabricks.com",
			"https://127.0.0.1",
			"not a url",
		} {
			if err := checkDatabricksURL(raw); err == nil {
				t.Fatalf("expected %q to be refused", raw)
			}
		}
	}
}
