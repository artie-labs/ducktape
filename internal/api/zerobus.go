package api

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/artie-labs/ducktape/api/pkg/ducktape"
)

var (
	// zerobusBatchRows is the most rows sent to Zerobus in one ingest call. Tunable via DUCKTAPE_ZEROBUS_BATCH_ROWS.
	zerobusBatchRows = envIntDefault("DUCKTAPE_ZEROBUS_BATCH_ROWS", 10_000)

	// zerobusFlushTimeoutMs caps how long a flush waits for Zerobus to acknowledge rows. The SDK default is 5 minutes.
	// Tunable via DUCKTAPE_ZEROBUS_FLUSH_TIMEOUT_MS. The SDK can't be cancelled, and closing a stream flushes again, so a
	// request whose client went away can keep running for up to two flushes; an ingest call blocked on Zerobus has no
	// bound of its own.
	zerobusFlushTimeoutMs = envIntDefault("DUCKTAPE_ZEROBUS_FLUSH_TIMEOUT_MS", 60_000)
)

// databricksDomains are the domains a Zerobus endpoint or workspace URL may be on. The route sends the client secret to
// these hosts, so it refuses any other.
var databricksDomains = []string{".databricks.com", ".azuredatabricks.net", ".databricks.us"}

// checkDatabricksURL returns an error unless raw is an https URL on a Databricks domain.
func checkDatabricksURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid URL %q: %w", raw, err)
	}
	if u.Scheme != "https" {
		return fmt.Errorf("URL %q must use https", raw)
	}
	host := strings.ToLower(u.Hostname())
	for _, domain := range databricksDomains {
		if strings.HasSuffix(host, domain) {
			return nil
		}
	}
	return fmt.Errorf("URL %q is not on a Databricks domain (%s)", raw, strings.Join(databricksDomains, ", "))
}

type zerobusTarget struct {
	endpoint     string
	workspaceURL string
	table        string
	clientID     string
	clientSecret string
}

func handleZerobusAppendArrow(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	target, ok := parseZerobusTarget(w, r)
	if !ok {
		return
	}

	rowsAppended, err := ZerobusAppendArrow(r.Context(), target, r.Body)
	if err != nil {
		errMsg := err.Error()
		handleInternalServerErrorJSON(w, ducktape.AppendResponse{Error: &errMsg}, err)
		return
	}
	writeAppendResponse(w, rowsAppended)
	slog.Info(fmt.Sprintf("zerobus arrow append complete for table %s", target.table), slog.Int64("totalRowsAppended", rowsAppended), slog.Duration("elapsed", time.Since(start)))
}

func writeAppendResponse(w http.ResponseWriter, rowsAppended int64) {
	body, err := json.Marshal(ducktape.AppendResponse{RowsAppended: rowsAppended})
	if err != nil {
		err := fmt.Errorf("failed to marshal response: %v", err)
		errMsg := err.Error()
		handleInternalServerErrorJSON(w, ducktape.AppendResponse{Error: &errMsg}, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(body)
}

// parseZerobusTarget reads and checks the X-Zerobus-* headers. On a bad request it writes the error and returns false.
func parseZerobusTarget(w http.ResponseWriter, r *http.Request) (zerobusTarget, bool) {
	if r.ProtoMajor != 2 {
		err := fmt.Errorf("HTTP/2 is required, got %s", r.Proto)
		errMsg := err.Error()
		handleBadRequestJSON(w, ducktape.AppendResponse{Error: &errMsg}, err)
		return zerobusTarget{}, false
	}

	for _, header := range []string{
		ducktape.ZerobusEndpointHeader,
		ducktape.ZerobusWorkspaceURLHeader,
		ducktape.ZerobusTableHeader,
		ducktape.ZerobusClientIDHeader,
		ducktape.ZerobusClientSecretHeader,
	} {
		if r.Header.Get(header) == "" {
			err := fmt.Errorf("%q header is required", header)
			errMsg := err.Error()
			handleBadRequestJSON(w, ducktape.AppendResponse{Error: &errMsg}, err)
			return zerobusTarget{}, false
		}
	}

	target := zerobusTarget{
		endpoint:     r.Header.Get(ducktape.ZerobusEndpointHeader),
		workspaceURL: r.Header.Get(ducktape.ZerobusWorkspaceURLHeader),
		table:        r.Header.Get(ducktape.ZerobusTableHeader),
		clientID:     r.Header.Get(ducktape.ZerobusClientIDHeader),
		clientSecret: r.Header.Get(ducktape.ZerobusClientSecretHeader),
	}

	for _, raw := range []string{target.endpoint, target.workspaceURL} {
		if err := checkDatabricksURL(raw); err != nil {
			errMsg := err.Error()
			handleBadRequestJSON(w, ducktape.AppendResponse{Error: &errMsg}, err)
			return zerobusTarget{}, false
		}
	}
	return target, true
}
