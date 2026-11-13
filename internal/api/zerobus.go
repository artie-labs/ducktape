package api

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/artie-labs/ducktape/api/pkg/ducktape"
	zerobus "github.com/databricks/zerobus-sdk/go"
)

var (
	// zerobusBatchRows is the most rows sent to Zerobus in one ingest call. Tunable via DUCKTAPE_ZEROBUS_BATCH_ROWS.
	zerobusBatchRows = envIntDefault("DUCKTAPE_ZEROBUS_BATCH_ROWS", 10_000)

	// zerobusBatchBytes is the most row bytes sent to Zerobus in one ingest call, kept well under gRPC message limits.
	// Tunable via DUCKTAPE_ZEROBUS_BATCH_BYTES.
	zerobusBatchBytes = envIntDefault("DUCKTAPE_ZEROBUS_BATCH_BYTES", 4*1024*1024)

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

func handleZerobusAppend(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	target, ok := parseZerobusTarget(w, r)
	if !ok {
		return
	}

	rowsAppended, bytesRead, err := ZerobusAppend(r.Context(), target, r.Body)
	if err != nil {
		errMsg := err.Error()
		handleInternalServerErrorJSON(w, ducktape.AppendResponse{Error: &errMsg}, err)
		return
	}
	writeAppendResponse(w, rowsAppended)
	slog.Info(fmt.Sprintf("zerobus append complete for table %s", target.table), slog.Int64("totalRowsAppended", rowsAppended), slog.Uint64("totalBytesRead", bytesRead), slog.Duration("elapsed", time.Since(start)))
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

// ZerobusAppend streams NDJSON rows from input into the target table and returns once Zerobus has acknowledged every
// row. Delivery is at-least-once: a failed request may have written some rows, so a retry can duplicate them. The SDK
// takes no context, so ctx is checked between batches and before the flush.
func ZerobusAppend(ctx context.Context, target zerobusTarget, input io.Reader) (rowsAppended int64, bytesRead uint64, err error) {
	sdk, err := zerobus.NewZerobusSdkWithOptions(target.endpoint, target.workspaceURL, zerobus.WithApplicationName("artie-ducktape"))
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create a zerobus client: %w", err)
	}
	defer sdk.Free()

	options := zerobus.DefaultStreamConfigurationOptions()
	options.RecordType = zerobus.RecordTypeJson
	options.FlushTimeoutMs = uint64(zerobusFlushTimeoutMs)
	stream, err := sdk.CreateStream(zerobus.TableProperties{TableName: target.table}, target.clientID, target.clientSecret, options)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to open a zerobus stream for table %s: %w", target.table, err)
	}
	// Close flushes too: after the explicit Flush below it has nothing left, and on an error path it still writes the
	// batches already queued, which at-least-once delivery allows.
	defer func() {
		if err := stream.Close(); err != nil {
			slog.Warn(fmt.Sprintf("failed to close the zerobus stream for table %s", target.table), slog.Any("err", err))
		}
	}()

	rowsAppended, bytesRead, err = ingestLines(input, func(batch []any) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err := stream.IngestRecordsOffset(batch)
		return err
	})
	if err != nil {
		return 0, 0, fmt.Errorf("failed to ingest rows into table %s: %w", target.table, err)
	}

	if err := ctx.Err(); err != nil {
		return 0, 0, fmt.Errorf("request ended before flushing rows into table %s: %w", target.table, err)
	}
	if err := stream.Flush(); err != nil {
		return 0, 0, fmt.Errorf("failed to flush rows into table %s: %w", target.table, err)
	}
	return rowsAppended, bytesRead, nil
}

// ingestLines reads NDJSON lines from input and passes them to ingest as strings, in batches capped by
// zerobusBatchRows and zerobusBatchBytes. A row larger than the byte cap goes in a batch of its own. Empty lines are
// skipped.
func ingestLines(input io.Reader, ingest func(batch []any) error) (rows int64, bytesRead uint64, err error) {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 0, min(64*1024, maxScannerBuffer)), maxScannerBuffer)

	var batch []any
	var batchBytes int
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		// Send the batch first if this row would push it over the byte cap, so a batch never exceeds the cap by a row.
		if len(batch) > 0 {
			if batchBytes+len(line) > zerobusBatchBytes {
				if err := ingest(batch); err != nil {
					return 0, 0, err
				}
				batch = nil
				batchBytes = 0
			}
		}

		batch = append(batch, string(line))
		batchBytes += len(line)
		bytesRead += uint64(len(line))
		rows++

		full := len(batch) >= zerobusBatchRows
		if batchBytes >= zerobusBatchBytes {
			full = true
		}
		if !full {
			continue
		}
		if err := ingest(batch); err != nil {
			return 0, 0, err
		}
		batch = nil
		batchBytes = 0
	}

	if err := scanner.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			return 0, 0, fmt.Errorf("row %d exceeded the scanner buffer limit of %d bytes; set DUCKTAPE_SCANNER_BUFFER env var to a larger value", rows+1, maxScannerBuffer)
		}
		return 0, 0, fmt.Errorf("failed to read request stream: %w", err)
	}

	if len(batch) > 0 {
		if err := ingest(batch); err != nil {
			return 0, 0, err
		}
	}
	return rows, bytesRead, nil
}
