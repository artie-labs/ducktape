package api

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
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
)

type zerobusTarget struct {
	endpoint     string
	workspaceURL string
	table        string
	clientID     string
	clientSecret string
}

func handleZerobusAppend(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if r.ProtoMajor != 2 {
		err := fmt.Errorf("HTTP/2 is required, got %s", r.Proto)
		errMsg := err.Error()
		handleBadRequestJSON(w, ducktape.AppendResponse{Error: &errMsg}, err)
		return
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
			return
		}
	}

	target := zerobusTarget{
		endpoint:     r.Header.Get(ducktape.ZerobusEndpointHeader),
		workspaceURL: r.Header.Get(ducktape.ZerobusWorkspaceURLHeader),
		table:        r.Header.Get(ducktape.ZerobusTableHeader),
		clientID:     r.Header.Get(ducktape.ZerobusClientIDHeader),
		clientSecret: r.Header.Get(ducktape.ZerobusClientSecretHeader),
	}

	rowsAppended, bytesRead, err := ZerobusAppend(target, r.Body)
	if err != nil {
		errMsg := err.Error()
		handleInternalServerErrorJSON(w, ducktape.AppendResponse{Error: &errMsg}, err)
		return
	}

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
	slog.Info(fmt.Sprintf("zerobus append complete for table %s", target.table), slog.Int64("totalRowsAppended", rowsAppended), slog.Uint64("totalBytesRead", bytesRead), slog.Duration("elapsed", time.Since(start)))
}

// ZerobusAppend streams NDJSON rows from input into the target table and returns once Zerobus has acknowledged every
// row. Delivery is at-least-once: a failed request may have written some rows, so a retry can duplicate them.
func ZerobusAppend(target zerobusTarget, input io.Reader) (rowsAppended int64, bytesRead uint64, err error) {
	sdk, err := zerobus.NewZerobusSdkWithOptions(target.endpoint, target.workspaceURL, zerobus.WithApplicationName("artie-ducktape"))
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create a zerobus client: %w", err)
	}
	defer sdk.Free()

	options := zerobus.DefaultStreamConfigurationOptions()
	options.RecordType = zerobus.RecordTypeJson
	stream, err := sdk.CreateStream(zerobus.TableProperties{TableName: target.table}, target.clientID, target.clientSecret, options)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to open a zerobus stream for table %s: %w", target.table, err)
	}
	// Close also flushes, so on the success path it's a no-op after the explicit Flush below.
	defer stream.Close()

	rowsAppended, bytesRead, err = ingestLines(input, func(batch []any) error {
		_, err := stream.IngestRecordsOffset(batch)
		return err
	})
	if err != nil {
		return 0, 0, fmt.Errorf("failed to ingest rows into table %s: %w", target.table, err)
	}

	if err := stream.Flush(); err != nil {
		return 0, 0, fmt.Errorf("failed to flush rows into table %s: %w", target.table, err)
	}
	return rowsAppended, bytesRead, nil
}

// ingestLines reads NDJSON lines from input and passes them to ingest as strings, in batches capped by
// zerobusBatchRows and zerobusBatchBytes. Empty lines are skipped.
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

		batch = append(batch, string(line))
		batchBytes += len(line)
		bytesRead += uint64(len(line))
		rows++

		if len(batch) < zerobusBatchRows && batchBytes < zerobusBatchBytes {
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
