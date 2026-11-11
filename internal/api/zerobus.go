package api

import (
	"bufio"
	"errors"
	"fmt"
	"io"
)

var (
	// zerobusBatchRows is the most rows sent to Zerobus in one ingest call. Tunable via DUCKTAPE_ZEROBUS_BATCH_ROWS.
	zerobusBatchRows = envIntDefault("DUCKTAPE_ZEROBUS_BATCH_ROWS", 10_000)

	// zerobusBatchBytes is the most row bytes sent to Zerobus in one ingest call, kept well under gRPC message limits.
	// Tunable via DUCKTAPE_ZEROBUS_BATCH_BYTES.
	zerobusBatchBytes = envIntDefault("DUCKTAPE_ZEROBUS_BATCH_BYTES", 4*1024*1024)
)

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
