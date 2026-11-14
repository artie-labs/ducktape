package api

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	zerobus "github.com/databricks/zerobus-sdk/go"
)

// zerobusArrowInflightBatches caps the Arrow batches waiting for Zerobus to acknowledge them. With the SDK's default of
// 1,000, a single stream outran Zerobus, which stopped acknowledging until the SDK recovered and resent the batches,
// duplicating rows. Tunable via DUCKTAPE_ZEROBUS_ARROW_INFLIGHT_BATCHES.
var zerobusArrowInflightBatches = envIntDefault("DUCKTAPE_ZEROBUS_ARROW_INFLIGHT_BATCHES", 50)

// ZerobusAppendArrow streams the Arrow IPC stream in input into the target table through Zerobus's Arrow Flight path,
// zstd-compressed, and returns once Zerobus has acknowledged every row. Delivery is at-least-once: a failed request may have written some rows, so a retry can duplicate them.
func ZerobusAppendArrow(ctx context.Context, target zerobusTarget, input io.Reader) (int64, error) {
	sdk, err := zerobus.NewZerobusSdkWithOptions(target.endpoint, target.workspaceURL, zerobus.WithApplicationName("artie-ducktape"))
	if err != nil {
		return 0, fmt.Errorf("failed to create a zerobus client: %w", err)
	}
	defer sdk.Free()

	var stream *zerobus.ZerobusArrowStream
	// Close flushes too: after the explicit Flush below it has nothing left, and on an error path it still writes the
	// batches already queued, which at-least-once delivery allows.
	defer func() {
		if stream == nil {
			return
		}
		if err := stream.Close(); err != nil {
			slog.Warn(fmt.Sprintf("failed to close the zerobus arrow stream for table %s", target.table), slog.Any("err", err))
		}
	}()

	rows, err := readArrowBatches(input, func(schemaIPC []byte) error {
		options := zerobus.DefaultArrowStreamConfigurationOptions()
		options.IPCCompression = zerobus.IPCCompressionZstd
		options.MaxInflightBatches = uint64(zerobusArrowInflightBatches)
		options.FlushTimeoutMs = uint64(zerobusFlushTimeoutMs)
		var err error
		stream, err = sdk.CreateArrowStream(target.table, schemaIPC, target.clientID, target.clientSecret, options)
		if err != nil {
			return fmt.Errorf("failed to open a zerobus arrow stream for table %s: %w", target.table, err)
		}
		return nil
	}, func(batchIPC []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err := stream.IngestBatch(batchIPC)
		return err
	})
	if err != nil {
		return 0, fmt.Errorf("failed to ingest arrow batches into table %s: %w", target.table, err)
	}
	if stream == nil {
		return 0, nil
	}

	if err := ctx.Err(); err != nil {
		return 0, fmt.Errorf("request ended before flushing rows into table %s: %w", target.table, err)
	}
	if err := stream.Flush(); err != nil {
		return 0, fmt.Errorf("failed to flush rows into table %s: %w", target.table, err)
	}
	return rows, nil
}

// readArrowBatches reads the Arrow IPC stream in input. It passes the schema to start as IPC bytes holding only the
// schema, then each record batch to ingest as IPC bytes holding the schema and that batch, the forms Zerobus takes. A
// batch over zerobusBatchRows rows is sent in slices of at most that many. It returns the rows read.
func readArrowBatches(input io.Reader, start func(schemaIPC []byte) error, ingest func(batchIPC []byte) error) (int64, error) {
	reader, err := ipc.NewReader(input)
	if err != nil {
		return 0, fmt.Errorf("failed to read the arrow IPC stream: %w", err)
	}
	defer reader.Release()

	schema := reader.Schema()
	schemaIPC, err := encodeArrowIPC(schema, nil)
	if err != nil {
		return 0, err
	}
	if err := start(schemaIPC); err != nil {
		return 0, err
	}

	var rows int64
	for reader.Next() {
		record := reader.RecordBatch()
		for offset := int64(0); offset < record.NumRows(); offset += int64(zerobusBatchRows) {
			slice := record.NewSlice(offset, min(offset+int64(zerobusBatchRows), record.NumRows()))
			batchIPC, err := encodeArrowIPC(schema, slice)
			sliceRows := slice.NumRows()
			slice.Release()
			if err != nil {
				return 0, err
			}
			if err := ingest(batchIPC); err != nil {
				return 0, err
			}
			rows += sliceRows
		}
	}
	if err := reader.Err(); err != nil {
		return 0, fmt.Errorf("failed to read the arrow IPC stream: %w", err)
	}
	return rows, nil
}

// encodeArrowIPC returns an IPC stream holding schema and, when record isn't nil, that one record batch.
func encodeArrowIPC(schema *arrow.Schema, record arrow.RecordBatch) ([]byte, error) {
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if record != nil {
		if err := writer.Write(record); err != nil {
			return nil, fmt.Errorf("failed to encode an arrow batch: %w", err)
		}
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to encode arrow IPC: %w", err)
	}
	return buf.Bytes(), nil
}
