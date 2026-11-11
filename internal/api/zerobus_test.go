package api

import (
	"errors"
	"strings"
	"testing"
)

// collectBatches runs ingestLines over input and returns the batches it ingested.
func collectBatches(t *testing.T, input string) ([][]any, int64, uint64, error) {
	t.Helper()
	var batches [][]any
	rows, bytesRead, err := ingestLines(strings.NewReader(input), func(batch []any) error {
		batches = append(batches, batch)
		return nil
	})
	return batches, rows, bytesRead, err
}

func TestIngestLines(t *testing.T) {
	{
		// Rows below both caps go in one batch at the end, and empty lines are skipped.
		batches, rows, bytesRead, err := collectBatches(t, "{\"a\":1}\n\n{\"a\":2}\n")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if rows != 2 {
			t.Fatalf("expected 2 rows, got %d", rows)
		}
		if bytesRead != 14 {
			t.Fatalf("expected 14 bytes read, got %d", bytesRead)
		}
		if len(batches) != 1 {
			t.Fatalf("expected 1 batch, got %d", len(batches))
		}
		if batches[0][0] != `{"a":1}` {
			t.Fatalf("expected the first row as a string, got %#v", batches[0][0])
		}
	}
	{
		// The row cap splits batches.
		prev := zerobusBatchRows
		zerobusBatchRows = 2
		// Restores it if a check fails and the reset at the end of the case is skipped.
		t.Cleanup(func() { zerobusBatchRows = prev })

		batches, rows, _, err := collectBatches(t, "{}\n{}\n{}\n{}\n{}\n")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if rows != 5 {
			t.Fatalf("expected 5 rows, got %d", rows)
		}
		if len(batches) != 3 {
			t.Fatalf("expected 3 batches, got %d", len(batches))
		}
		if len(batches[2]) != 1 {
			t.Fatalf("expected the last batch to hold the 1 leftover row, got %d", len(batches[2]))
		}
		zerobusBatchRows = prev
	}
	{
		// The byte cap splits batches too.
		prev := zerobusBatchBytes
		zerobusBatchBytes = 9
		// Restores it if a check fails and the reset at the end of the case is skipped.
		t.Cleanup(func() { zerobusBatchBytes = prev })

		batches, _, _, err := collectBatches(t, "{\"a\":123}\n{\"a\":456}\n")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(batches) != 2 {
			t.Fatalf("expected 2 batches, got %d", len(batches))
		}
		zerobusBatchBytes = prev
	}
	{
		// A row that would push a batch over the byte cap starts a new batch, so no batch goes over the cap, and a row
		// larger than the cap goes alone.
		prev := zerobusBatchBytes
		zerobusBatchBytes = 15
		// Restores it if a check fails and the reset at the end of the case is skipped.
		t.Cleanup(func() { zerobusBatchBytes = prev })

		batches, rows, _, err := collectBatches(t, "{\"a\":123}\n{\"a\":456}\n{\"a\":\"0123456789\"}\n{}\n")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if rows != 4 {
			t.Fatalf("expected 4 rows, got %d", rows)
		}
		if len(batches) != 4 {
			t.Fatalf("expected 4 batches, got %d: %v", len(batches), batches)
		}
		if batches[2][0] != `{"a":"0123456789"}` {
			t.Fatalf("expected the oversized row alone in the third batch, got %v", batches[2])
		}
		zerobusBatchBytes = prev
	}
	{
		// An ingest error stops the read and comes back.
		boom := errors.New("boom")
		_, _, err := ingestLines(strings.NewReader("{}\n"), func([]any) error { return boom })
		if !errors.Is(err, boom) {
			t.Fatalf("expected the ingest error, got %v", err)
		}
	}
	{
		// A row longer than the scanner buffer says how to raise it.
		prev := maxScannerBuffer
		maxScannerBuffer = 8
		// Restores it if a check fails and the reset at the end of the case is skipped.
		t.Cleanup(func() { maxScannerBuffer = prev })

		_, _, _, err := collectBatches(t, "{\"a\":\"too long\"}\n")
		if err == nil {
			t.Fatal("expected a scanner buffer error")
		}
		if !strings.Contains(err.Error(), "DUCKTAPE_SCANNER_BUFFER") {
			t.Fatalf("expected the error to name DUCKTAPE_SCANNER_BUFFER, got %v", err)
		}
		maxScannerBuffer = prev
	}
}
