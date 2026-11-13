package api

import (
	"bytes"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var testArrowSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	{Name: "name", Type: arrow.BinaryTypes.String},
}, nil)

// arrowStream returns an IPC stream with one record batch of ids [0, rows).
func arrowStream(t *testing.T, rows int) []byte {
	t.Helper()
	builder := array.NewRecordBuilder(memory.DefaultAllocator, testArrowSchema)
	defer builder.Release()
	for id := range rows {
		builder.Field(0).(*array.Int64Builder).Append(int64(id))
		builder.Field(1).(*array.StringBuilder).Append("row")
	}
	record := builder.NewRecordBatch()
	defer record.Release()

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(testArrowSchema))
	if err := writer.Write(record); err != nil {
		t.Fatalf("failed to write the record: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close the writer: %v", err)
	}
	return buf.Bytes()
}

// decodeIPC returns the schema and the row count of each record batch in an IPC stream.
func decodeIPC(t *testing.T, data []byte) (*arrow.Schema, []int64) {
	t.Helper()
	reader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("failed to read IPC: %v", err)
	}
	defer reader.Release()
	var counts []int64
	for reader.Next() {
		counts = append(counts, reader.RecordBatch().NumRows())
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("failed to read IPC: %v", err)
	}
	return reader.Schema(), counts
}

func TestReadArrowBatches(t *testing.T) {
	original := zerobusBatchRows
	t.Cleanup(func() { zerobusBatchRows = original })
	{
		// The schema goes first on its own, then each batch with the schema, sliced to zerobusBatchRows rows.
		zerobusBatchRows = 10
		var schemaIPC []byte
		var batches [][]byte
		rows, err := readArrowBatches(bytes.NewReader(arrowStream(t, 25)), func(s []byte) error {
			schemaIPC = s
			return nil
		}, func(b []byte) error {
			batches = append(batches, bytes.Clone(b))
			return nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if rows != 25 {
			t.Fatalf("expected 25 rows, got %d", rows)
		}
		schema, counts := decodeIPC(t, schemaIPC)
		if !schema.Equal(testArrowSchema) {
			t.Fatalf("expected the schema, got %s", schema)
		}
		if len(counts) != 0 {
			t.Fatalf("expected no batches with the schema, got %v", counts)
		}
		var got []int64
		for _, batch := range batches {
			_, counts := decodeIPC(t, batch)
			got = append(got, counts...)
		}
		if len(got) != 3 || got[0] != 10 || got[1] != 10 || got[2] != 5 {
			t.Fatalf("expected batches of 10, 10 and 5 rows, got %v", got)
		}
		zerobusBatchRows = original
	}
	{
		// Input that isn't an IPC stream fails before anything is sent.
		_, err := readArrowBatches(strings.NewReader("not arrow"), func([]byte) error {
			t.Fatal("start shouldn't be called")
			return nil
		}, func([]byte) error {
			t.Fatal("ingest shouldn't be called")
			return nil
		})
		if err == nil {
			t.Fatal("expected an error")
		}
	}
}
