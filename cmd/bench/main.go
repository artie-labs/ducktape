package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"golang.org/x/sync/errgroup"
	"log"
	"strings"
	"time"

	"github.com/artie-labs/ducktape/api/pkg/ducktape"
)

type WorkerStatistics struct {
	BytesWritten uint64
	RowsAppended uint64
	Elapsed      time.Duration
}

func main() {
	dsn := flag.String("dsn", "", "DuckDB connection string")
	database := flag.String("database", "benchmark", "Database name")
	schema := flag.String("schema", "main", "Schema name")
	table := flag.String("table", "benchmark_append", "Table name")
	ducktapeURL := flag.String("ducktape-url", "http://localhost:8080", "DuckTape URL")
	concurrency := flag.Int("concurrency", 1, "Number of concurrent append streams")
	numRows := flag.Int("num-rows", 1_000_000, "Number of rows to append")
	rowSize := flag.Int("row-size", 1024, "Size of each row in bytes")
	flag.Parse()

	if *dsn == "" {
		log.Fatal("DSN is required")
	}

	if *ducktapeURL == "" {
		log.Fatal("DuckTape URL is required")
	}

	ctx := context.Background()

	client := ducktape.NewClient(*ducktapeURL)
	err := client.Ping(ctx, *dsn)
	if err != nil {
		log.Fatalf("failed to ping DuckTape: %v", err)
	}

	_, err = client.Execute(ctx, ducktape.ExecuteRequest{
		Statements: []ducktape.ExecuteStatement{
			{Query: fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
					id BIGINT,
					created_at TIMESTAMP,
					user_id BIGINT,
					event_type VARCHAR,
					metadata VARCHAR,
					payload VARCHAR
				);`, *table)},
		}}, *dsn, func(r ducktape.ExecuteRequest) ([]byte, error) {
		return json.Marshal(r)
	}, func(r []byte) (*ducktape.ExecuteResponse, error) {
		var resp ducktape.ExecuteResponse
		if err := json.Unmarshal(r, &resp); err != nil {
			return nil, err
		}
		return &resp, nil
	})
	if err != nil {
		log.Fatalf("failed to execute: %v", err)
	}

	rowsPerWorker := *numRows / *concurrency
	extraRows := *numRows % *concurrency

	var (
		g                 errgroup.Group
		totalBytesWritten uint64
		totalRowsAppended uint64
		bytesWrittenMu    = make(chan struct{}, 1) // Use channel as a mutex
		workerStats       = make([]WorkerStatistics, *concurrency)
	)

	startTime := time.Now()

	for i := 0; i < *concurrency; i++ {
		workerID := i
		workerRows := rowsPerWorker
		if workerID < extraRows {
			workerRows++
		}
		g.Go(func() error {
			var rowIndex uint64 = uint64(workerID * rowsPerWorker)
			var workerRowsAppended uint64
			generatePayload := strings.Repeat("x", *rowSize)
			var workerBytesWritten uint64

			streamIterator := func(yield func(ducktape.RowMessageResult) bool) {
				for j := 0; j < workerRows; j++ {
					rowValues := []any{
						rowIndex,
						"2024-11-15T10:30:00Z",
						rowIndex % 1000,
						"benchmark_event",
						fmt.Sprintf(`{"worker":%d,"index":%d}`, workerID, rowIndex),
						generatePayload,
					}
					if !yield(ducktape.RowMessageResult{Row: ducktape.RowMessage{Values: rowValues}}) {
						return
					}
					rowIndex++
					workerRowsAppended++
				}
			}

			_, err := client.Append(
				ctx,
				*dsn,
				*database,
				*schema,
				*table,
				streamIterator,
				func(r ducktape.RowMessage) ([]byte, error) {
					bs, err := json.Marshal(r)
					if err == nil {
						workerBytesWritten += uint64(len(bs))
					}
					return bs, err
				},
				func(r []byte) (*ducktape.AppendResponse, error) {
					var resp ducktape.AppendResponse
					if err := json.Unmarshal(r, &resp); err != nil {
						return nil, err
					}
					return &resp, nil
				},
			)
			workerElapsed := time.Since(startTime)
			// Atomically update counters after append is done
			bytesWrittenMu <- struct{}{}
			totalBytesWritten += workerBytesWritten
			totalRowsAppended += workerRowsAppended
			workerStats[workerID] = WorkerStatistics{
				BytesWritten: workerBytesWritten,
				RowsAppended: workerRowsAppended,
				Elapsed:      workerElapsed,
			}
			<-bytesWrittenMu

			if err != nil {
				return fmt.Errorf("worker %d: %w", workerID, err)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		log.Fatalf("append failed: %v", err)
	}

	elapsed := time.Since(startTime)
	bytesPerSecond := float64(totalBytesWritten) / elapsed.Seconds()
	rowsPerSecond := float64(totalRowsAppended) / elapsed.Seconds()
	log.Printf("Appended %d rows (%d workers) in %v", totalRowsAppended, *concurrency, elapsed)
	log.Printf("Total bytes written: %d (%.2f MiB)", totalBytesWritten, float64(totalBytesWritten)/(1024*1024))
	log.Printf("Throughput: %.2f bytes/sec (%.2f MiB/sec)", bytesPerSecond, bytesPerSecond/(1024*1024))
	log.Printf("Throughput: %.2f rows/sec", rowsPerSecond)
	for workerID, stat := range workerStats {
		log.Printf("Worker %d: %.2f bytes/sec (%.2f MiB/sec), %.2f rows/sec, elapsed %v seconds", workerID, float64(stat.BytesWritten)/stat.Elapsed.Seconds(), float64(stat.BytesWritten)/(1024*1024*stat.Elapsed.Seconds()), float64(stat.RowsAppended)/stat.Elapsed.Seconds(), stat.Elapsed.Seconds())
	}

}
