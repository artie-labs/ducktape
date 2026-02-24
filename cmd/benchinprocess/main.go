package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/artie-labs/ducktape/api/pkg/ducktape"
	"github.com/artie-labs/ducktape/internal/api"
	_ "github.com/duckdb/duckdb-go/v2"
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
	concurrency := flag.Int("concurrency", 1, "Number of concurrent append streams")
	numRows := flag.Int("num-rows", 1_000_000, "Number of rows to append")
	rowSize := flag.Int("row-size", 1024, "Size of each row in bytes")
	flag.Parse()

	ctx := context.Background()

	if *dsn == "" {
		log.Fatal("DSN is required")
	}

	db, err := sql.Open("duckdb", *dsn)
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("failed to ping database: %v", err)
	}

	createTableSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id BIGINT PRIMARY KEY,
		created_at TIMESTAMP,
		user_id BIGINT,
		event_type VARCHAR,
		metadata VARCHAR,
		payload VARCHAR
	);`, *table)

	_, err = db.ExecContext(ctx, createTableSQL)
	if err != nil {
		log.Fatalf("failed to create table: %v", err)
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
			startIndex := uint64(workerID * rowsPerWorker)
			generatePayload := strings.Repeat("x", *rowSize)

			// Create a pipe to stream NDJSON data
			pr, pw := io.Pipe()

			// Write to pipe in a goroutine - errors will be propagated through the pipe
			go func() {
				defer pw.Close()
				bw := bufio.NewWriterSize(pw, ducktape.RecommendedBufferSize)
				encoder := json.NewEncoder(bw)
				for j := 0; j < workerRows; j++ {
					rowIndex := startIndex + uint64(j)
					rowMsg := ducktape.RowMessage{
						Values: []any{
							rowIndex,
							"2024-11-15T10:30:00Z",
							rowIndex % 1000,
							"benchmark_event",
							fmt.Sprintf(`{"worker":%d,"index":%d}`, workerID, rowIndex),
							generatePayload,
						},
					}
					if err := encoder.Encode(rowMsg); err != nil {
						pw.CloseWithError(err)
						return
					}
				}
				if err := bw.Flush(); err != nil {
					pw.CloseWithError(err)
					return
				}
			}()

			// Wrap pipe reader with a buffer to allow read-ahead, similar to how
			// HTTP client buffers provide decoupling between writer and reader
			bufferedReader := bufio.NewReaderSize(pr, ducktape.RecommendedBufferSize)

			rowsAppended, bytesRead, err := api.Append(ctx, db, *database, *schema, *table, bufferedReader)
			workerElapsed := time.Since(startTime)

			// Atomically update counters after append is done
			bytesWrittenMu <- struct{}{}
			totalBytesWritten += bytesRead
			totalRowsAppended += uint64(rowsAppended)
			workerStats[workerID] = WorkerStatistics{
				BytesWritten: bytesRead,
				RowsAppended: uint64(rowsAppended),
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
