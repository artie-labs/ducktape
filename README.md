<h1
 align="center">
 <img
      align="center"
      alt="Artie Transfer"
      src="https://github.com/user-attachments/assets/d85de641-4245-4795-9863-cb5082ef3881"
      style="width:100%;"
    />
</h1>

<div align="center">
  <h3>Ducktape 🦆</h3>
  <p>Lightweight REST API for DuckDB with HTTP/2 streaming support.</p>
  <a href="https://artie.com/slack"><img src="https://img.shields.io/badge/slack-@artie-blue.svg?logo=slack"/></a>
  <a href="https://github.com/artie-labs/ducktape/blob/master/LICENSE.txt"><img src="https://img.shields.io/badge/License-MIT-yellow.svg"/></a>
</div>

## What is ducktape?

Ducktape is a standalone microservice to:

- **Append**: Append rows directly into DuckDB by streaming NDJSON over HTTP/2.
- **Query**: Fetch rows from DuckDB.
- **Execute**: Run statements within a transaction.

**Why?** DuckDB's Go driver requires CGO, which breaks cross-compilation, complicates CI/CD, and bloats Docker images. Instead of rewriting the build pipelines for [Transfer](https://github.com/artie-labs/transfer), we isolated DuckDB behind a network boundary.

The performance penalty is small—**~90% of native throughput** over the network. Pure Go apps stay portable; ducktape handles the CGO.

A [native Go client](#go-client) library is included.

## How it works

```
your service → stream NDJSON over HTTP/2 → ducktape → DuckDB (local file or MotherDuck)
```

Ducktape uses:

- **HTTP/2 streaming** for high-throughput ingestion
- **DuckDB's Appender API** for fast, type-aware row insertion
- **NDJSON** as a simple, language-agnostic wire format

If your app can produce NDJSON, it can talk to ducktape.

## Performance

| Benchmark                | Throughput   |
| ------------------------ | ------------ |
| In-process DuckDB append | ~848 MiB/sec |
| Ducktape over HTTP/2     | ~757 MiB/sec |

That's **~90% of native performance**, even across the network. For real-time ingestion workloads, this was fast enough that we didn't need to embed DuckDB at all.

See [BENCHMARKS.md](BENCHMARKS.md) for detailed results.

## Quick start

### Docker

```bash
docker pull artielabs/ducktape:latest
docker run -e DUCKTAPE_LOG="debug" --rm --publish 8080:8080 --volume $PWD:/data artielabs/ducktape:latest

# absolute path in DSN is required when ducktape runs in Docker and writing to local file
curl -X POST 'http://localhost:8080/api/query' \
--header 'X-DuckDB-Connection-String: /data/test.db' \
--header 'Content-Type: application/json' \
--data '{
    "Query": "CREATE TABLE test_file (id BIGINT);"
}'
# test.db will be created in your current working directory
```

### Development

```bash
make start
# Or with debug logging
make debug
# Or manually
PORT=8080 DUCKTAPE_LOG=debug go run cmd/main.go

# Health check
curl http://localhost:8080/health

# Readiness check
curl http://localhost:8080/ready
```

Server runs on port 8080 by default.

## API usage

### Execute

Execute one or more SQL statements in a transaction:

```bash
curl -X POST http://localhost:8080/api/execute \
  -H "X-DuckDB-Connection-String: duck.db" \
  -H "Content-Type: application/json" \
  -d '{"statements": [
    {"query": "CREATE TABLE users (name TEXT, age INTEGER)"},
    {"query": "INSERT INTO users VALUES (?, ?)", "args": ["Alice", 30]},
    {"query": "INSERT INTO users VALUES (?, ?)", "args": ["Bob", 25]}
  ]}'
```

### Query

```bash
curl -X POST http://localhost:8080/api/query \
  -H "X-DuckDB-Connection-String: duck.db" \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM users WHERE name = ?", "args": ["Alice"]}'
```

#### Query after running an init script

* This example uses an init script to set up a connection to a self-hosted ducklake and then query it

```bash
# this will run the given file to set up the ducklake connection prior to running 'show schemas;'
curl -sS -X POST 'http://localhost:8080/api/query' \
--header 'X-DuckDB-Connection-String: rcfile:/sensitive/.duckdbrc' \
--header 'Content-Type: application/json' \
--data '{
    "Query": "show schemas;"
}'
```


### Append

Streams NDJSON data over HTTP/2. Each line is a `RowMessage` with a `rv` (row values) array. Use the Go client for streaming large datasets.

## Configuration

- `PORT`: Server port (default: `8080`)
- `DUCKTAPE_LOG`: Log level (`debug`, `info`, `warn`, `error`)

## Go client

- Install Go module for client.
  ```bash
  go get github.com/artie-labs/ducktape/api
  ```
- Usage:

  ```go
  import "github.com/artie-labs/ducktape/api/pkg/ducktape"

  client := ducktape.NewClient("http://localhost:8080")
  ```

- [Client source code](api/pkg/ducktape/client.go)

## License

MIT License. See [LICENSE](https://github.com/artie-labs/ducktape/blob/master/LICENSE.txt) for details.
