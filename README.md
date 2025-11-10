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
  <h3>Ducktape</h3>
  <p>Lightweight REST API for DuckDB with HTTP/2 streaming support.</p>
  <a href="https://artie.com/slack"><img src="https://img.shields.io/badge/slack-@artie-blue.svg?logo=slack"/></a>
  <a href="https://github.com/artie-labs/duck/blob/master/LICENSE.txt"><img src="https://img.shields.io/badge/License-MIT-yellow.svg"/></a>
</div>

## Features

- **Execute**: Run DDL/DML queries that don't return results
- **Query**: Fetch rows from DuckDB
- **Append**: Stream data via HTTP/2 with NDJSON format
- **Go Client**: Native Go client library included

## Quick start

### Docker
```bash
docker run -e DUCKTAPE_LOG="debug" --rm --publish 8080:8080 --volume $PWD:/data artielabs/ducktape:latest

curl -X POST 'http://localhost:8080/api/query' \
--header 'X-DuckDB-Connection-String: data/test.db' \
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
```

Server runs on port 8080 by default.

## API usage

### Execute

```bash
curl -X POST http://localhost:8080/api/execute \
  -H "X-DuckDB-Connection-String: /path/to/duck.db" \
  -H "Content-Type: application/json" \
  -d '{"query": "CREATE TABLE users (name TEXT)", "args": []}'
```

### Query

```bash
curl -X POST http://localhost:8080/api/query \
  -H "X-DuckDB-Connection-String: /path/to/duck.db" \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM users WHERE name = ?", "args": ["Alice"]}'
```

### Append

Streams NDJSON data over HTTP/2. Each line is a `RowMessage` with a `rv` (row values) array. Use the Go client for streaming large datasets.

## Go client

```bash
go get github.com/artie-labs/ducktape/api
```

```go
import "github.com/artie-labs/ducktape/api/pkg/ducktape"

client := ducktape.NewClient("http://localhost:8080")
```

## Configuration

- `PORT`: Server port (default: `8080`)
- `DUCKTAPE_LOG`: Log level (`debug`, `info`, `warn`, `error`)


## License

MIT License. See [LICENSE](https://github.com/artie-labs/ducktape/blob/master/LICENSE.txt) for details.
