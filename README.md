# 🦆 Ducktape

**The quack-tacular HTTP API for DuckDB that'll get your data ducks in a row!**

## What the Duck?

Ducktape is a feather-light HTTP server that lets you interact with DuckDB without getting your webbed feet wet. No more waddling through complicated database setups—just fire up the server and start quacking... er, querying!

## Features (Our Finest Plumage)

- **Execute**: Make waves with queries that don't return data (DDL, DML)
- **Query**: Dive deep and fetch rows from your data pond
- **Append**: Stream data like a duck taking to water (with HTTP/2 support!)
- **Go Client**: A mallard-ble client library that speaks fluent duck

## Quack Start

### Running the Server

Don't be a sitting duck—get started in seconds:

```bash
make start
# Or for debug logging (to see all the quacking under the hood)
make debug
# Or set your own PORT and log level
PORT=8080 DUCKTAPE_LOG=debug go run cmd/main.go
```

The server will be paddling along on port 8080 by default. Duck yeah!

### Using the API

#### Execute a Query (No results, just action!)

```bash
curl -X POST http://localhost:8080/api/execute \
  -H "X-DuckDB-Connection-String: /path/to/duck.db" \
  -H "Content-Type: application/json" \
  -d '{"query": "CREATE TABLE pond (duck_name TEXT)", "args": []}'
```

#### Query Your Data Pond

```bash
curl -X POST http://localhost:8080/api/query \
  -H "X-DuckDB-Connection-String: /path/to/duck.db" \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM pond WHERE duck_name = ?", "args": ["Donald"]}'
```

#### Append Data (Streaming like a pro)

Stream your data in NDJSON (Newline Delimited JSON) format over HTTP/2—each line is a `RowMessage` with a `rv` (row values) array. This lets you pipe massive datasets without ruffling any feathers. Use the Go client for this one, it's ducky!

## The Flock (Go Client)

## Installation

You can fetch the latest version of Ducktape with Go modules:

```bash
go get github.com/artie-labs/ducktape/api
```

```go
import "github.com/artie-labs/ducktape/api/pkg/ducktape"

client := ducktape.NewClient("http://localhost:8080")

// Execute, query, and append to your heart's content!
// This client won't leave you feeling like a lame duck.
```

## Environment Variables

- `PORT`: Where the server nests (default: `8080`)
- `DUCKTAPE_LOG`: Log level—`debug`, `info`, `warn`, or `error`

## Why Ducktape?

Because when you need to stick data together, nothing beats ducktape! It's the only tool that:

- Doesn't quack under pressure
- Makes data migration a breeze (no migration pun intended... wait, that's geese)
- Keeps your database connections from going south for the winter
- Is absolutely un-bird-lievable

## Health Check

Make sure your duck is still alive and quacking:

```bash
curl http://localhost:8080/health
```

If you get "OK", your duck's in good shape!

## License

Artie Transfer is licensed under ELv2. Please see the [LICENSE](https://github.com/artie-labs/ducktape/blob/master/LICENSE.txt) file for additional information. If you have any licensing questions please email hi@artie.com.

---

_Built with love by the fine fowl at [Artie Labs](https://www.artie.com/). No ducks were harmed in the making of this API._
