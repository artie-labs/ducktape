package ducktape

const (
	ExecuteRoute = "/api/execute"
	QueryRoute   = "/api/query"
	AppendRoute  = "/api/append"
	PingRoute    = "/api/ping"

	DuckDBConnectionStringHeader = "X-DuckDB-Connection-String"
	DuckDBDatabaseHeader         = "X-DuckDB-Database"
	DuckDBSchemaHeader           = "X-DuckDB-Schema"
	DuckDBTableHeader            = "X-DuckDB-Table"
)

type QueryRequest struct {
	Query string `json:"query"`
	Args  []any  `json:"args"`
}

type QueryResponse struct {
	Rows  []map[string]any `json:"rows"`
	Error *string          `json:"error"`
}

type ExecuteRequest struct {
	Query string `json:"query"`
	Args  []any  `json:"args"`
}

type ExecuteResponse struct {
	RowsAffectedCount int64   `json:"rowsAffected"`
	Error             *string `json:"error"`
}

func (r ExecuteResponse) LastInsertId() (int64, error) {
	return 0, nil
}

func (r ExecuteResponse) RowsAffected() (int64, error) {
	return r.RowsAffectedCount, nil
}

type RowMessage struct {
	Values []any `json:"rv"`
}

type RowMessageResult struct {
	Row   RowMessage `json:"row"`
	Error *string    `json:"error"`
}

type AppendResponse struct {
	RowsAppended int64   `json:"rowsAppended"`
	Error        *string `json:"error"`
}
