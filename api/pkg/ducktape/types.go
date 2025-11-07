package ducktape

const (
	ExecuteRoute = "/api/execute"
	QueryRoute   = "/api/query"
	AppendRoute  = "/api/append"

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
	RowsAffected int64   `json:"rowsAffected"`
	Error        *string `json:"error"`
}

type RowMesssage struct {
	Values []any `json:"rv"`
}

type AppendResponse struct {
	RowsAppended int64   `json:"rowsAppended"`
	Error        *string `json:"error"`
}
