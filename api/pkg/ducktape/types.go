package ducktape

const (
	ExecuteRoute = "/api/execute"
	QueryRoute   = "/api/query"
	AppendRoute  = "/api/append"
	PingRoute    = "/api/ping"

	// ZerobusAppendArrowRoute streams an Arrow IPC stream (a schema, then record batches) into a Databricks Delta
	// table through Zerobus's Arrow Flight path. The schema's field names and types must match the table's.
	ZerobusAppendArrowRoute = "/api/zerobus/append-arrow"

	DuckDBConnectionStringHeader = "X-DuckDB-Connection-String"
	DuckDBDatabaseHeader         = "X-DuckDB-Database"
	DuckDBSchemaHeader           = "X-DuckDB-Schema"
	DuckDBTableHeader            = "X-DuckDB-Table"

	// ZerobusEndpointHeader is the workspace's Zerobus endpoint, such as https://<id>.zerobus.<region>.cloud.databricks.com.
	ZerobusEndpointHeader = "X-Zerobus-Endpoint"
	// ZerobusWorkspaceURLHeader is the workspace URL, used to get OAuth tokens.
	ZerobusWorkspaceURLHeader = "X-Zerobus-Workspace-URL"
	// ZerobusTableHeader is the target table as catalog.schema.table.
	ZerobusTableHeader        = "X-Zerobus-Table"
	ZerobusClientIDHeader     = "X-Zerobus-Client-ID"
	ZerobusClientSecretHeader = "X-Zerobus-Client-Secret"

	// RecommendedBufferSize is the recommended buffer size for streaming appends.
	RecommendedBufferSize = 1 << 20 // 1 MB
)

type QueryRequest struct {
	Query string `json:"query"`
	Args  []any  `json:"args"`
}

type QueryResponse struct {
	Rows  []map[string]any `json:"rows"`
	Error *string          `json:"error"`
}

type ExecuteStatement struct {
	Query string `json:"query"`
	Args  []any  `json:"args"`
}

type ExecuteRequest struct {
	Statements []ExecuteStatement `json:"statements"`
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
	Row RowMessage `json:"row"`
	// Err stops the append. Append returns it wrapped, so callers can match it with errors.Is or errors.As.
	Err error `json:"-"`
}

type AppendResponse struct {
	RowsAppended int64   `json:"rowsAppended"`
	Error        *string `json:"error"`
}
