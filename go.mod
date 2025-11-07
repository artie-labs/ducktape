module github.com/artie-labs/ducktape

go 1.25.3

require github.com/duckdb/duckdb-go/v2 v2.5.1

require (
	github.com/artie-labs/ducktape/api v0.0.0
	github.com/json-iterator/go v1.1.12
	golang.org/x/net v0.46.0
)

replace github.com/artie-labs/ducktape/api => ./api

require (
	github.com/apache/arrow-go/v18 v18.4.1 // indirect
	github.com/duckdb/duckdb-go-bindings v0.1.22 // indirect
	github.com/duckdb/duckdb-go-bindings/darwin-amd64 v0.1.22 // indirect
	github.com/duckdb/duckdb-go-bindings/darwin-arm64 v0.1.22 // indirect
	github.com/duckdb/duckdb-go-bindings/linux-amd64 v0.1.22 // indirect
	github.com/duckdb/duckdb-go-bindings/linux-arm64 v0.1.22 // indirect
	github.com/duckdb/duckdb-go-bindings/windows-amd64 v0.1.22 // indirect
	github.com/duckdb/duckdb-go/arrowmapping v0.0.24 // indirect
	github.com/duckdb/duckdb-go/mapping v0.0.24 // indirect
	github.com/go-viper/mapstructure/v2 v2.4.0 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/google/flatbuffers v25.2.10+incompatible // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	golang.org/x/exp v0.0.0-20250408133849-7e4ce0ab07d0 // indirect
	golang.org/x/mod v0.28.0 // indirect
	golang.org/x/sync v0.17.0 // indirect
	golang.org/x/sys v0.37.0 // indirect
	golang.org/x/telemetry v0.0.0-20250908211612-aef8a434d053 // indirect
	golang.org/x/text v0.30.0 // indirect
	golang.org/x/tools v0.37.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
)
