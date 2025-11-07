.PHONY: setup
setup:
	go work init . ./api && go mod download && go mod tidy && cd api && go mod download && go mod tidy

.PHONY: start
start:
	go run cmd/main.go

.PHONY: debug
debug:
	DUCKTAPE_LOG=debug go run cmd/main.go

.PHONY: test
test:
	go test -count 1 ./...

.PHONY: bench
bench:
	go test -bench=. ./... -benchmem
