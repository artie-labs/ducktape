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

.PHONY: build
build:
	docker run --rm --privileged \
		-v $(PWD):/go/src/github.com/artie-labs/ducktape \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-w /go/src/github.com/artie-labs/ducktape \
		-e CGO_ENABLED=1 \
		goreleaser/goreleaser-cross:latest build --clean

.PHONY: release
release:
	docker run --rm --privileged \
		-v $(PWD):/go/src/github.com/artie-labs/ducktape \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-w /go/src/github.com/artie-labs/ducktape \
		-e CGO_ENABLED=1 \
		-e GITHUB_TOKEN \
		goreleaser/goreleaser-cross:latest release --clean
	@echo "Pushing Docker images..."
	@TAG=$$(git describe --tags --abbrev=0 2>/dev/null || echo "latest"); \
	docker push artielabs/ducktape:latest && \
	docker push artielabs/ducktape:$$TAG || true
	@echo "Release complete!"
