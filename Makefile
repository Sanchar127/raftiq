.PHONY: build test test-race coverage fmt fmt-check vet lint vuln tidy verify check install-tools

build:
	go build ./...

test:
	go test ./...

test-race:
	go test -race ./...

coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

fmt:
	gofmt -w .
	goimports -w .

fmt-check:
	@test -z "$$(gofmt -l .)" || (echo "Go files are not formatted:"; gofmt -l .; exit 1)

vet:
	go vet ./...

lint:
	golangci-lint run

vuln:
	govulncheck ./...

tidy:
	go mod tidy

verify:
	go mod verify

check: fmt-check vet lint test test-race verify vuln
	@echo "All quality checks passed."

install-tools:
	go install golang.org/x/tools/cmd/goimports@latest
	go install golang.org/x/vuln/cmd/govulncheck@latest
