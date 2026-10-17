GO ?= go
BIN ?= claude-acp

.PHONY: build test vet fmt run clean

build:
	$(GO) build -trimpath -o $(BIN) ./cmd/claude-acp

test:
	$(GO) test -race -count=1 ./...

vet:
	$(GO) vet ./...

fmt:
	gofmt -w cmd internal tool

run:
	$(GO) run ./cmd/claude-acp serve

clean:
	rm -f $(BIN)
