.PHONY: build test test-integration vet lint fmt up down

build:
	go build ./...

test:
	go test ./...

# test-integration runs the PostgreSQL testcontainers suite (storage, migrations).
test-integration:
	go test -tags integration -count=1 ./internal/...

vet:
	go vet ./...

# lint mounts the repository; the modules come from the GitHub origin (Go proxy).
lint:
	docker run --rm -v "$(CURDIR)":/src -w /src golangci/golangci-lint:latest-alpine golangci-lint run ./...

fmt:
	gofmt -l .

up:
	docker compose -f deploy/docker-compose.yml --profile app up -d

down:
	docker compose -f deploy/docker-compose.yml down -v
