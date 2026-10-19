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

# lint mounts the whole services/ directory: the replace directives to
# ../cbl-kit and ../cbl-protos resolve inside the container.
lint:
	docker run --rm -v "$(CURDIR)/..":/src -w /src/$(notdir $(CURDIR)) golangci/golangci-lint:latest-alpine golangci-lint run ./...

fmt:
	gofmt -l .

up:
	docker compose -f deploy/docker-compose.yml --profile app up -d

down:
	docker compose -f deploy/docker-compose.yml down -v
