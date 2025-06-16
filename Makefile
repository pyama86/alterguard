.PHONY: build test clean lint fmt vet deps docker-build docker-push help release release-snapshot release-check

# Variables
BINARY_NAME=alterguard
VERSION?=$(shell git describe --tags --always --dirty)
LDFLAGS=-ldflags "-s -w -X github.com/pyama86/alterguard/cmd.version=${VERSION}"
DOCKER_IMAGE=alterguard
DOCKER_TAG?=latest

# Default target
all: fmt vet test build

# Build the binary
build:
	@echo "Building ${BINARY_NAME}..."
	go build ${LDFLAGS} -o ${BINARY_NAME} .

# Run tests
test:
	@echo "Running tests..."
	go test -v -race ./...

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	go test -v -race -coverprofile=coverage.out ./...

# Clean build artifacts
clean:
	@echo "Cleaning..."
	rm -f ${BINARY_NAME}
	rm -rf dist/

# Format code
fmt:
	@echo "Formatting code..."
	go fmt ./...

# Vet code
vet:
	@echo "Vetting code..."
	go vet ./...

# Lint code (requires golangci-lint)
lint:
	@echo "Linting code..."
	golangci-lint run

# Install dependencies
deps:
	@echo "Installing dependencies..."
	go mod download
	go mod tidy
	which goreleaser &> /dev/null || \
		{ echo "goreleaser not found, installing..."; \
		  go install github.com/goreleaser/goreleaser/v2@latest; }

# Update dependencies
deps-update:
	@echo "Updating dependencies..."
	go get -u ./...
	go mod tidy

# Build Docker image (deprecated - use goreleaser)
docker-build:
	@echo "Building Docker image..."
	@echo "Note: Consider using 'make release' with GoReleaser instead"
	docker build -t ${DOCKER_IMAGE}:${DOCKER_TAG} .

# Push Docker image (deprecated - use goreleaser)
docker-push: docker-build
	@echo "Pushing Docker image..."
	@echo "Note: Consider using 'make release' with GoReleaser instead"
	docker push ${DOCKER_IMAGE}:${DOCKER_TAG}

# Run locally with example config
run-example:
	@echo "Running with example configuration..."
	@echo "Note: Set DATABASE_DSN and SLACK_WEBHOOK_URL environment variables"
	./alterguard run --common-config examples/config-common.yaml --tasks-config examples/tasks.yaml --dry-run

# Docker environment commands
docker-up:
	@echo "Starting Docker environment..."
	docker-compose up -d --build --pull=never
	@echo "Waiting for MySQL to be ready..."
	docker-compose exec mysql mysqladmin ping -h localhost --silent
	@echo "Docker environment is ready!"

docker-down:
	@echo "Stopping Docker environment..."
	docker-compose down

docker-logs:
	@echo "Showing Docker logs..."
	docker-compose logs -f

run-docker-all: docker-down docker-up run-docker-exec run-docker-swap run-docker-cleanup

# Run alterguard in Docker environment
run-docker-exec: docker-up
	@echo "Running alterguard in Docker environment..."
	docker-compose exec alterguard /app/alterguard run --common-config examples/docker/config-common.yaml --tasks-config examples/docker/tasks.yaml

# Run alterguard swap in Docker environment
run-docker-swap: docker-up
	@echo "Running alterguard swap in Docker environment..."
	docker-compose exec alterguard /app/alterguard swap large_table --common-config examples/docker/config-common.yaml --tasks-config examples/docker/tasks.yaml

# Run alsterguard cleanup in Docker environment
run-docker-cleanup: docker-up
	@echo "Running alterguard cleanup in Docker environment..."
	docker-compose exec alterguard /app/alterguard cleanup large_table --drop-table --drop-triggers --common-config examples/docker/config-common.yaml --tasks-config examples/docker/tasks.yaml

# Run pt-osc directly in Docker environment
run-pt-osc-docker:
	@echo "Running pt-online-schema-change directly in Docker environment..."
	docker-compose exec pt-toolkit pt-online-schema-change --help

# Connect to MySQL in Docker environment
mysql-connect:
	@echo "Connecting to MySQL in Docker environment..."
	docker-compose exec mysql mysql -u testuser -ptestpassword testdb

# Install development tools
install-tools:
	@echo "Installing development tools..."
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install github.com/goreleaser/goreleaser@latest

# Security scan
security:
	@echo "Running security scan..."
	gosec ./...

# Install gosec if not present
install-gosec:
	@echo "Installing gosec..."
	go install github.com/securego/gosec/v2/cmd/gosec@latest

# Release build using GoReleaser
release:
	@echo "Building release with GoReleaser..."
	goreleaser release --clean

# Release snapshot (for testing)
release-snapshot:
	@echo "Building snapshot release with GoReleaser..."
	goreleaser release --snapshot --clean

# Check GoReleaser configuration
release-check:
	@echo "Checking GoReleaser configuration..."
	goreleaser check

# Help
help:
	@echo "Available targets:"
	@echo "  build            - Build the binary"
	@echo "  test             - Run tests"
	@echo "  test-coverage    - Run tests with coverage report"
	@echo "  clean            - Clean build artifacts"
	@echo "  fmt              - Format code"
	@echo "  vet              - Vet code"
	@echo "  lint             - Lint code (requires golangci-lint)"
	@echo "  deps             - Install dependencies"
	@echo "  deps-update      - Update dependencies"
	@echo "  docker-build     - Build Docker image (deprecated - use goreleaser)"
	@echo "  docker-push      - Push Docker image (deprecated - use goreleaser)"
	@echo "  run-example      - Run with example configuration"
	@echo "  install-tools    - Install development tools"
	@echo "  security         - Run security scan"
	@echo "  install-gosec    - Install gosec security scanner"
	@echo "  release          - Build release with GoReleaser"
	@echo "  release-snapshot - Build snapshot release with GoReleaser"
	@echo "  release-check    - Check GoReleaser configuration"
	@echo "  help             - Show this help message"
