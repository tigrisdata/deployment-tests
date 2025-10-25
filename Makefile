# S3 Performance Test Makefile

# Variables
BINARY_NAME=t3-validator
GO_VERSION=1.19

# Default target
.PHONY: all
all: build

# Build the binary
.PHONY: build
build:
	@echo "Building $(BINARY_NAME)..."
	go build -o $(BINARY_NAME)
	@echo "Build complete: $(BINARY_NAME)"

# Build for multiple platforms
.PHONY: build-all
build-all:
	@echo "Building for multiple platforms..."
	GOOS=linux GOARCH=amd64 go build -o $(BINARY_NAME)-linux-amd64
	GOOS=darwin GOARCH=amd64 go build -o $(BINARY_NAME)-darwin-amd64
	GOOS=windows GOARCH=amd64 go build -o $(BINARY_NAME)-windows-amd64.exe
	@echo "Multi-platform build complete"

# Run the complete test suite (requires BUCKET environment variable)
.PHONY: run
run:
	@if [ -z "$(BUCKET)" ]; then \
		echo "Error: BUCKET environment variable is required"; \
		echo "Usage: make run BUCKET=your-bucket-name"; \
		exit 1; \
	fi
	@echo "Running Tigris performance test suite on bucket: $(BUCKET)"
	./$(BINARY_NAME) -bucket $(BUCKET)

# Run with custom parameters
.PHONY: run-custom
run-custom:
	@if [ -z "$(BUCKET)" ]; then \
		echo "Error: BUCKET environment variable is required"; \
		echo "Usage: make run-custom BUCKET=your-bucket-name CONCURRENCY=10 DURATION=5m"; \
		exit 1; \
	fi
	@echo "Running Tigris performance test suite with custom parameters..."
	./$(BINARY_NAME) -bucket $(BUCKET) \
		-concurrency $(or $(CONCURRENCY),10) \
		-duration $(or $(DURATION),5m) \
		-prefix $(or $(PREFIX),perf-test) \
		-global-endpoint $(or $(GLOBAL_ENDPOINT),"") \
		-regional-endpoints $(or $(REGIONAL_ENDPOINTS),"")

# Install dependencies
.PHONY: deps
deps:
	@echo "Installing dependencies..."
	go mod tidy
	go mod download
	@echo "Dependencies installed"

# Run tests (if any)
.PHONY: test
test:
	@echo "Running tests..."
	go test -v ./...

# Run linter
.PHONY: lint
lint:
	@echo "Running linter..."
	golangci-lint run

# Format code
.PHONY: fmt
fmt:
	@echo "Formatting code..."
	go fmt ./...

# Clean build artifacts
.PHONY: clean
clean:
	@echo "Cleaning build artifacts..."
	rm -f $(BINARY_NAME)
	rm -f $(BINARY_NAME)-linux-amd64
	rm -f $(BINARY_NAME)-darwin-amd64
	rm -f $(BINARY_NAME)-windows-amd64.exe
	@echo "Clean complete"

# Show help
.PHONY: help
help:
	@echo "Tigris Performance Test - Available targets:"
	@echo ""
	@echo "  build        - Build the binary (default)"
	@echo "  build-all    - Build for multiple platforms"
	@echo "  run          - Run complete test suite (requires BUCKET env var)"
	@echo "  run-custom   - Run with custom parameters (requires BUCKET env var)"
	@echo "  test-endpoints - Run with specific endpoints (requires BUCKET, GLOBAL_ENDPOINT, REGIONAL_ENDPOINTS)"
	@echo "  deps         - Install dependencies"
	@echo "  test         - Run tests"
	@echo "  lint         - Run linter"
	@echo "  fmt          - Format code"
	@echo "  clean        - Clean build artifacts"
	@echo "  help         - Show this help message"
	@echo ""
	@echo "Examples:"
	@echo "  make build"
	@echo "  make run BUCKET=my-test-bucket"
	@echo "  make run-custom BUCKET=my-bucket CONCURRENCY=10 DURATION=5m"
	@echo "  make test-endpoints BUCKET=my-bucket GLOBAL_ENDPOINT=https://t3.storage.dev REGIONAL_ENDPOINTS=https://sjc.storage.dev"
	@echo "  make clean"

# Development targets
.PHONY: dev
dev: deps build
	@echo "Development setup complete"

# Quick test run (short duration)
.PHONY: quick-test
quick-test:
	@if [ -z "$(BUCKET)" ]; then \
		echo "Error: BUCKET environment variable is required"; \
		exit 1; \
	fi
	@echo "Running quick test (1 minute)..."
	./$(BINARY_NAME) -bucket $(BUCKET) -duration 1m -concurrency 2

# Test with specific endpoints
.PHONY: test-endpoints
test-endpoints:
	@if [ -z "$(BUCKET)" ] || [ -z "$(GLOBAL_ENDPOINT)" ] || [ -z "$(REGIONAL_ENDPOINTS)" ]; then \
		echo "Error: BUCKET, GLOBAL_ENDPOINT, and REGIONAL_ENDPOINTS environment variables are required"; \
		echo "Usage: make test-endpoints BUCKET=your-bucket GLOBAL_ENDPOINT=https://t3.storage.dev REGIONAL_ENDPOINTS=https://iad1.storage.dev,https://ord1.storage.dev"; \
		exit 1; \
	fi
	@echo "Running test with specific endpoints..."
	./$(BINARY_NAME) -bucket $(BUCKET) \
		-global-endpoint $(GLOBAL_ENDPOINT) \
		-regional-endpoints $(REGIONAL_ENDPOINTS) \
		-concurrency $(or $(CONCURRENCY),10) \
		-duration $(or $(DURATION),5m)
