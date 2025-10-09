# S3 Performance Test Makefile

# Variables
BINARY_NAME=s3-perf-test
MAIN_FILE=main.go
GO_VERSION=1.19

# Default target
.PHONY: all
all: build

# Build the binary
.PHONY: build
build:
	@echo "Building $(BINARY_NAME)..."
	go build -o $(BINARY_NAME) $(MAIN_FILE)
	@echo "Build complete: $(BINARY_NAME)"

# Build for multiple platforms
.PHONY: build-all
build-all:
	@echo "Building for multiple platforms..."
	GOOS=linux GOARCH=amd64 go build -o $(BINARY_NAME)-linux-amd64 $(MAIN_FILE)
	GOOS=darwin GOARCH=amd64 go build -o $(BINARY_NAME)-darwin-amd64 $(MAIN_FILE)
	GOOS=windows GOARCH=amd64 go build -o $(BINARY_NAME)-windows-amd64.exe $(MAIN_FILE)
	@echo "Multi-platform build complete"

# Run the application with default parameters (requires BUCKET environment variable)
.PHONY: run
run:
	@if [ -z "$(BUCKET)" ]; then \
		echo "Error: BUCKET environment variable is required"; \
		echo "Usage: make run BUCKET=your-bucket-name"; \
		exit 1; \
	fi
	@echo "Running S3 performance test on bucket: $(BUCKET)"
	./$(BINARY_NAME) -bucket $(BUCKET)

# Run with custom parameters
.PHONY: run-custom
run-custom:
	@if [ -z "$(BUCKET)" ]; then \
		echo "Error: BUCKET environment variable is required"; \
		echo "Usage: make run-custom BUCKET=your-bucket-name WORKERS=10 SIZE=10485760"; \
		exit 1; \
	fi
	@echo "Running S3 performance test with custom parameters..."
	./$(BINARY_NAME) -bucket $(BUCKET) \
		-workers $(or $(WORKERS),4) \
		-size $(or $(SIZE),1048576) \
		-objects $(or $(OBJECTS),100) \
		-duration $(or $(DURATION),5m) \
		-prefix $(or $(PREFIX),perf-test)

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
	@echo "S3 Performance Test - Available targets:"
	@echo ""
	@echo "  build        - Build the binary (default)"
	@echo "  build-all    - Build for multiple platforms"
	@echo "  run          - Run with default parameters (requires BUCKET env var)"
	@echo "  run-custom   - Run with custom parameters (requires BUCKET env var)"
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
	@echo "  make run-custom BUCKET=my-bucket WORKERS=10 SIZE=10485760"
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
	./$(BINARY_NAME) -bucket $(BUCKET) -duration 1m -workers 2 -objects 10
