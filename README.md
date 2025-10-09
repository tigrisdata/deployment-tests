# S3 Performance Test Suite

A comprehensive Go-based performance testing tool for S3 services. This tool implements a complete test suite covering connectivity, latency, and throughput benchmarks across global and regional endpoints.

## Test Suite Overview

### **Connectivity Tests**
- **Health Check**: Tests health endpoint connectivity on each S3 endpoint (appends /admin/health)
- **S3 Connectivity**: Tests S3 service connectivity using HeadBucket operations

### **Latency Benchmarks** (10 concurrency)
- **PUT Latency**: Tests PUT operations with 1MiB, 100MiB, and 1GiB objects
- **GET Latency**: Tests GET operations with 1MiB, 100MiB, and 1GiB objects
- **GET from Remote Latency**: Tests GET operations from remote endpoints with 1MiB objects

### **Throughput Benchmarks** (10 concurrency)
- **PUT Throughput**: Tests PUT operations with 1MiB, 100MiB, and 1GiB objects
- **GET Throughput**: Tests GET operations with 1MiB, 100MiB, and 1GiB objects

## Features

- **Multi-Endpoint Testing**: Tests global and multiple US regional endpoints
- **Comprehensive Metrics**: Detailed latency percentiles (P50, P95, P99), throughput, and error rates
- **Configurable Concurrency**: Adjustable concurrent operations for realistic load testing
- **Multiple Object Sizes**: Tests with 1MiB, 100MiB, and 1GiB objects
- **Real-time Results**: Live performance metrics during test execution
- **Professional Reporting**: Detailed test results with statistical analysis

## Prerequisites

- Go 1.19 or later
- AWS credentials configured (via AWS CLI, environment variables, or IAM roles)
- Access to an S3 bucket for testing
- Network connectivity to S3 endpoints

## Installation

1. Clone or download this repository
2. Install dependencies:
   ```bash
   make deps
   ```

3. Build the application:
   ```bash
   make build
   ```

## Usage

### Basic Usage

```bash
# Run complete test suite
make run BUCKET=your-bucket-name

# Or directly
./s3-perf-test -bucket your-bucket-name
```

### Command Line Options

| Flag | Description | Default |
|------|-------------|---------|
| `-bucket` | S3 bucket name (required) | - |
| `-concurrency` | Number of concurrent operations | 10 |
| `-duration` | Test duration for throughput tests | 5m |
| `-prefix` | S3 key prefix | perf-test |
| `-global-endpoint` | Global S3 endpoint URL | - |
| `-us-endpoints` | Comma-separated US regional endpoints | - |

### Examples

**Basic test suite:**
```bash
make run BUCKET=my-test-bucket
```

**Custom concurrency and duration:**
```bash
make run-custom BUCKET=my-bucket CONCURRENCY=20 DURATION=10m
```

**Test with specific endpoints:**
```bash
make test-endpoints BUCKET=my-bucket \
  GLOBAL_ENDPOINT=https://s3.amazonaws.com \
  US_ENDPOINTS=https://s3.us-east-1.amazonaws.com,https://s3.us-west-2.amazonaws.com
```

**Quick test (1 minute):**
```bash
make quick-test BUCKET=my-bucket
```

**Direct command line:**
```bash
./s3-perf-test -bucket my-bucket \
  -concurrency 10 \
  -duration 5m \
  -global-endpoint https://s3.amazonaws.com \
  -us-endpoints https://s3.us-east-1.amazonaws.com,https://s3.us-west-2.amazonaws.com
```

## Test Results

The tool provides comprehensive performance metrics:

### Connectivity Results
```
================================================================================
CONNECTIVITY TESTS
================================================================================

Testing Global Endpoint: https://s3.amazonaws.com
  Health Check: SUCCESS - 15.2ms
  S3 Connectivity: SUCCESS - 245ms

Testing US Regional Endpoint: https://s3.us-east-1.amazonaws.com
  Health Check: SUCCESS - 12.8ms
  S3 Connectivity: SUCCESS - 198ms
```

### Latency Results
```
================================================================================
LATENCY BENCHMARKS
================================================================================

Testing Endpoint: https://s3.us-east-1.amazonaws.com
------------------------------------------------------------
PUT Latency Tests:
  Testing 1048576 bytes... Avg: 125ms, P95: 180ms, P99: 220ms
  Testing 104857600 bytes... Avg: 1.2s, P95: 1.5s, P99: 1.8s
  Testing 1073741824 bytes... Avg: 12.5s, P95: 15.2s, P99: 18.1s

GET Latency Tests:
  Testing 1048576 bytes... Avg: 98ms, P95: 145ms, P99: 175ms
  Testing 104857600 bytes... Avg: 850ms, P95: 1.1s, P99: 1.3s
  Testing 1073741824 bytes... Avg: 8.2s, P95: 10.1s, P99: 12.5s
```

### Throughput Results
```
================================================================================
THROUGHPUT BENCHMARKS
================================================================================

Testing Endpoint: https://s3.us-east-1.amazonaws.com
------------------------------------------------------------
PUT Throughput Tests:
  Testing 1048576 bytes... 45.2 MB/s, 43.1 ops/s
  Testing 104857600 bytes... 78.5 MB/s, 0.8 ops/s
  Testing 1073741824 bytes... 85.2 MB/s, 0.08 ops/s

GET Throughput Tests:
  Testing 1048576 bytes... 52.1 MB/s, 49.7 ops/s
  Testing 104857600 bytes... 92.3 MB/s, 0.9 ops/s
  Testing 1073741824 bytes... 98.7 MB/s, 0.09 ops/s
```

## AWS Credentials

The tool uses the AWS SDK for Go v2, which supports multiple credential sources:

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. AWS credentials file (`~/.aws/credentials`)
3. IAM roles (if running on EC2)
4. AWS SSO

## Performance Considerations

- **Object Size**: Larger objects increase latency but may improve throughput
- **Concurrency**: Higher concurrency increases load but may hit rate limits
- **Endpoint Selection**: Choose endpoints close to your location for better performance
- **Network Conditions**: Test results depend on network latency and bandwidth

## Rate Limits

Be aware of S3 rate limits:
- PUT/COPY/POST/DELETE: 3,500 requests per second per prefix
- GET/HEAD: 5,500 requests per second per prefix

## Cleanup

The tool automatically cleans up test objects by deleting them after testing. However, if the test is interrupted, you may need to manually clean up objects with the specified prefix.

## Development

```bash
# Install dependencies
make deps

# Build the project
make build

# Run tests
make test

# Format code
make fmt

# Run linter
make lint

# Clean build artifacts
make clean
```

## License

This project is part of the Tigris Data deployment testing suite.