# S3 Performance Test

A Go-based performance testing tool for AWS S3 services. This tool measures the performance of various S3 operations including PUT, GET, DELETE, and LIST operations with configurable parameters.

## Features

- **Concurrent Testing**: Multi-worker concurrent testing to simulate real-world load
- **Comprehensive Metrics**: Detailed performance statistics including throughput, latency, and error rates
- **Configurable Parameters**: Customizable object sizes, number of workers, test duration, and more
- **Multiple Operations**: Tests PUT, GET, DELETE, and LIST operations
- **Real-time Monitoring**: Live performance metrics during test execution

## Prerequisites

- Go 1.19 or later
- AWS credentials configured (via AWS CLI, environment variables, or IAM roles)
- Access to an S3 bucket for testing

## Installation

1. Clone or download this repository
2. Install dependencies:
   ```bash
   go mod tidy
   ```

3. Build the application:
   ```bash
   go build -o s3-perf-test .
   ```

## Usage

### Basic Usage

```bash
./s3-perf-test -bucket your-bucket-name
```

### Command Line Options

| Flag | Description | Default |
|------|-------------|---------|
| `-bucket` | S3 bucket name (required) | - |
| `-size` | Object size in bytes | 1048576 (1MB) |
| `-objects` | Number of objects per worker | 100 |
| `-workers` | Number of concurrent workers | 4 |
| `-duration` | Test duration | 5m |
| `-prefix` | S3 key prefix | perf-test |

### Examples

**Basic performance test:**
```bash
./s3-perf-test -bucket my-test-bucket
```

**High-load test with larger objects:**
```bash
./s3-perf-test -bucket my-test-bucket -size 10485760 -workers 10 -objects 500
```

**Short-duration test:**
```bash
./s3-perf-test -bucket my-test-bucket -duration 1m -workers 2
```

**Custom prefix:**
```bash
./s3-perf-test -bucket my-test-bucket -prefix my-performance-test
```

## AWS Credentials

The tool uses the AWS SDK for Go v2, which supports multiple credential sources in the following order:

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. AWS credentials file (`~/.aws/credentials`)
3. IAM roles (if running on EC2)
4. AWS SSO

## Output

The tool provides detailed performance metrics for each operation type:

```
================================================================================
S3 PERFORMANCE TEST RESULTS
================================================================================

Operation: PutObject
  Total operations: 400
  Errors: 0 (0.00%)
  Total duration: 2m30s
  Average duration: 375ms
  Min duration: 150ms
  Max duration: 800ms
  Throughput: 2.67 ops/sec

Operation: GetObject
  Total operations: 400
  Errors: 0 (0.00%)
  Total duration: 1m45s
  Average duration: 262ms
  Min duration: 100ms
  Max duration: 600ms
  Throughput: 3.81 ops/sec
```

## Performance Considerations

- **Object Size**: Larger objects will increase PUT/GET times but may improve throughput
- **Concurrent Workers**: More workers increase load but may hit rate limits
- **Service Location**: The service will automatically detect the appropriate region

## Rate Limits

Be aware of S3 rate limits:
- PUT/COPY/POST/DELETE: 3,500 requests per second per prefix
- GET/HEAD: 5,500 requests per second per prefix

## Cleanup

The tool automatically cleans up test objects by deleting them after testing. However, if the test is interrupted, you may need to manually clean up objects with the specified prefix.

## License

This project is part of the Tigris Data deployment testing suite.
