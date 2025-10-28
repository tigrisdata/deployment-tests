# Tigris Validator Test Suite

A comprehensive Go-based performance testing tool for Tigris. This tool implements a complete test suite covering connectivity, consistency, performance, and specialized workload tests across global and regional endpoints.

## Test Suite Overview

The test suite includes four types of tests that can be run independently or together:

### **Connectivity Tests**

- **S3 Connectivity**: Tests service connectivity using HeadBucket operations

### **Consistency Tests** (50 iterations per test with statistical analysis)

- **Read-After-Write Consistency**: Tests object replication convergence across all regions
  - Measures convergence time (Avg, P95, P99)
  - Tracks immediate vs. eventual consistency distribution
  - Includes same-region and cross-region validation
- **List-After-Write Consistency**: Tests list operation consistency across all regions
  - Measures list convergence time with multiple objects
  - Validates ETag matching across regions
  - Includes same-region and cross-region validation

### **Performance Benchmarks** (configurable concurrency, default 20)

- **PUT Performance**: Tests PUT operations with 1 MiB, 10 MiB, and 100 MiB objects
  - 100 MiB objects use multipart upload with 10 MiB parts and parallel uploads
  - Collects both latency and throughput metrics in a single test run
- **GET Performance**: Tests GET operations with 1 MiB, 10 MiB, and 100 MiB objects
  - 100 MiB objects use parallel downloads
  - Includes TTFB (Time To First Byte) metrics
  - Collects both latency and throughput metrics in a single test run

### **Transcoding Workload Tests** (simulates video transcoding workloads)

- **Large File Range Reads**: Simulates encoders reading chunks from large source files (10GB+)
  - Uses HTTP range requests to read 100MB chunks
  - Measures TTFB and download latency for range requests
  - Tests parallel access to large files from multiple workers
- **Small File Burst Writes**: Simulates writing encoded video segments (1-6MB)
  - High-frequency writes of small output files
  - Measures write throughput and latency
- **Read-After-Write Consistency**: Validates immediate consistency with <200ms target
  - Tests consistency of written segments across global endpoint
  - Tracks percentage meeting latency target
  - Reports immediate vs. eventual consistency distribution
- **Configurable Duration**: Default 5-minute test with 200 parallel jobs
- **Source Files**: 10 × 10GB source files (configurable in code)

## Features

- **Configurable Test Selection**: Run specific test suites or all tests together
- **Multi-Endpoint Testing**: Tests global and multiple regional endpoints
- **Consistency Testing**: Validates read-after-write and multi-region consistency
- **Comprehensive Metrics**: Detailed latency percentiles (Avg, P95, P99), TTFB, throughput, and error rates
- **Configurable Concurrency**: Adjustable concurrent operations for realistic load testing
- **Multiple Object Sizes**: Tests with 1 MiB, 10 MiB, and 100 MiB objects
- **Optimized Performance**: Per-worker S3 clients, buffer pooling, multipart uploads/downloads
- **Memory-Efficient**: Automatic streaming uploads for large objects (>10MB) to minimize memory usage
  - Small objects (≤10MB): Fast in-memory generation and upload
  - Large objects (>10MB): Streaming generation with ~10MB chunks (uses only ~100MB memory vs 90GB for a 90GB file)
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
# Make sure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set
./t3-validator -bucket your-bucket-name
```

### Command Line Options

| Flag                  | Description                                                                                     | Default                                                                 |
| --------------------- | ----------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| `-bucket`             | S3 bucket name (required)                                                                       | -                                                                       |
| `-concurrency`        | Number of concurrent operations                                                                 | 20                                                                      |
| `-prefix`             | S3 key prefix                                                                                   | perf-test                                                               |
| `-global-endpoint`    | Global S3 endpoint URL                                                                          | https://oracle.storage.dev                                              |
| `-regional-endpoints` | Comma-separated regional endpoints                                                              | https://iad.storage.dev,https://ord.storage.dev,https://sjc.storage.dev |
| `-tests`              | Comma-separated list of tests to run: `connectivity`, `consistency`, `performance`, `transcode` | all                                                                     |

### Examples

**Basic test suite (all tests):**

```bash
./t3-validator -bucket my-bucket
```

**Run only connectivity and consistency tests:**

```bash
./t3-validator -bucket my-bucket -tests connectivity,consistency
```

**Run only consistency tests:**

```bash
./t3-validator -bucket my-bucket -tests consistency
```

**Run only performance tests:**

```bash
./t3-validator -bucket my-bucket -tests performance
```

**Run performance tests with custom concurrency:**

```bash
./t3-validator -bucket my-bucket -tests performance -concurrency 50
```

**Run transcoding workload test:**

```bash
./t3-validator -bucket my-bucket -tests transcode
```

**Run multiple test types:**

```bash
./t3-validator -bucket my-bucket -tests consistency,transcode
```

## Test Results

The tool provides comprehensive performance metrics:

### Connectivity Results

```
================================================================================
 CONNECTIVITY TESTS
================================================================================

Testing Global Endpoint: https://oracle.storage.dev
  S3 Connectivity: SUCCESS - 37.504ms

Testing Regional Endpoint: https://iad.storage.dev
  S3 Connectivity: SUCCESS - 24.337ms

Testing Regional Endpoint: https://ord.storage.dev
  S3 Connectivity: SUCCESS - 172.161ms

Testing Regional Endpoint: https://sjc.storage.dev
  S3 Connectivity: SUCCESS - 410.564ms
```

### Consistency Results

```
================================================================================
 CONSISTENCY TESTS
================================================================================

Testing Global Endpoint: https://oracle.storage.dev

PUT|GET (Read-After-Write Consistency) (50 iterations)
  global -> global (50 iterations)
    Convergence - Avg:       0s, P95:       0s, P99:       0s
    Distribution - Immediate: 100.0%, Eventual:   0.0%, Timeout:   0.0%
  global -> iad (50 iterations)
    Convergence - Avg:       0s, P95:       0s, P99:       0s
    Distribution - Immediate: 100.0%, Eventual:   0.0%, Timeout:   0.0%
  global -> ord (50 iterations)
    Convergence - Avg:       0s, P95:       0s, P99:       0s
    Distribution - Immediate: 100.0%, Eventual:   0.0%, Timeout:   0.0%
  global -> sjc (50 iterations)
    Convergence - Avg:       0s, P95:       0s, P99:       0s
    Distribution - Immediate: 100.0%, Eventual:   0.0%, Timeout:   0.0%
  SUCCESS - Read-After-Write Consistency test completed (28.100s)


PUT|LIST (List-After-Write Consistency) (10 iterations)
  global -> global (10 iterations)
    Convergence - Avg:       0s, P95:       0s, P99:       0s
    Distribution - Immediate: 100.0%, Eventual:   0.0%, Timeout:   0.0%
  global -> iad (10 iterations)
    Convergence - Avg:       0s, P95:       0s, P99:       0s
    Distribution - Immediate: 100.0%, Eventual:   0.0%, Timeout:   0.0%
  global -> ord (10 iterations)
    Convergence - Avg: 440.000ms, P95: 700.000ms, P99: 700.000ms
    Distribution - Immediate:   0.0%, Eventual: 100.0%, Timeout:   0.0%
  global -> sjc (10 iterations)
    Convergence - Avg:       0s, P95:       0s, P99:       0s
    Distribution - Immediate: 100.0%, Eventual:   0.0%, Timeout:   0.0%
  SUCCESS - List-After-Write Consistency test completed (8.404s)
```

### Performance Results

```
================================================================================
 PERFORMANCE TESTS
================================================================================

Configuration:
  Concurrency: 20
  Benchmark Sizes: 1 MiB, 10 MiB, 100 MiB

Testing Endpoint: global
------------------------------------------------------------
PUT Performance Tests:
  Testing 1 MiB (100 records, 1000 ops)...
    Latency    - Avg:   81.481ms, P95:  135.375ms, P99:  186.596ms
    Throughput -     1.83 Gbps |  228.531 ops/s | 1000 success
  Testing 10 MiB (100 records, 1000 ops)...
    Latency    - Avg:  222.576ms, P95:  341.281ms, P99:  643.780ms
    Throughput -     4.84 Gbps |   60.505 ops/s | 1000 success
  Testing 100 MiB (10 records, 100 ops, multipart: 10 MiB parts)...
    Latency    - Avg:     1.005s, P95:     1.303s, P99:     9.089s
    Throughput -     5.25 Gbps |    6.559 ops/s | 100 success

GET Performance Tests:
  Testing 1 MiB (100 records, 1000 ops)...
    Latency    - Avg:   28.458ms, P95:   41.117ms, P99:   97.337ms
    TTFB       - Avg:   19.454ms, P95:   31.146ms, P99:   88.739ms
    Throughput -     3.71 Gbps |  464.342 ops/s | 1000 success
  Testing 10 MiB (100 records, 1000 ops)...
    Latency    - Avg:  110.844ms, P95:  166.582ms, P99:  254.632ms
    TTFB       - Avg:   20.451ms, P95:   43.849ms, P99:   93.251ms
    Throughput -    12.59 Gbps |  157.419 ops/s | 1000 success
  Testing 100 MiB (10 records, 100 ops)...
    Latency    - Avg:  725.410ms, P95:     1.084s, P99:     1.609s
    TTFB       - Avg:   72.541ms, P95:  108.444ms, P99:  160.942ms
    Throughput -    15.92 Gbps |   19.902 ops/s | 100 success
```

### Transcoding Workload Results

```
================================================================================
 TRANSCODING WORKLOAD TESTS
================================================================================

Configuration:
  Source Files: 10 files, 10.0 GiB each
  Chunk Size: 100.0 MiB per read
  Segment Size: 1.0 MiB - 6.0 MiB per write
  Parallel Jobs: 200 parallel jobs
  Test Duration: 5m0s
------------------------------------------------------------
Transcoding Simulation (200 parallel jobs, 5m0s duration):

Read Operations (Range Requests, 100.0 MiB chunks):
  Latency    - Avg:     6.096s, P95:    10.411s, P99:    11.996s
  TTFB       - Avg:   83.735ms, P95:  202.869ms, P99:  370.036ms
  Throughput - 28.16 ops/s (23.62 Gbps) | 8447 success

Write Operations (Output Segments, 1.0 MiB - 6.0 MiB):
  Latency    - Avg:  699.104ms, P95:     1.071s, P99:     6.621s
  Throughput - 28.16 ops/s (0.83 Gbps) | 8447 success

Read-After-Write Consistency:
  Convergence - Avg:      0s, P95:      0s, P99:      0s
  Distribution - Immediate (<200ms): 100.0%, Eventual (>200ms):   0.0%, Failed:   0.0%
  Target (<200ms): 100.0% within target

Cleanup Phase: Removing test objects... DONE

Total Duration: 433.915s
```

## AWS Credentials

The tool uses the AWS SDK for Go v2, which supports multiple credential sources:

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. AWS credentials file (`~/.aws/credentials`)

## Using with GCS

The tool supports testing with GCS endpoints. To use with GCS, set the `-global-endpoint` flag to the GCS endpoint URL. GCS doesn't support region-based endpoints for multi-region buckets, so you will have to set the regional endpoints to empty string.

```bash
./t3-validator -bucket my-bucket -global-endpoint https://storage.googleapis.com -regional-endpoints ""
```

For details on how to setup credentials for GCS buckets, see the [GCS documentation](https://docs.cloud.google.com/storage/docs/aws-simple-migration).

## Performance Considerations

- **Object Size**: Larger objects increase latency but may improve throughput
  - 100 MiB objects automatically use multipart upload/download for better performance
- **Concurrency**: Higher concurrency increases load but may hit rate limits
  - Each worker thread has its own S3 client with isolated connection pool
- **Endpoint Selection**: Choose endpoints close to your location for better performance
- **Network Conditions**: Test results depend on network latency and bandwidth
- **Optimizations**:
  - Buffer pooling for memory efficiency
  - Parallel multipart uploads (10 MiB parts, 10 concurrent parts)
  - Parallel downloads for large objects

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
