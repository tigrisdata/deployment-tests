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

- **Large File Range Reads**: Simulates encoders reading chunks from large source files (90GB+)
  - Uses HTTP range requests to read 200MB chunks
  - Measures TTFB and download latency for range requests
  - Tests parallel access to large files from multiple workers
- **Small File Burst Writes**: Simulates writing encoded video segments (1-6MB)
  - High-frequency writes of small output files
  - Measures write throughput and latency
- **Read-After-Write Consistency**: Validates immediate consistency with <200ms target
  - Tests consistency of written segments across global endpoint
  - Tracks percentage meeting latency target
  - Reports immediate vs. eventual consistency distribution
- **Configurable Duration**: Default 5-minute test with 100 parallel jobs
- **Source Files**: 10 × 90GB source files (configurable in code)

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

| Flag                  | Description                                                                        | Default                                                                 |
| --------------------- | ---------------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| `-bucket`             | S3 bucket name (required)                                                          | -                                                                       |
| `-concurrency`        | Number of concurrent operations                                                    | 20                                                                      |
| `-prefix`             | S3 key prefix                                                                      | perf-test                                                               |
| `-global-endpoint`    | Global S3 endpoint URL                                                             | https://oracle.storage.dev                                              |
| `-regional-endpoints` | Comma-separated regional endpoints                                                 | https://iad.storage.dev,https://ord.storage.dev,https://sjc.storage.dev |
| `-tests`              | Comma-separated list of tests to run: `connectivity`, `consistency`, `performance`, `transcode` | all                                                                     |

### Examples

**Basic test suite (all tests):**

```bash
./t3-validator -bucket my-test-bucket \
  -global-endpoint https://t3.storage.dev \
  -regional-endpoints https://iad1.storage.dev,https://sjc.storage.dev
```

**Run only connectivity and consistency tests:**

```bash
./t3-validator -bucket my-bucket \
  -global-endpoint https://t3.storage.dev \
  -regional-endpoints https://iad1.storage.dev,https://sjc.storage.dev \
  -tests connectivity,consistency
```

**Run only consistency tests:**

```bash
./t3-validator -bucket my-bucket \
  -global-endpoint https://t3.storage.dev \
  -regional-endpoints https://iad1.storage.dev,https://sjc.storage.dev \
  -tests consistency
```

**Run only performance tests (skip connectivity and consistency):**

```bash
./t3-validator -bucket my-bucket \
  -global-endpoint https://t3.storage.dev \
  -regional-endpoints https://iad1.storage.dev,https://sjc.storage.dev \
  -tests performance
```

**Run performance tests with custom concurrency:**

```bash
./t3-validator -bucket my-bucket \
  -global-endpoint https://t3.storage.dev \
  -regional-endpoints https://iad1.storage.dev,https://sjc.storage.dev \
  -tests performance \
  -concurrency 50
```

**Run transcoding workload test:**

```bash
./t3-validator -bucket my-bucket \
  -global-endpoint https://oracle.storage.dev \
  -tests transcode
```

**Run multiple test types:**

```bash
./t3-validator -bucket my-bucket \
  -global-endpoint https://oracle.storage.dev \
  -tests consistency,transcode
```

## Test Results

The tool provides comprehensive performance metrics:

### Connectivity Results

```
================================================================================
CONNECTIVITY TESTS
================================================================================

Testing Global Endpoint: https://t3.storage.dev
  S3 Connectivity: SUCCESS - 245ms

Testing Regional Endpoint: https://sjc.storage.dev
  S3 Connectivity: SUCCESS - 198ms
```

### Consistency Results

```
================================================================================
CONSISTENCY TESTS
================================================================================

Testing Global Endpoint: https://t3.storage.dev

PUT|GET (Read-After-Write Consistency) (50 iterations)
  global -> global (50 iterations)
    Convergence - Avg:       0s, P95:       0s, P99:       0s
    Distribution - Immediate: 100.0%, Eventual:   0.0%, Timeout:   0.0%
  global -> iad (50 iterations)
    Convergence - Avg: 406.000ms, P95: 900.000ms, P99:   1.100s
    Distribution - Immediate:   0.0%, Eventual: 100.0%, Timeout:   0.0%
  global -> ord1 (50 iterations)
    Convergence - Avg:  7.000ms, P95: 100.000ms, P99: 200.000ms
    Distribution - Immediate:  95.0%, Eventual:   5.0%, Timeout:   0.0%
  global -> sjc (50 iterations)
    Convergence - Avg:       0s, P95:       0s, P99:       0s
    Distribution - Immediate: 100.0%, Eventual:   0.0%, Timeout:   0.0%
  SUCCESS - Read-After-Write Consistency test completed (145.779s)

PUT|LIST (List-After-Write Consistency) (50 iterations)
  global -> global (50 iterations)
    Convergence - Avg:       0s, P95:       0s, P99:       0s
    Distribution - Immediate: 100.0%, Eventual:   0.0%, Timeout:   0.0%
  global -> iad (50 iterations)
    Convergence - Avg: 500.000ms, P95:   1.000s, P99:   1.500s
    Distribution - Immediate:   0.0%, Eventual: 100.0%, Timeout:   0.0%
  global -> ord1 (50 iterations)
    Convergence - Avg:       0s, P95:       0s, P99:       0s
    Distribution - Immediate: 100.0%, Eventual:   0.0%, Timeout:   0.0%
  global -> sjc (50 iterations)
    Convergence - Avg:       0s, P95:       0s, P99:       0s
    Distribution - Immediate: 100.0%, Eventual:   0.0%, Timeout:   0.0%
  SUCCESS - List-After-Write Consistency test completed (52.3s)
```

### Performance Results

```
================================================================================
 PERFORMANCE TESTS
================================================================================

Testing Endpoint: https://t3.storage.dev
------------------------------------------------------------
PUT Performance Tests:
  Testing 1 MiB (100 records, 1000 ops)...
    Latency    - Avg:   76.848ms, P95:  122.042ms, P99:  164.746ms
    Throughput -  224.674 MB/s |  224.674 ops/s | 1000 success
  Testing 10 MiB (100 records, 1000 ops)...
    Latency    - Avg:  186.293ms, P95:  250.002ms, P99:  382.832ms
    Throughput -  847.265 MB/s |   84.726 ops/s | 1000 success
  Testing 100 MiB (10 records, 100 ops, multipart: 10 MiB parts)...
    Latency    - Avg:  833.991ms, P95:  986.061ms, P99:     2.554s
    Throughput -  729.186 MB/s |    7.292 ops/s | 100 success

GET Performance Tests:
  Testing 1 MiB (100 records, 1000 ops)...
    Latency    - Avg:   29.874ms, P95:   47.641ms, P99:   91.357ms
    TTFB       - Avg:   20.464ms, P95:   37.175ms, P99:   73.371ms
    Throughput -  467.864 MB/s |  467.864 ops/s | 1000 success
  Testing 10 MiB (100 records, 1000 ops)...
    Latency    - Avg:  119.839ms, P95:  165.933ms, P99:  260.861ms
    TTFB       - Avg:   24.100ms, P95:   54.530ms, P99:   96.673ms
    Throughput - 1319.319 MB/s |  131.932 ops/s | 1000 success
  Testing 100 MiB (10 records, 100 ops)...
    Latency    - Avg:  750.610ms, P95:     1.024s, P99:     1.117s
    TTFB       - Avg:   75.061ms, P95:  102.399ms, P99:  111.651ms
    Throughput - 2035.193 MB/s |   20.352 ops/s | 100 success
```

### Transcoding Workload Results

```
================================================================================
 TRANSCODING WORKLOAD TESTS
================================================================================

Configuration:
  Source Files: 10 × 90.0 GiB
  Chunk Size: 200.0 MiB (range reads)
  Segment Size: 1.0 MiB - 6.0 MiB
  Parallel Jobs: 100
  Duration: 5m0s

------------------------------------------------------------
Setup Phase: Uploading 10 source files (90 GB each)...
  Progress: 10/10 files uploaded
  Completed: 10 files (900.0 GiB total) in 15m23.456s

------------------------------------------------------------
Transcoding Simulation (100 parallel jobs, 5m0s duration):

Read Operations (Range Requests, 200.0 MiB chunks):
  Latency    - Avg:  245.123ms, P95:  456.789ms, P99:  892.345ms
  TTFB       - Avg:   45.678ms, P95:   89.123ms, P99:  156.789ms
  Throughput - 6.17 ops/s | 45823 success

Write Operations (Output Segments, 1.0 MiB - 6.0 MiB):
  Latency    - Avg:   87.234ms, P95:  145.678ms, P99:  234.567ms
  Throughput - 123.45 ops/s | 37046 success

Read-After-Write Consistency:
  Convergence - Avg:   12.345ms, P95:   78.901ms, P99:  156.789ms
  Distribution - Immediate (<200ms): 98.5%, Eventual (>200ms):  1.5%, Failed:  0.0%
  Target (<200ms): 98.5% within target

Cleanup Phase: Removing test objects... DONE

Total Duration: 21m57s
```

## AWS Credentials

The tool uses the AWS SDK for Go v2, which supports multiple credential sources:

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. AWS credentials file (`~/.aws/credentials`)

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
