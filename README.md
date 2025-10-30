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
    Convergence - Avg:       0s, P50:       0s, P95:       0s, P99:       0s
    Distribution - Immediate: 100.0%, Eventual:   0.0%, Timeout:   0.0%
  global -> iad (50 iterations)
    Convergence - Avg:       0s, P50:       0s, P95:       0s, P99:       0s
    Distribution - Immediate: 100.0%, Eventual:   0.0%, Timeout:   0.0%
  global -> ord1 (50 iterations)
    Convergence - Avg:       0s, P50:       0s, P95:       0s, P99:       0s
    Distribution - Immediate: 100.0%, Eventual:   0.0%, Timeout:   0.0%
  global -> sjc (50 iterations)
    Convergence - Avg:       0s, P50:       0s, P95:       0s, P99:       0s
    Distribution - Immediate: 100.0%, Eventual:   0.0%, Timeout:   0.0%
  SUCCESS - Read-After-Write Consistency test completed (29.750s)


PUT|LIST (List-After-Write Consistency) (10 iterations)
  global -> global (10 iterations)
    Convergence - Avg:       0s, P50:       0s, P95:       0s, P99:       0s
    Distribution - Immediate: 100.0%, Eventual:   0.0%, Timeout:   0.0%
  global -> iad (10 iterations)
    Convergence - Avg: 470.000ms, P50: 500.000ms, P95: 700.000ms, P99: 700.000ms
    Distribution - Immediate:   0.0%, Eventual: 100.0%, Timeout:   0.0%
  global -> ord1 (10 iterations)
    Convergence - Avg: 20.000ms, P50:       0s, P95: 200.000ms, P99: 200.000ms
    Distribution - Immediate:  90.0%, Eventual:  10.0%, Timeout:   0.0%
  global -> sjc (10 iterations)
    Convergence - Avg:       0s, P50:       0s, P95:       0s, P99:       0s
    Distribution - Immediate: 100.0%, Eventual:   0.0%, Timeout:   0.0%
  SUCCESS - List-After-Write Consistency test completed (11.233s)


Testing Regional Endpoint: https://iad.storage.dev

PUT|GET (Read-After-Write Consistency) (50 iterations)
  iad -> iad (50 iterations)
    Convergence - Avg:       0s, P50:       0s, P95:       0s, P99:       0s
    Distribution - Immediate: 100.0%, Eventual:   0.0%, Timeout:   0.0%
  iad -> ord1 (50 iterations)
    Convergence - Avg:       0s, P50:       0s, P95:       0s, P99:       0s
    Distribution - Immediate: 100.0%, Eventual:   0.0%, Timeout:   0.0%
  iad -> sjc (50 iterations)
    Convergence - Avg:       0s, P50:       0s, P95:       0s, P99:       0s
    Distribution - Immediate: 100.0%, Eventual:   0.0%, Timeout:   0.0%
  SUCCESS - Read-After-Write Consistency test completed (29.467s)


PUT|LIST (List-After-Write Consistency) (10 iterations)
  iad -> iad (10 iterations)
    Convergence - Avg:       0s, P50:       0s, P95:       0s, P99:       0s
    Distribution - Immediate: 100.0%, Eventual:   0.0%, Timeout:   0.0%
  iad -> ord1 (10 iterations)
    Convergence - Avg: 440.000ms, P50: 400.000ms, P95:   1.000s, P99:   1.000s
    Distribution - Immediate:   0.0%, Eventual: 100.0%, Timeout:   0.0%
  iad -> sjc (10 iterations)
    Convergence - Avg: 30.000ms, P50:       0s, P95: 200.000ms, P99: 200.000ms
    Distribution - Immediate:  80.0%, Eventual:  20.0%, Timeout:   0.0%
  SUCCESS - List-After-Write Consistency test completed (11.905s)


Testing Regional Endpoint: https://ord1.storage.dev

PUT|GET (Read-After-Write Consistency) (50 iterations)
  ord1 -> ord1 (50 iterations)
    Convergence - Avg:       0s, P50:       0s, P95:       0s, P99:       0s
    Distribution - Immediate: 100.0%, Eventual:   0.0%, Timeout:   0.0%
  ord1 -> iad (50 iterations)
    Convergence - Avg:       0s, P50:       0s, P95:       0s, P99:       0s
    Distribution - Immediate: 100.0%, Eventual:   0.0%, Timeout:   0.0%
  ord1 -> sjc (50 iterations)
    Convergence - Avg:       0s, P50:       0s, P95:       0s, P99:       0s
    Distribution - Immediate: 100.0%, Eventual:   0.0%, Timeout:   0.0%
  SUCCESS - Read-After-Write Consistency test completed (31.068s)


PUT|LIST (List-After-Write Consistency) (10 iterations)
  ord1 -> ord1 (10 iterations)
    Convergence - Avg:       0s, P50:       0s, P95:       0s, P99:       0s
    Distribution - Immediate: 100.0%, Eventual:   0.0%, Timeout:   0.0%
  ord1 -> iad (10 iterations)
    Convergence - Avg: 370.000ms, P50: 300.000ms, P95:   1.000s, P99:   1.000s
    Distribution - Immediate:   0.0%, Eventual: 100.0%, Timeout:   0.0%
  ord1 -> sjc (10 iterations)
    Convergence - Avg:       0s, P50:       0s, P95:       0s, P99:       0s
    Distribution - Immediate: 100.0%, Eventual:   0.0%, Timeout:   0.0%
  SUCCESS - List-After-Write Consistency test completed (11.114s)


Testing Regional Endpoint: https://sjc.storage.dev

PUT|GET (Read-After-Write Consistency) (50 iterations)
  sjc -> sjc (50 iterations)
    Convergence - Avg:       0s, P50:       0s, P95:       0s, P99:       0s
    Distribution - Immediate: 100.0%, Eventual:   0.0%, Timeout:   0.0%
  sjc -> iad (50 iterations)
    Convergence - Avg:       0s, P50:       0s, P95:       0s, P99:       0s
    Distribution - Immediate: 100.0%, Eventual:   0.0%, Timeout:   0.0%
  sjc -> ord1 (50 iterations)
    Convergence - Avg:       0s, P50:       0s, P95:       0s, P99:       0s
    Distribution - Immediate: 100.0%, Eventual:   0.0%, Timeout:   0.0%
  SUCCESS - Read-After-Write Consistency test completed (29.548s)


PUT|LIST (List-After-Write Consistency) (10 iterations)
  sjc -> sjc (10 iterations)
    Convergence - Avg:       0s, P50:       0s, P95:       0s, P99:       0s
    Distribution - Immediate: 100.0%, Eventual:   0.0%, Timeout:   0.0%
  sjc -> iad (10 iterations)
    Convergence - Avg: 500.000ms, P50: 500.000ms, P95: 800.000ms, P99: 800.000ms
    Distribution - Immediate:   0.0%, Eventual: 100.0%, Timeout:   0.0%
  sjc -> ord1 (10 iterations)
    Convergence - Avg: 10.000ms, P50:       0s, P95: 100.000ms, P99: 100.000ms
    Distribution - Immediate:  90.0%, Eventual:  10.0%, Timeout:   0.0%
  SUCCESS - List-After-Write Consistency test completed (11.266s)

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
    Latency    - Avg:   59.181ms, P50:   51.104ms, P95:   93.315ms, P99:  304.641ms
    Throughput -     2.41 Gbps |  301.547 ops/s | 1000 success
  Testing 10 MiB (100 records, 1000 ops)...
    Latency    - Avg:  401.494ms, P50:  397.413ms, P95:  470.478ms, P99:  551.062ms
    Throughput -     3.91 Gbps |   48.861 ops/s | 1000 success
  Testing 100 MiB (10 records, 100 ops, multipart: 10 MiB parts)...
    Latency    - Avg:     4.150s, P50:     4.177s, P95:     4.475s, P99:     4.557s
    Throughput -     3.79 Gbps |    4.736 ops/s | 100 success

GET Performance Tests:
  Testing 1 MiB (100 records, 1000 ops)...
    Latency    - Avg:   31.130ms, P50:   27.197ms, P95:   42.445ms, P99:  101.441ms
    TTFB       - Avg:   18.303ms, P50:   14.729ms, P95:   28.980ms, P99:   87.669ms
    Throughput -     4.04 Gbps |  505.508 ops/s | 1000 success
  Testing 10 MiB (100 records, 1000 ops)...
    Latency    - Avg:  387.065ms, P50:  342.567ms, P95:  848.962ms, P99:     1.234s
    TTFB       - Avg:   25.144ms, P50:   15.497ms, P95:   56.346ms, P99:  137.887ms
    Throughput -     3.78 Gbps |   47.232 ops/s | 1000 success
  Testing 100 MiB (10 records, 100 ops)...
    Latency    - Avg:     3.635s, P50:     3.630s, P95:     5.764s, P99:     6.786s
    TTFB       - Avg:  363.502ms, P50:  363.043ms, P95:  576.402ms, P99:  678.618ms
    Throughput -     3.89 Gbps |    4.865 ops/s | 97 success, 3 failed

Setup Phase: Uploading 10 source files (10.0 GiB each)...
  Progress: 1/10 files uploaded (204.197s)
  Progress: 2/10 files uploaded (206.691s)
  Progress: 3/10 files uploaded (206.721s)
  Progress: 4/10 files uploaded (207.250s)
  Progress: 5/10 files uploaded (208.751s)
  Progress: 6/10 files uploaded (209.745s)
  Progress: 7/10 files uploaded (210.602s)
  Progress: 8/10 files uploaded (211.316s)
  Progress: 9/10 files uploaded (211.737s)
  Progress: 10/10 files uploaded (212.186s)
  Completed: 10 files (100.0 GiB total) in 212.186s - 4.05 Gbps
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
  Latency    - Avg:    40.368s, P50:    40.080s, P95:    50.389s, P99:    55.594s
  TTFB       - Avg:   68.345ms, P50:   39.495ms, P95:  241.691ms, P99:  348.031ms
  Throughput - 5.03 ops/s (4.22 Gbps) | 1509 success

Write Operations (Output Segments, 1.0 MiB - 6.0 MiB):
  Latency    - Avg:   91.503ms, P50:   83.407ms, P95:  153.664ms, P99:  262.266ms
  Throughput - 5.03 ops/s (0.15 Gbps) | 1509 success

Read-After-Write Consistency:
  Convergence - Avg:      0s, P50:      0s, P95:      0s, P99:      0s
  Distribution - Immediate (<200ms): 100.0%, Eventual (>200ms):   0.0%, Failed:   0.0%
  Target (<200ms): 100.0% within target

Cleanup Phase: Removing test objects... DONE

Total Duration: 374.053s
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
