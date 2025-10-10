#!/bin/bash

export AWS_PROFILE=prod

make build

# Example 1: Run all tests (default behavior)
./t3-validator -bucket ef-test-1 \
  -global-endpoint https://t3.storage.dev \
  -us-endpoints https://iad1.storage.dev,https://sjc.storage.dev,https://ord1.storage.dev \
  -duration 10s

# Example 2: Run only connectivity and consistency tests
./t3-validator -bucket ef-test-1 \
  -global-endpoint https://t3.storage.dev \
  -us-endpoints https://iad1.storage.dev,https://sjc.storage.dev,https://ord1.storage.dev \
  -tests connectivity,consistency

# Example 3: Run only consistency tests
./t3-validator -bucket ef-test-1 \
  -global-endpoint https://t3.storage.dev \
  -us-endpoints https://iad1.storage.dev,https://sjc.storage.dev,https://ord1.storage.dev \
  -tests consistency

# Example 4: Run only performance benchmarks
./t3-validator -bucket ef-test-1 \
  -global-endpoint https://t3.storage.dev \
  -us-endpoints https://iad1.storage.dev,https://sjc.storage.dev,https://ord1.storage.dev \
  -tests latency,throughput \
  -duration 10s
