#!/bin/bash

export AWS_PROFILE=prod

make build

./s3-perf-test -bucket ef-test-1 \
  -global-endpoint https://t3.storage.dev \
  -us-endpoints https://iad1.storage.dev,https://sjc.storage.dev,https://ord1.storage.dev \
  -duration 10s
