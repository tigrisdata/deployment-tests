package main

// BenchmarkSize defines a test configuration for a specific object size
type BenchmarkSize struct {
	ObjectSize    int64  // Size in bytes
	DisplayName   string // e.g., "1 MiB", "10 MiB"
	RecordCount   int    // Records to preload (YCSB: recordcount)
	OpCount       int    // Operations for benchmark (YCSB: operationcount)
	UseMultipart  bool   // Enable multipart upload for large objects
	MultipartSize int64  // Part size for multipart uploads (0 = auto)
}

// DefaultBenchmarkSizes defines default test configurations following YCSB patterns
// Strategy: Keep dataset size roughly constant (~1 GB) across different object sizes
// Multipart uploads are enabled for objects >= 100 MiB for better performance
var DefaultBenchmarkSizes = []BenchmarkSize{
	{
		ObjectSize:    1 * 1024 * 1024,
		DisplayName:   "1 MiB",
		RecordCount:   100,
		OpCount:       1000,
		UseMultipart:  false,
		MultipartSize: 0,
	},
	{
		ObjectSize:    10 * 1024 * 1024,
		DisplayName:   "10 MiB",
		RecordCount:   100,
		OpCount:       1000,
		UseMultipart:  false,
		MultipartSize: 0,
	},
	// {
	// 	ObjectSize:    100 * 1024 * 1024, // 100 MiB
	// 	DisplayName:   "100 MiB",
	// 	RecordCount:   10,              // 10 records = ~1 GB dataset
	// 	OpCount:       100,             // 100 ops
	// 	UseMultipart:  true,            // Enable multipart for large objects
	// 	MultipartSize: 10 * 1024 * 1024, // 10 MiB parts (AWS minimum is 5 MiB)
	// },
}

// TestConfig holds configuration for the performance test
type TestConfig struct {
	BucketName      string
	BenchmarkSizes  []BenchmarkSize // Test configurations for different object sizes
	Concurrency     int
	Prefix          string
	GlobalEndpoint  string
	USEndpoints     []string
	RunConnectivity bool
	RunConsistency  bool
	RunPerformance  bool
}
