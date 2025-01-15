Memory buffer instead of a file. Scan until found for get

```
goos: darwin
goarch: arm64
pkg: reduction.dev/dkv/sstable
cpu: Apple M1 Pro
BenchmarkUsingBinarySearchIndexForGet/get_in_100,000_entries-10         	1000000000	         0.009526 ns/op
BenchmarkUsingBinarySearchIndexForGet/get_in_500,000_entries-10         	1000000000	         0.004419 ns/op
BenchmarkUsingBinarySearchIndexForGet/get_in_2,500,000_entries-10       	1000000000	         0.2658 ns/op
```

Memory buffer instead of a file. Scan with binary search 16 entry spacing.

```
goos: darwin
goarch: arm64
pkg: reduction.dev/dkv/sstable
cpu: Apple M1 Pro
BenchmarkUsingBinarySearchIndexForGet/get_in_100,000_entries-10         	1000000000	         0.0000102 ns/op
BenchmarkUsingBinarySearchIndexForGet/get_in_500,000_entries-10         	1000000000	         0.0000077 ns/op
BenchmarkUsingBinarySearchIndexForGet/get_in_2,500,000_entries-10       	1000000000	         0.0000180 ns/op
```

Using a disk file with no search index. Writing the file then immediately reading it back.

```
goos: darwin
goarch: arm64
pkg: reduction.dev/dkv/sstable
cpu: Apple M1 Pro
BenchmarkUsingBinarySearchIndexForGet_DiskFile/get_in_100,000_entries-10         	1000000000	         0.2996 ns/op
BenchmarkUsingBinarySearchIndexForGet_DiskFile/get_in_500,000_entries-10         	   25898	     57437 ns/op
BenchmarkUsingBinarySearchIndexForGet_DiskFile/get_in_2,500,000_entries-10       	       1	1165220167 ns/op
```

Using a disk file with search index. Writing the file then immediately reading it back.

```
goos: darwin
goarch: arm64
pkg: reduction.dev/dkv/sstable
cpu: Apple M1 Pro
BenchmarkUsingBinarySearchIndexForGet_DiskFile/get_in_100,000_entries-10         	1000000000	         0.0000497 ns/op
BenchmarkUsingBinarySearchIndexForGet_DiskFile/get_in_500,000_entries-10         	1000000000	         0.0000715 ns/op
BenchmarkUsingBinarySearchIndexForGet_DiskFile/get_in_2,500,000_entries-10       	1000000000	         0.0000677 ns/op
```

Missing keys for Get no Bloom filter.

```
goos: darwin
goarch: arm64
pkg: reduction.dev/dkv/sstable
cpu: Apple M1 Pro
BenchmarkGetNotFound/get_in_100,000_entries-10         	1000000000	         0.0000753 ns/op
BenchmarkGetNotFound/get_in_500,000_entries-10         	1000000000	         0.0000812 ns/op
BenchmarkGetNotFound/get_in_2,500,000_entries-10       	1000000000	         0.0000899 ns/op
PASS
ok  	mitchlloyd.com/dkv/sstable	24.369s
```

Missing keys for Get with Bloom filter (3.2MB, 5 hashes)

```
goos: darwin
goarch: arm64
pkg: reduction.dev/dkv/sstable
cpu: Apple M1 Pro
BenchmarkGetNotFound/get_in_100,000_entries-10         	1000000000	         0.0000001 ns/op
BenchmarkGetNotFound/get_in_500,000_entries-10         	1000000000	         0.0000005 ns/op
BenchmarkGetNotFound/get_in_2,500,000_entries-10       	1000000000	         0.0000002 ns/op
```
