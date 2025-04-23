# Kinesis Fake

Testing directly against Kinesis is slow and subject to rate limits for
operations like `MergeShards`. This fake aims to imitate Kinesis as closely as
possible, and LLMs seem to understand how real Kinesis should work.

## Implementation Notes

The fake does not use SDK types internally because the JSON does not serialize
correctly. Specifically, the SDK expects JSON numbers for `*time.Time` types
instead of the default string serialization. Progress on
[golang/go#71497](https://github.com/golang/go/issues/71497) may eventually make
it possible to use the SDK types in the fake.
