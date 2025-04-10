package kinesisfake

// ExpireShardIterators marks all shard iterators with timestamps less than or equal to the current
// lastIteratorTimestamp as expired. Any future GetRecords calls with these iterators will receive
// an ExpiredIteratorException.
func (f *Fake) ExpireShardIterators() {
	f.iteratorsExpirationAt.Store(f.lastIteratorTimestamp.Load())
}
