package kinesisfake

// ExpireShardIterators marks all shard iterators with timestamps less than or equal to the current
// lastIteratorTimestamp as expired. Any future GetRecords calls with these iterators will receive
// an ExpiredIteratorException.
func (f *Fake) ExpireShardIterators() {
	f.iteratorsExpirationAt.Store(f.lastIteratorTimestamp.Load())
}

// SetGetRecordsError sets an error that will be returned by GetRecords calls.
// Set to nil to clear the error.
func (f *Fake) SetGetRecordsError(err error) {
	f.getRecordsError = err
}
