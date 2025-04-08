package kinesisfake

func (f *Fake) ExpireShardIterator(shardIterator string) {
	f.db.activeShardIterators[shardIterator] = false
}
