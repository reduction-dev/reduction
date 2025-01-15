package bg

// Task2 calls the provided func and returns a func that can be used to wait for
// completion. Unlike errgroup or WaitGroup, it's ok for many consumers to use
// wait().
func Task2[T any](fn func() (v T, err error)) func() (T, error) {
	var out T
	var outErr error
	done := make(chan struct{})
	go func() {
		out, outErr = fn()
		close(done)
	}()

	return func() (T, error) {
		<-done
		return out, outErr
	}
}
