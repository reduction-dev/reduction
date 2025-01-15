package ptr

func New[T any](s T) *T {
	return &s
}
