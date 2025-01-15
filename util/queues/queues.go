package queues

type popper[T any] interface {
	Pop() (T, bool)
}

func Drain[T any, Q popper[T]](queue Q) []T {
	var result []T
	for {
		val, ok := queue.Pop()
		if !ok {
			break
		}
		result = append(result, val)
	}
	return result
}

func DrainBackwards[T any, Q interface{ PopLast() (T, bool) }](queue Q) []T {
	var result []T
	for {
		val, ok := queue.PopLast()
		if !ok {
			break
		}
		result = append(result, val)
	}
	return result
}

func Push[T any](q interface{ Push(T) }, val ...T) {
	for _, v := range val {
		q.Push(v)
	}
}
