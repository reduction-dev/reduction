# Project Coding Guidelines

## Testing

- Use github.com/stretchr/testify/assert assertions.
- Only use blackbox testing (test the public API of package) and use
  `<package>_test` packages.
- Prefer top-level test functions like `TestMyThing_SpecificCase` over sub tests
  or table-driven tests.
- Avoid "mock" or "test" prefixes for variables in tests.
  - Good: `user`, `url`, `job`
  - Avoid: `testUser`, `dummyURL`, `mockJob`
- Use "fakes" that implement an interface for test doubles Good names describe
  the implementation when possible (`MemoryLogger`, `FailingHttpServer`) or use
  "fake" as a prefix (`FakeSourceReader`).
- Async testing:
  - Use `assert.Eventually` and `assert.EventuallyWithT` instead of `time.Sleep`.
  - When validating goroutine order, exercise code with many iterations to build
    confidence.
  - Inject dependencies to control timing.
- Organize test files with happy-path test cases first, then degenerate test cases, then
  supporting functions and types.
- Use testify assertion messages in lieu of short comments:
  ```go
	assert.True(t, value, "comment can go here")
  ```

## Modern Go

- Go's range operator allows this pattern:

  ```go
  for i := range 10 {
    fmt.Println(i) // Prints 0 - 9
  }
  ```

- Go does _not_ support generic methods.

- Go's testing.T has a `t.Context()` method that provides a context
  that's canceled at the end of a test.

  Do this:

  ```go
  func TestXxx(t *testing.T) {
    myFunc(t.Context())
  }
  ```

  Instead of this:

  ```go
  func TestXxx(t *testing.T) {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    myFunc(ctx)
  }
  ```
