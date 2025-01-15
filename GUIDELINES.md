# Project Coding Guidelines

## Testing

- Use github.com/stretchr/testify/assert for assertions.
- Only test public package functions (blackbox testing).
- Avoid table-driven tests.
- Don't use terms like "mock" and "test" as prefixes for variables in tests
  because the context of testing should be assumed. For instance rather than
  naming something "mockLogger" it might be "recordingLogger". Instead of naming a
  new user "testUser", just "user" is better.
