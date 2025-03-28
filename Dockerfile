ARG TARGET_ARCH=amd64

FROM golang:1.24 AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN GOOS=linux GOARCH=${TARGET_ARCH} go build -o reduction ./cmd/reduction

FROM alpine:3.18
WORKDIR /app
RUN apk --no-cache add ca-certificates

# Copy binary into container
COPY --from=builder /app/reduction /app/reduction

# Set executable permissions
RUN chmod +x /app/reduction

# Default command that will be overridden by ECS task definitions
ENTRYPOINT ["/app/reduction"]

# Default to showing help text if no command is provided
CMD ["--help"]
