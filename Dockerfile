FROM alpine:3.18
WORKDIR /app
RUN apk --no-cache add ca-certificates curl

# Copy the pre-built binary for the target architecture
ARG TARGET_ARCH
COPY build/linux-${TARGET_ARCH}/reduction /app/reduction

# Set executable permissions
RUN chmod +x /app/reduction

# Default command that will be overridden by ECS task definitions
ENTRYPOINT ["/app/reduction"]

# Default to showing help text if no command is provided
CMD ["--help"]
