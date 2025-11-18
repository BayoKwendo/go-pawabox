# ---- Build stage ----
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copy Go module files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy all source code (including config.yml)
COPY . .

# Build the Go binary
RUN go build -o main ./cmd/main.go


# ---- Run stage ----
FROM alpine:latest

WORKDIR /app

# Copy only the built binary and the config file
COPY --from=builder /app/main .
COPY --from=builder /app/config.yml .

# Expose Fiber (or Go app) port
EXPOSE 3007

# Start the app
CMD ["./main"]
