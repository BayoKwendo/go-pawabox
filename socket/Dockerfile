FROM golang:latest AS builder

WORKDIR /app

# Copy Go module files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy all source code (including config.yml)
COPY . .

# Build the Go binary
RUN go build -o main ./cmdsocket/socket_server.go

# Expose Fiber (or Go app) port
EXPOSE 3006

# Start the app
CMD ["./main"]