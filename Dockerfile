# ---- Build stage ----
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go.mod and go.sum for dependency caching
COPY go.mod go.sum ./
RUN go mod download

# Copy the entire project
COPY . .

# Build your app from cmd/main.go
RUN go build -o main ./cmd/main.go

# ---- Run stage ----
FROM alpine:latest

WORKDIR /app

# Copy built binary
COPY --from=builder /app/main .

# Expose app port (change to match your app)
EXPOSE 8080

# Run the binary
CMD ["./main"]
