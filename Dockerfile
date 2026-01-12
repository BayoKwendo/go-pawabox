# ---- Build stage ----
FROM golang:latest AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download


COPY . .

# RUN chown -R 1000:1000 upload_files || true
# RUN chmod -R 777 upload_files

# Build static binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o main ./cmd/main.go

# ---- Run stage ----
FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/main .
COPY --from=builder /app/config.yml .

EXPOSE 3007

CMD ["./main"]
