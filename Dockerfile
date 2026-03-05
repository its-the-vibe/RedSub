FROM golang:1.26.0-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o redsub .

FROM scratch

# CA certificates are required for TLS connections to GCP Pub/Sub.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

COPY --from=builder /app/redsub /redsub

ENTRYPOINT ["/redsub", "--config", "/config.yaml"]
