FROM --platform=$BUILDPLATFORM golang:1.27.0-alpine AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o redsub .

FROM gcr.io/distroless/static-debian13:nonroot

COPY --from=builder /app/redsub /redsub

USER nonroot:nonroot

ENTRYPOINT ["/redsub", "--config", "/config.yaml"]
