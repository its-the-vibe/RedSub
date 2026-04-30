# RedSub

[![CI](https://github.com/its-the-vibe/RedSub/actions/workflows/ci.yaml/badge.svg)](https://github.com/its-the-vibe/RedSub/actions/workflows/ci.yaml)

A simple service written in Go that will pop json payloads from a Redis queue and publish to GCP pubsub

## Development

### Makefile targets

| Target | Description |
| ------ | ----------- |
| `make build` | Compile the Go project |
| `make test` | Run all unit tests |
| `make lint` | Run `go vet` |
| `make ci` | Run lint and tests (used in CI) |
