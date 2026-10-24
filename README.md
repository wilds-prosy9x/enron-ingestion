# enron-ingestion

A Go service for ingesting the [Enron email dataset](https://www.cs.cmu.edu/~enron/), with a REST API for checking ingestion progress.

> Work in progress: the API currently returns placeholder values and no ingestion is implemented yet.

## Requirements

- Go 1.27.3+
- Docker (optional, for building the container image)

## Project layout

```
cmd/server/        Entry point: starts the HTTP server
internal/server/   HTTP routes, handlers and integration tests
Dockerfile         Multi-stage build producing a small distroless image
```

## Running locally

```bash
go run ./cmd/server
```

The server listens on `:8080` by default. Set the `ADDR` environment variable to change it, e.g. `ADDR=:9000 go run ./cmd/server`.

## Testing

```bash
go test ./...
```

The integration tests start a real HTTP server and call the API over the network.

## Docker

```bash
docker build -t enron-ingestion .
docker run --rm -p 8080:8080 enron-ingestion
```

## API

### `GET /status`

Returns the current ingestion status.

```bash
curl localhost:8080/status
```

```json
{"state": "not_started", "processed_message_count": 0}
```

| Field                     | Type    | Description                                                              |
|---------------------------|---------|--------------------------------------------------------------------------|
| `state`                   | string  | One of `not_started`, `running`, `finished`, `failed`                    |
| `processed_message_count` | integer | Number of email messages processed so far                                |
