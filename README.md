# enron-ingestion

A Go service that ingests the [Enron email dataset](https://www.cs.cmu.edu/~enron/) into Redis, with a REST API for starting the ingestion, checking its progress and listing the top senders.

## Requirements

- Go 1.27.3+
- Redis 7+
- Docker (optional, for building the container image and for the Redis tests)
- The dataset archive, [`enron_mail_20150507.tar.gz`](https://www.cs.cmu.edu/~enron/enron_mail_20150507.tar.gz), in a local directory or in S3 / Google Cloud Storage

## Project layout

```
cmd/server/          Entry point: reads the config and starts the HTTP server
internal/server/     HTTP routes, handlers and integration tests
internal/ingest/     Archive parsing, the runner that records messages in batches, and the starter
internal/source/     Opens the archive from local or cloud storage
internal/store/      Ingestion state and sender counts in Redis
internal/redistest/  Redis container shared by tests
Dockerfile           Multi-stage build producing a small distroless image
```

## Configuration

The server is configured with environment variables:

| Variable         | Default                      | Description                                                                                  |
|------------------|------------------------------|----------------------------------------------------------------------------------------------|
| `DATASET_BUCKET` | (required)                   | Where the archive is, e.g. `file:///data`, `s3://bucket?region=eu-north-1` or `gs://bucket` |
| `DATASET_KEY`    | `enron_mail_20150507.tar.gz` | The archive's name in the bucket                                                             |
| `DEDUP_MODE`     | `content`                    | How copies of the same email are recognised; see below                                       |
| `ADDR`           | `:8080`                      | Address the HTTP server listens on                                                           |
| `REDIS_ADDR`     | `localhost:6379`             | Redis address                                                                                |
| `REDIS_PASSWORD` | (none)                       | Redis password                                                                               |

The dataset stores the same email several times, e.g. in both the sender's `sent` and `all_documents` folders. Each copy counts towards `processed_message_count`, but only once towards `unique_message_count` and the sender counts. `DEDUP_MODE` selects how copies are recognised:

- `content`: same sender, date, subject, recipients and body. Use this for the Enron dataset, where every stored copy was given a new `Message-ID`.
- `message_id`: same `Message-ID` header, for archives that kept their original IDs.

Cloud credentials are read the usual way for each provider, e.g. `AWS_*` environment variables or `GOOGLE_APPLICATION_CREDENTIALS`.

## Running locally

```bash
docker run --rm -d --name redis -p 6379:6379 redis:7.4
DATASET_BUCKET=file://$HOME/datasets go run ./cmd/server
curl -X POST localhost:8080/start
```

If Redis isn't reachable when the server starts, it logs a warning and keeps running; requests fail with `500` until Redis is available.

## Testing

```bash
go test ./...
```

The store and integration tests start a Redis container with [Testcontainers](https://testcontainers.com/), so they need Docker; without it they are skipped. The integration tests start a real HTTP server, ingest a small generated archive and call the API over the network.

## Docker

```bash
docker build -t enron-ingestion .
docker network create enron
docker run --rm -d --name redis --network enron redis:7.4
docker run --rm -p 8080:8080 --network enron --stop-timeout 30 \
  -e REDIS_ADDR=redis:6379 -e DATASET_BUCKET=file:///data \
  -v "$HOME/datasets:/data:ro" \
  enron-ingestion
```

On `SIGTERM` the server stops accepting requests, cancels a run in progress, records the messages it has already read and marks the run `failed`. That can take up to about 20 seconds, longer than Docker's default 10-second grace period, hence `--stop-timeout 30`.

## Ingestion runs

There is one ingestion run per Redis database: once it has finished, `POST /start` is refused. A `failed` run can be started again; its data is discarded and the ingestion starts over.

Only one instance should run, but running a second one by mistake is safe. A running run holds a lease in Redis that its instance renews every 10 seconds. While the lease is held, other instances can't start a run. If the instance dies without marking the run failed, e.g. because it was killed, the lease expires after 30 seconds, and the next `POST /start` replaces the abandoned run. An instance whose run has been replaced stops it and can no longer change the stored data.

The app's Redis keys start with `{enron}:`.

## API

Errors are returned as JSON with a non-2xx status code:

```json
{"error": "ingestion already started"}
```

Unexpected errors return `500` with `{"error": "internal error"}`; the details are only logged.

### `POST /start`

Starts the ingestion in the background.

```bash
curl -X POST localhost:8080/start
```

```json
{"state": "running"}
```

| Status | Meaning                                       |
|--------|-----------------------------------------------|
| `202`  | The run has started                           |
| `409`  | A run is already in progress or has finished  |

### `GET /status`

Returns the current ingestion status.

```bash
curl localhost:8080/status
```

```json
{"state": "running", "dedup_mode": "content", "processed_message_count": 120000, "unique_message_count": 51000}
```

| Field                     | Type    | Description                                                              |
|---------------------------|---------|--------------------------------------------------------------------------|
| `state`                   | string  | One of `not_started`, `running`, `finished`, `failed`                    |
| `dedup_mode`              | string  | `DEDUP_MODE` of the current or last run; omitted before the first run    |
| `processed_message_count` | integer | Number of email messages processed so far, including copies             |
| `unique_message_count`    | integer | Number of distinct emails so far                                         |
| `error`                   | string  | Why the run failed; only present when `state` is `failed`                |

The counts are updated in batches, at least every half second while the run is in progress.

### `GET /top-senders`

Returns the 10 senders with the most distinct emails, highest count first. Sender addresses are lowercased; emails without a usable `From` address aren't counted.

```bash
curl localhost:8080/top-senders
```

```json
{"senders": [{"email": "jeff.dasovich@enron.com", "count": 6000}, {"email": "kay.mann@enron.com", "count": 5000}]}
```

| Field             | Type    | Description                                                 |
|-------------------|---------|-------------------------------------------------------------|
| `senders`         | array   | Up to 10 senders; empty before any emails are counted       |
| `senders[].email` | string  | Sender address                                              |
| `senders[].count` | integer | Number of distinct emails from the sender                   |

Senders with equal counts are listed in reverse alphabetical order.
