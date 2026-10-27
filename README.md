# enron-ingestion

A Go service that ingests the [Enron email dataset](https://www.cs.cmu.edu/~enron/) and counts its senders in memory, with a REST API for starting the ingestion, checking its progress and listing the top senders.

## Requirements

- Go 1.27.3+
- Docker (optional, for building and running the container image)
- The dataset archive, [`enron_mail_20150507.tar.gz`](https://www.cs.cmu.edu/~enron/enron_mail_20150507.tar.gz), in a local directory or in S3 / Google Cloud Storage

## Project layout

```
cmd/server/          Entry point: reads the config and starts the HTTP server
internal/server/     HTTP routes, handlers and integration tests
internal/ingest/     Archive parsing, the runner that records messages in batches, and the starter
internal/source/     Opens the archive from local or cloud storage
internal/store/      Ingestion state and sender counts, kept in memory
Dockerfile           Multi-stage build producing a small distroless image
docker-compose.yml   The app, for running locally
```

## Configuration

The server is configured with environment variables:

| Variable         | Default                      | Description                                                                                  |
|------------------|------------------------------|----------------------------------------------------------------------------------------------|
| `DATASET_BUCKET` | (required)                   | Where the archive is, e.g. `file:///data`, `s3://bucket?region=eu-north-1` or `gs://bucket` |
| `DATASET_KEY`    | `enron_mail_20150507.tar.gz` | The archive's name in the bucket                                                             |
| `DEDUP_MODE`     | `content`                    | How copies of the same email are recognised; see below                                       |
| `ADDR`           | `:8080`                      | Address the HTTP server listens on                                                           |

The dataset stores the same email several times, e.g. in both the sender's `sent` and `all_documents` folders. Each copy counts towards `processed_message_count`, but only once towards `unique_message_count` and the sender counts. `DEDUP_MODE` selects how copies are recognised:

- `content`: same sender, date, subject, recipients and body. Use this for the Enron dataset, where every stored copy was given a new `Message-ID`. The order and case of the `To` and `Cc` addresses don't matter, nor do line endings, whitespace at the end of lines or trailing blank lines in the body.
- `message_id`: same `Message-ID` header, for archives that kept their original IDs.

Cloud credentials are read the usual way for each provider, e.g. `AWS_*` environment variables or `GOOGLE_APPLICATION_CREDENTIALS`.

## Running with Docker Compose

`docker-compose.yml` runs the app, reading the archive from a local directory:

```bash
docker compose up --build -d
curl -X POST localhost:8080/start
curl localhost:8080/status
```

By default the archive is read from the repository's parent directory; set `DATASET_DIR` to use another one, e.g. `DATASET_DIR=$HOME/datasets docker compose up --build -d`. `DATASET_KEY` and `DEDUP_MODE` can be set the same way.

The results are kept in memory, so they are lost when the container stops; see [Ingestion runs](#ingestion-runs).

## Running locally

```bash
DATASET_BUCKET=file://$HOME/datasets go run ./cmd/server
curl -X POST localhost:8080/start
```

## Testing

```bash
go test ./...
```

The tests don't need Docker or any other service. The integration tests start a real HTTP server, ingest a small generated archive and call the API over the network.

## Docker

```bash
docker build -t enron-ingestion .
docker run --rm -p 8080:8080 --stop-timeout 30 \
  -e DATASET_BUCKET=file:///data \
  -v "$HOME/datasets:/data:ro" \
  enron-ingestion
```

On `SIGTERM` the server stops accepting requests, waits up to 10 seconds for requests in progress, and cancels a run in progress. Docker's default grace period is also 10 seconds, so `--stop-timeout 30` leaves some headroom.

## Ingestion runs

There is one ingestion run per server process: once it has finished, `POST /start` is refused. A `failed` run can be started again; its data is discarded and the ingestion starts over.

The state and counts are kept in memory only. When the server stops or restarts, they are lost: `GET /status` reports `not_started` again and the dataset has to be ingested from the start. Each server process has its own state, so instances don't share runs or results.

## Design decisions

### In-memory store

The ingestion state, dedup keys and sender counts are kept in memory instead of an external store:

- The data is small: a few hundred thousand dedup keys and the sender counts fit comfortably in one process's memory.
- Ingestion is a single job. Running more instances wouldn't make it faster, and a dataset this size doesn't need several instances to serve the API.
- It keeps the service self-contained: running it locally needs nothing but the archive.

The cost is that results are lost on restart and aren't shared between instances; see [Ingestion runs](#ingestion-runs).

### Running in production

A deployment that must survive restarts or run several instances would need an external store, such as Redis, behind the `store.Store` interface. Beyond keeping the data, that store has to coordinate the instances:

- Starting a run must be atomic across instances, so that only one of them ingests the dataset. The store's `TryStart` already does this check-and-set in one step for this reason.
- A run must be leased: the instance running it renews the lease regularly, and a run whose lease has expired is reported as failed and can be started again. Otherwise an instance that dies mid-run leaves the run in `running` for good.

An earlier version of this service did both with Redis; it was replaced by the in-memory store as more complexity than the dataset needs. See the repository history before commit `9f065c7`.

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
| `503`  | The server is shutting down                   |

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
| `error`                   | string  | Why the run failed; only present when `state` is `failed`. See below     |

When a run fails, `error` is one of the following; the full error is only logged:

| `error`                             | Meaning                                                                    |
|-------------------------------------|----------------------------------------------------------------------------|
| `dataset archive not found`         | There is no `DATASET_KEY` in `DATASET_BUCKET`                              |
| `dataset archive could not be read` | The archive is corrupt or truncated, or reading it failed                  |
| `ingestion cancelled`               | The server shut down during the run                                        |
| `internal error`                    | Anything else, e.g. storage access failing                                 |

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

Senders with equal counts are listed in alphabetical order.

### `GET /healthz`

Liveness probe: reports that the process is up and serving requests. It checks no dependencies, so an unavailable store doesn't get the process restarted.

```bash
curl localhost:8080/healthz
```

```json
{"status": "ok"}
```

### `GET /readyz`

Readiness probe: reports whether the API can be served, i.e. whether the store can be read. It doesn't depend on the ingestion state; the server is ready before, during and after a run.

```bash
curl localhost:8080/readyz
```

```json
{"status": "ready"}
```

| Status | Meaning                                                   |
|--------|-----------------------------------------------------------|
| `200`  | Ready                                                     |
| `503`  | The store can't be read: `{"error": "store unavailable"}` |
