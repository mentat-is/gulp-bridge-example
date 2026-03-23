# Bridge Example

This repository implements a small "bridge" agent example that connects a local system data source (journalctl) to a remote Gulp server. The bridge runs as an HTTP service, accepts commands from the bridge manager to start and stop ingestion tasks, reads system logs,  and forwards batched events to the gulp.

## Architecture

- On startup the application initializes the local SQLite database and ensures required tables exist.
- It checks for a saved registration record in the database. If none exists the bridge performs a registration flow with the manager service and persists a hashed local bridge token.
- If registration exists the bridge resumes any tasks that are marked as `ongoing` and restarts the corresponding ingestion workers.
- The bridge exposes a small HTTP API used by the bridge manager (Gulp) to command ingestion tasks and to perform health checks.

Error handling and lifecycle behavior

- Ingestion worker errors are logged; when a worker shuts down the bridge updates the local DB task status to `stopped` and notifies Gulp of the final task status using the `set_bridge_task_status` call.
- The bridge attempts to gracefully terminate background processes (journalctl) and flush any remaining batched events on worker shutdown.

## Configuration

The bridge reads `config.json` at module load. Important configuration keys used by the code:

- `user_id` and `password` — credentials for obtaining a gulp auth token (`/login`).
- `url_server` — base URL of the bridge manager (Gulp) API.
- `bridge_url` and `bridge_port` — how this bridge is reachable by Gulp.
- `bridge_name` — registration display name.
- `gulp_context_id` — context id attached to each event.
- `batch_size` — number of events to collect before sending a batch (default 50).

Make sure `config.json` contains these keys before starting the service.

## Exposed Endpoints

The bridge exposes these HTTP endpoints (all accept POST and require Bearer authentication):

- `/start_ingestion` — Start an ingestion task.
  - Payload must include `bridge_id`, `bridge_task_id`, and `operation_id` (and optionally `plugin_params`).
  - The bridge creates or updates a DB task record, marks it `ongoing`, and spawns an `ingestion_worker` background task.

- `/stop_ingestion` — Stop a running ingestion task.
  - Accepts `bridge_task_id` and `bridge_id` (via JSON body).
  - Updates DB task status to `stopped`, signals the worker to stop, and removes runtime tracking.

- `/health_check` — Manager health-check endpoint.
  - Returns bridge status, number of active tasks, persisted tasks and version.

Authentication: endpoints use a local registration token verification flow (the bridge stores a hashed local token in the DB and compares using constant-time comparison).

## Gulp (manager) Endpoints the Bridge Calls

The bridge calls the following manager endpoints during its operations:

- `/login` — obtains an auth token used to authenticate subsequent calls.
- `/register_bridge` — registers the bridge (sends bridge URL, name, and a generated bridge token). The bridge stores a hashed copy of the generated token in the DB.
- `/ingest_raw` — used by `ingest_raw_event` to upload batches of events (multipart with a JSON payload part and a binary chunk part).
- `/set_bridge_task_status` — used to report final task status changes (e.g., `stopped`, `failed`) back to Gulp.

All calls first obtain a gulp auth token via `/login` and include it in headers.

## ingestion_worker (detailed)

Purpose

- `ingestion_worker` reads system logs by invoking `journalctl -f -n 0 -o json`, parses each JSON line, enriches it with bridge context and timestamps, batches events and forwards them to the manager via `ingest_raw_event`.

Worker flow

- Launch `journalctl` as a subprocess and read lines asynchronously.
- For each JSON line: parse, add bridge metadata, append to the current batch.
- When batch size reaches `batch_size` configuration, call `ingest_raw_event` with the batch, then clear the batch.
- Periodically check `stop_event` to allow graceful shutdown.

Mandatory fields and annotations the worker adds (these are required and expected by the GulpDocumen):

- `gulp.operation_id` — the manager-provided operation id for correlation.
- `event.original` — the original JSON line text as string.
- `gulp.source_id` — a short identifier of the source (in this code: `journalctl`).
- `gulp.context_id` — the configured `gulp_context_id` value from `config.json`.
- `@timestamp` — the event timestamp. The code converts `__REALTIME_TIMESTAMP` to `@timestamp`.

Shutdown and failure handling

- On any worker exit the bridge updates the local DB via `update_task_status(task_id, "stopped")`.
- The worker reads any stderr output from the subprocess; if an error message is present it sets the final status reported to Gulp to `failed`, otherwise `stopped`.
- Remaining events in the batch are flushed before final exit.

## Persistence (DB)

The bridge uses a local SQLite DB (`agent.db`) with two tables:

- `registration` — stores `id`, `endpoint`, `status`, and `token_auth` (hashed bridge token).
- `tasks` — stores ingestion tasks: `id` (bridge_task_id), `bridge_id`, `param`, `operation_id`, `status`.

## Running locally

Install dependencies (example):

```bash
python -m pip install fastapi uvicorn httpx aiosqlite
```

Start the bridge:

```bash
python main.py
```

Or run with uvicorn directly:

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

## Notes and Best Practices

This is an bridge example designed to be used as astarting point to implement any bridge.
In this case we received log from `joutnalctl`, but you can retrive logs from other source in other way.

Remeber that dictionary must contain a subset of mandatory params:
- `@timestamp`: show the specific log timestamp (use Unix timestamp)
- `gulp.operation_id`: returned when the bridge manager creates an ingest task (different task can work in different operation)
- `gulp.context_id`: the context where the bridge is working (e.g., the name of machine/pc)
- `gulp.source_id`: the source where logs originates
- `event.original`: is all log message

Gulp create an index inside opensearch with ECS fields. The query_params contain all the information set by bridge manager (Gulp) regarding how log fields are mapped; however, you can send query_params empty and set mappings inside bridge.

Best practive: Fields that cannot be mapped to ECS should be placed under `gulp.unmapped` flat object. This allows you can find all unmapped data in a single object and avoids creating a separate flat field for every unmapped entry.

For more information about mappings refer to gulp plugin and mapping [documentation](https://github.com/mentat-is/gulp/blob/master/docs/plugins_and_mapping.md) 


