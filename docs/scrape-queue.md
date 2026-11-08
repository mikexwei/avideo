# Asynchronous workflows and service management

AVideo runs three independently controlled processes.

- Web serves the existing library UI, synchronous local operations, Admin and SSE.
- Event Consumer advances workflows and commits business data. Browser requests and model calls release the workflow until a durable result arrives.
- Browser Service alone owns Playwright and the persistent JavDB profile. Every browser operation enters its durable command queue and obeys service cooldown, including highest-priority work and login operations.

LTX is disabled for this phase. Its HTTP endpoints return 410. Existing pending or running LTX tasks become `SUSPENDED`; completed results and history remain stored.

## Start and control

Start Web with `./avideo_server.sh --start`. Open `/admin` for service controls, workflows, commands, result events, execution history and cooldown settings. Admin has no login, as configured for this deployment. Foreign-Origin mutation requests are rejected.

```sh
.venv/bin/python -m core.worker_client start --service consumer
.venv/bin/python -m core.worker_client start --service browser
.venv/bin/python -m core.worker_client status --service consumer
.venv/bin/python -m core.worker_client status --service browser
.venv/bin/python -m core.worker_client pause --service consumer
.venv/bin/python -m core.worker_client resume --service consumer
.venv/bin/python -m core.worker_client restart --service consumer
.venv/bin/python -m core.worker_client stop --service consumer
```

The same actions apply to `--service browser`. The default service is Consumer, preserving the original worker command entry point. Each service has an exclusive process lock and a separate supervisor. Logs are `data/logs/consumer.log` and `data/logs/browser.log`.

New service controls initially have `desired=stop`. Start both explicitly for the first run. Submitting work does not override an explicitly stopped service. A service whose desired state is run is automatically restarted after an unexpected exit.

Consumer pause stops workflow advancement and new dependency dispatch. Already issued browser commands continue, including queued commands, and save results. Consumer stop drains in-flight model calls into durable results before exiting. Its supervisor then exits. Restarting Consumer recovers local stages and model calls while leaving browser commands and the browser process intact.

Browser pause retains the browser session and finishes the current command. Stop and restart finish the current command before closing the profile. Process termination interrupts at scraper checkpoints. Interrupted attempts remain history and return to the command queue. Login expiry or manual verification pauses Browser Service; complete the browser interaction and resume it. Login-check and open-login control commands remain available while paused and still obey cooldown.

The old `core.scrape_worker` entry point delegates to the Consumer supervisor. It no longer owns a browser or imports the Web application to execute workflows.

## Workflow and event storage

- `scrape_tasks` retains stable public task IDs, previous results and execution history links. `execution_id` identifies one run; retry starts a new execution and preserves local recovery journals.
- `checkpoint_json` stores the current stage, per-film batch results and file move journals. Local preparation uses `PREPARING`, external waits use `WAITING`, and ready stages use `PENDING`.
- `dependency_commands` stores browser and model/image commands, execution ID, stage, priority, owner and attempt number. A unique execution/stage key prevents duplicate submission.
- `dependency_results` stores the immutable result of each attempt and its acknowledgement. Retryable failures are retained but atomically marked as handled while their command is requeued.
- `scrape_events` stores frontend SSE progress scoped to the current execution, so retries never replay an earlier completion. These events do not drive business execution or acknowledge dependency results.
- `service_state` stores independent process state, heartbeat, pause and restart generation.
- `workflow_targets` serializes workflows targeting the same film. Synchronous edits remain available; snapshots prevent stale external results from overwriting them.
- `admin_history` records service controls, settings changes, cancellation, priority changes and retries.

Command insertion and entering the waiting stage commit together. Browser completion and result publication commit together. Consumer acknowledges a result while saving it in the durable workflow continuation. Business metadata/field writes and the next stage then commit in one transaction, so a crash between acknowledgement and business processing can resume from the saved response.

Upload reception, file placement and initial insertion complete synchronously before returning the workflow ID. If Web exits during local preparation, Consumer resumes the saved upload plan after its owning PID disappears. Subsequent stages are browser scrape, metadata, asynchronous image/series work, translation, short title and local rename. Batch rescrapes save each film's result and continue independently.

Downloads and model calls use two bounded threads inside Consumer. They publish results; business updates happen on the Consumer thread. New external commands are not dispatched while Consumer is paused. Slow model calls do not block browser-result consumption.

## Validation, priority and cooling

The browser checks cancellation, execution version and the current film or actor snapshot at access checkpoints. Consumer rechecks the snapshot before applying results. Film snapshots include relations, so deletion, code changes, replacement, manual titles, actor changes or tag changes invalidate stale work. Such results remain visible in history. Re-request or retry after reviewing the changed target.

Web work defaults to priority 0; automatic video/actor scans use -1000. Explicit highest-tier tasks use the former forced flag solely for ranking. Within a tier, larger numeric priority runs first and equal priorities use FIFO. A higher-priority command may preempt at browser checkpoints. It still waits for the reserved cooldown.

Admin exposes seconds for `interval_min`, `interval_max`, `backoff_base` and `backoff_max`. Defaults are 32.5–107.5 seconds between command starts, with exponential failure backoff from 60 to 900 seconds. Existing scraper page stabilization waits also apply inside Browser Service. Page readiness timeouts remain technical waits.

`next_allowed_at` is saved before browser access, so a crash or preemption preserves the interval. Updating parameters affects the next reservation and leaves the current deadline in force. All values must be finite, within 1–86400 seconds and have minimum no greater than maximum. Login walls or manual verification cannot be bypassed by priority.

## HTTP API

| Endpoint | Behavior |
| --- | --- |
| `POST /api/videos/add-auto` | Synchronous local preparation, then 202 with `job_id` |
| `POST /api/videos/rescrape` | 202 with `job_id` and selected codes |
| `POST /api/videos/<code>/rescrape` | 202 with `job_id` |
| `GET /api/jobs/<id>` | Task, checkpoint, status and decoded result |
| `GET /api/jobs/<id>/stream` | Durable SSE supporting `Last-Event-ID` and `after` |
| `GET /api/admin/services` | Both service states and browser settings |
| `POST /api/admin/services/<consumer-or-browser>/control` | JSON `action` |
| `GET/PATCH /api/admin/browser/settings` | Cooldown settings |
| `POST /api/admin/browser/login-status` | 202 with asynchronous task ID |
| `POST /api/admin/browser/open-login` | 202 with asynchronous task ID |
| `GET /api/admin/<workflows-or-commands-or-events-or-history-or-audit>` | Paginated list with `limit`, `offset`, optional `status` or `task_id` |
| `POST /api/admin/workflows/<id>/<cancel-or-retry-or-priority>` | Workflow management; priority uses JSON `priority` |
| `POST /api/admin/events/<id>/redeliver` | Retain pending delivery or create a safe retry for a final failed event |
| `/api/ltx/*` | 410 while LTX is disabled |

Worker compatibility control routes now address Consumer. Local manual upload, replacement, deletion, renaming and metadata editing retain synchronous API behavior. `WorkerClient` calls now return task IDs; they do not wait for browser results. CLI scans and title translation submit durable tasks and return, with progress available in Admin. The daily pipeline queues a durable coordinator that waits for video tasks, then actor tasks, then translation tasks without blocking the Consumer.

Acknowledged successful events cannot be replayed. Failed work retries with a new execution ID; failed film events in a completed batch create a new single-film workflow. Earlier retryable attempts cannot be replayed after a later attempt succeeds.

## Migration and recovery

Before switching, stop the old Web and mixed worker at a safe boundary and back up SQLite using its backup API. This deployment created `data/avideo.db.bak.20261106_three_process` before migration. `RuntimeStore` adds tables and the execution column idempotently, preserving old histories, SSE IDs and business results. LTX quota data remains stored and unused.

For a failed local workflow, inspect its error and move journal, correct the target conflict or external dependency, then retry from Admin. File renaming refuses to overwrite an existing destination. Keep source, target and journal until reconciliation finishes.

A code rollback alone cannot replay new waiting workflows through the former mixed worker. Before any rollback, stop both new services, preserve the current database and files, and reconcile in-flight workflows. The pre-migration backup is usable only when restoring it will not discard subsequent uploads, edits or file moves. After new business changes, perform a forward repair or reconcile those changes before using a backup.
