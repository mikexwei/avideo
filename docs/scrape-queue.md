# Scrape Worker and Queue

The Web process never owns a Playwright browser. A detached supervisor runs one
worker process, guarded by filesystem locks. The worker owns the persistent
JavDB Chrome profile, serializes browser operations, and refreshes idle pages.
Restarting Flask does not stop the supervisor, worker, or browser.

## Process Control

```sh
.venv/bin/python -m core.worker_client start
.venv/bin/python -m core.worker_client pause
.venv/bin/python -m core.worker_client resume
.venv/bin/python -m core.worker_client stop
.venv/bin/python -m core.worker_client restart
.venv/bin/python -m core.worker_client status
```

Pause and stop finish the current film. Pause retains the browser; stop closes it
and exits the worker and supervisor. Restart waits for a safe boundary, then the
supervisor replaces the worker. Unexpected worker exits are restarted automatically.
Logs: `data/logs/scrape_worker.log`.

## Persistence

- `scrape_tasks`: pending/running/completed/failed/skipped/cancelled tasks, priority,
  last result, retry schedule, cancellation flag, and workflow checkpoints.
- `scrape_history`: an append-only record of each execution or interruption.
- `scrape_events`: durable Web workflow events, replayed using SSE event IDs.
- `scrape_settings`: process controls and quota settings.
- `scrape_worker`: worker PID, heartbeat, current task, and live state.
- `ltx_scrape_attempts`: quota accounting; existing counts survive the migration.

LTX codes are unique after removing local `-C/-U/-UC` suffixes. Existing completed
codes require rescrape confirmation. Requeueing preserves the previous result;
a failed rescrape does not erase previously extracted links. Deletion removes a
task from the visible queue, but retains its history and never deletes video files.

Before executing, the worker checks current task cancellation and current movie
existence, deletion, VR status, and excluded tags. Skipped movies consume no quota.
Quota waiting releases the browser and is revisited after settings changes.

## Scheduling

1. Immediate tasks.
2. Web workflows, video scrapes, and actress scrapes.
3. Normal LTX tasks, by descending priority and FIFO within each priority.

Normal LTX tasks use daily and hourly quotas in `America/Los_Angeles`. The default
daily cap is 200 with a normal hourly curve peaking at 17:00. Web tasks do not use
LTX quota. Immediate tasks bypass pause, scheduling, quotas, exclusion tags, VR
filtering, and artificial waits. They still need a valid movie record and a usable
JavDB login. Their attempts remain in quota accounting even when exceeding the cap.

Immediate tasks preempt at browser checkpoints, not during database commits or
file renames. Interrupted tasks return to the durable queue. Upload workflows
journal planned file moves and checkpoints; batch rescrapes checkpoint completed
codes. Worker recovery requeues interrupted tasks without resetting quota usage.

## HTTP Endpoints

All mutation endpoints use JSON. Browser control requests reject foreign Origins.

| Endpoint | Purpose |
| --- | --- |
| `GET /api/ltx/queue` | Queue items, metadata, priorities, states, and last results |
| `GET /api/ltx/status` | Worker heartbeat, process controls, quota and queue counts |
| `POST /api/ltx/scrape/enqueue` | `{codes: [...], immediate?: bool, confirm_rescrape?: bool}` |
| `POST /api/ltx/queue/bulk` | `{codes: [...], action, priority?, confirm_rescrape?}` |
| `GET /api/ltx/queue/<task_id>/history` | Execution history |
| `POST /api/worker/control` | `{action: start/resume/pause/stop/restart}` |
| `GET/PATCH /api/worker/quota` | `daily_limit`, `peak_hour`, `spread` |
| `GET /api/jobs/<task_id>/stream` | Persistent workflow SSE; supports `Last-Event-ID` |

Bulk actions: `requeue`, `immediate`, `priority`, `cancel`, `delete`.
HTTP 409 with `confirm_required: true` requires resubmitting with
`confirm_rescrape: true`; the first request changes no tasks.
