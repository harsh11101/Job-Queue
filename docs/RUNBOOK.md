# Operations runbook

## Health checks

| Endpoint | Use |
| --- | --- |
| `/actuator/health/readiness` | Must be `UP` before routing traffic |
| `/actuator/health/liveness` | Container liveness probe |
| `/actuator/health` | Overall status; component details only for authorized callers |

The dashboard polls `/actuator/health` and shows **Backend up** / **Backend unreachable**. If it shows unreachable while `curl` works, the browser is being refused by CORS: add the dashboard's origin to `CORS_ALLOWED_ORIGINS`.

## Metrics

Exposed at `/actuator/prometheus`, all tagged with `application`. A counter appears only after it is first incremented.

| Metric | Tags | Meaning |
| --- | --- | --- |
| `jobqueue_jobs_total` | `created` | Create requests (Micrometer name `jobqueue.jobs.created`); `created="false"` are idempotent repeats |
| `jobqueue_outbox_published_total` | `topic` | Outbox rows sent to Kafka |
| `jobqueue_outbox_publish_failed_total` | `topic` | Sends that failed; the row is retried next tick |
| `jobqueue_worker_completed_total` | `topic` | Jobs that succeeded |
| `jobqueue_worker_retry_scheduled_total` | `topic` | Failures with a retry scheduled |
| `jobqueue_worker_dlq_total` | `topic` | Jobs moved to the dead-letter queue |
| `jobqueue_worker_lock_skipped_total` | `topic` | Messages skipped because another worker held the lease |
| `jobqueue_jobs_cancelled_total` | | Jobs cancelled through the API |
| `jobqueue_worker_terminal_skipped_total` | `status` | Messages for jobs already finished or cancelled |
| `jobqueue_worker_start_skipped_total` | `topic` | Messages that could not start their attempt: the job was cancelled after the worker read it, or the message was stale |
| `jobqueue_worker_early_rescheduled_total` | `topic` | Retry messages that arrived before their due time |
| `jobqueue_worker_lease_recovered_total` | | Jobs found `PROCESSING` after their lease expired and moved on by the recovery task |
| `jobqueue_worker_lease_lost_total` | `topic` | A worker finished an attempt that had already been recovered; its result was discarded |

Suggested alerts: any increase of `jobqueue_worker_dlq_total`; a sustained rate of `jobqueue_outbox_publish_failed_total` (Kafka unreachable); any `lease_recovered` (a worker died mid-job, or jobs run longer than the 6-minute lease); any `lease_lost` (jobs run longer than the lease).

## Finding stuck work

```sql
-- Outbox rows not yet published, oldest first
SELECT status, topic, attempts, not_before, last_error, updated_at
FROM job_outbox WHERE status <> 'PUBLISHED' ORDER BY created_at LIMIT 50;

-- Jobs that have been PROCESSING for more than 10 minutes
SELECT id, type, attempt, updated_at FROM jobs
WHERE status = 'PROCESSING' AND updated_at < now() - interval '10 minutes';
```

- `FAILED` outbox rows are retried every second; check `last_error` and Kafka health.
- `PUBLISHING` rows older than 5 minutes are reclaimed automatically.
- A job left `PROCESSING` by a crashed worker is recovered automatically; see below.

## Recovery of stranded jobs

When a worker dies mid-attempt, Kafka redelivers its message within seconds. The dead worker's Redis lease is still held, so the new consumer skips and acknowledges the message, and after that no message exists for the job.

`ExpiredLeaseRecovery` runs on every replica every 30 s (`app.job-queue.worker.lease-recovery-delay-ms`). It finds jobs still `PROCESSING` after `leased_until` (attempt start + `lock-ttl`, 6 min). Each one is ended as a retryable failure with the error *Worker lease expired before the attempt finished*: the job gets its normal retry, or goes to DLQ if it was on its last attempt.

A job is therefore moving again within about `lock-ttl` + 30 s of its worker dying, at most about 6.5 minutes.

Ending an attempt is fenced: the update applies only while the job is still `PROCESSING` at that attempt number. If the original worker was only slow rather than dead, its later result is discarded (`jobqueue_worker_lease_lost_total`) and cannot overwrite the job. The retry then finds the side-effect receipt and does not repeat the effect. Replicas need no coordination, because only one party can end a given attempt.

## Cancelling a job

`POST /api/v1/jobs/{id}/cancel`, or **Cancel job** on the job's page in the dashboard.

| Job status | Result |
| --- | --- |
| `QUEUED`, `RETRY_SCHEDULED`, `DLQ` | `200`, job becomes `CANCELLED` |
| `CANCELLED` | `200`, unchanged (safe to repeat) |
| `PROCESSING`, `SUCCEEDED` | `409 INVALID_JOB_STATE` |

`CANCELLED` is final: the job never runs again, and replay returns `409`. In the same transaction the job's unsent outbox rows are marked `CANCELLED`, so they are never published. A message already on Kafka is skipped when it arrives.

Cancel and a worker starting the job update the same row, so exactly one wins. If the worker starts first, the cancel is refused with `409`; wait for the attempt to end and cancel again if the job is then retrying or in DLQ. A running job cannot be stopped mid-attempt, and cancelling never undoes an effect that already happened: if `side_effect_receipts` has a row for the job, its effect is done.

## DLQ handling

1. Find DLQ jobs: dashboard **Jobs** page filtered by *Dead letter*, or `GET /api/v1/jobs?status=DLQ`.
2. Read the cause: `GET /api/v1/jobs/{id}` (`errorMessage`, `failureClass`) and `GET /api/v1/jobs/{id}/attempts`.
3. Fix the cause: a downstream dependency, or a bad payload. Payloads cannot be edited; a bad payload needs a new job with a new idempotency key.
4. Replay: `POST /api/v1/jobs/{id}/replay` with `{"priority": "HIGH"}` or `{}` (LOW), or **Replay job** on the job's page.
   Or, if the job should never run again, cancel it (see above); a cancelled job cannot be replayed.

Replay runs **one** more attempt. The attempt counter keeps counting, so if that attempt fails the job goes straight back to DLQ instead of starting a new retry chain. Replay is safe if the original effect already happened: the side-effect receipt key is unique, so the effect is not repeated. Replaying a job that is not in DLQ returns `409 INVALID_JOB_STATE`.

## Scaling

- Every replica runs API, publisher and workers. Add replicas to scale all three.
- Consumers per lane are set by `app.job-queue.worker.{high,low,retry}-concurrency` (6 / 2 / 3) **per replica**. Total consumers on a topic beyond its partition count (`kafka.topic-partitions`, 6) sit idle; add partitions first.
- Keep `DB_POOL_SIZE × replicas` below PostgreSQL's `max_connections`, leaving headroom for migrations and admin sessions.

## Known limitations

- `worker.processing-timeout` (5 min) is checked only against the lease TTL at startup; job execution is not interrupted. A job that runs past its 6-minute lease is treated as stranded: it is retried, and the slow worker's result is discarded when it finishes. The side-effect receipt prevents a duplicate effect, but the work runs twice. Keep real job handlers well under the lease. The demo executor finishes in milliseconds.
- `jobs-dlq` is written for audit only; nothing consumes it.
- No retention is applied to `job_attempts`, published `job_outbox` rows or `side_effect_receipts`.
