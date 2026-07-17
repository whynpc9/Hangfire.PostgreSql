# Kingbase ASP.NET Core 3.1 E2E Validation

## Validation Target

- Date: 2026-07-17
- Database server: KingbaseES (PostgreSQL-compatible deployment)
- Application target framework: ASP.NET Core 3.1 (`netcoreapp3.1`)
- Hangfire server workers: 2
- Storage implementation: this repository's advisory-lock implementation
- Test database: an isolated database created only for this validation

No database credentials or server address are stored in the repository.

## Scenario

The application enqueues two executions of the same Hangfire job method protected by `DisableConcurrentExecution`.

1. The first execution runs for 1,860 seconds (31 minutes).
2. After the first execution starts, a second execution of the same method is enqueued.
3. `DistributedLockTimeout` remains configured at 10 minutes.
4. The second worker waits for the same distributed lock.
5. The run fails immediately if both job methods execute at the same time.
6. The run succeeds only if the contender starts after the 31-minute execution completes.

This reproduces the production risk where the original table-backed implementation could treat a still-active lock as expired once a long-running job exceeded `DistributedLockTimeout`.

## Application

The reusable validation application is located at:

`samples/Hangfire.PostgreSql.AspNetCore31E2E`

It exposes:

- `/e2e/status` for the current run state
- `/hangfire` for the Hangfire dashboard

The connection string is supplied only through `KINGBASE_E2E_CONNECTION_STRING`.

## Results

### Compatibility smoke test

- Hangfire schema installation succeeded on KingbaseES.
- Hangfire Server started with two workers.
- A 15-second long job and a 2-second contender completed sequentially.
- Observed maximum concurrency was 1.

### 31-minute validation

- Long-running job elapsed time: 31.00 minutes
- Contender duration: 5 seconds
- Observed maximum concurrency: 1
- Contender started before long job completed: no
- Hangfire job state for both executions: `Succeeded`
- Process exit code: 0

At the 10-minute lock timeout boundary:

- granted advisory locks: 1
- legacy `hangfire.lock` rows: 0
- contender execution had not started

After both jobs completed:

- granted advisory locks: 0
- legacy `hangfire.lock` rows: 0

## Conclusion

The advisory-lock implementation remained exclusive for the full 31-minute job lifetime on KingbaseES. The second execution waited until the first execution completed, and the lock was released cleanly afterward.
