# ASP.NET Core 3.1 Kingbase E2E

This web application validates the advisory-lock implementation with a real Hangfire server.

It enqueues a long-running job and, after that job starts, enqueues a contender using the same method protected by `DisableConcurrentExecution`. The run succeeds only when the contender starts after the long-running job completes and observed maximum concurrency stays at one.

Required environment variables:

- `KINGBASE_E2E_CONNECTION_STRING`: connection string for an isolated test database
- `E2E_AUTO_START=true`: automatically starts the test

Optional environment variables:

- `E2E_LONG_JOB_SECONDS`: defaults to `1860` (31 minutes)
- `E2E_CONTENDER_JOB_SECONDS`: defaults to `5`
- `E2E_STOP_ON_COMPLETION`: defaults to `true`

Endpoints:

- `/e2e/status`: current verification state
- `/hangfire`: Hangfire dashboard
