# Distributed Lock Fix Summary

## Background

This fork started from upstream `1.8.0` because our application depends on that line.

In production we observed a locking bug when a single background job runs for a long time, especially when a run exceeds about 30 minutes. In the original `1.8.0` implementation this can lead to stale lock cleanup or duplicate execution attempts around `DisableConcurrentExecution`.

## Root Cause In `1.8.0`

Upstream `1.8.0` implemented distributed locks through the `hangfire.lock` table.

- Lock acquisition inserted a row into `hangfire.lock`.
- Lock release deleted that row during `Dispose`.
- Stale lock cleanup deleted rows older than `DistributedLockTimeout`.

This design has two important failure modes:

1. If a process exits ungracefully, the lock row can remain until timeout because PostgreSQL is not releasing it automatically.
2. If a job is still running after `DistributedLockTimeout`, another worker can delete the still-active row and acquire the same logical lock.

The second point matches the long-running job scenario we saw in practice.

## Implementation Changes

The lock implementation was rewritten to use PostgreSQL session-level advisory locks instead of the `lock` table.

### `src/Hangfire.PostgreSql/PostgreSqlDistributedLock.cs`

- Replaced table-backed lock acquisition with `pg_try_advisory_lock`.
- Replaced table-backed unlock with `pg_advisory_unlock`.
- Added a stable 64-bit hash of the resource name to map string resources to PostgreSQL advisory lock keys.
- Preserved the existing Hangfire timeout semantics by polling until the requested timeout elapses.
- Treated a closed connection during unlock as already released, because PostgreSQL releases session-level advisory locks when the session ends.

### `src/Hangfire.PostgreSql/PostgreSqlConnection.cs`

- Added a dedicated connection for distributed locks.
- Added in-process reference counting so the same `JobStorageConnection` can re-enter the same logical lock safely.
- Changed the dedicated lock connection to `Pooling = false`.

Disabling pooling for the lock connection is important. With pooled connections, closing the client connection would only return the session to the pool, and the advisory lock could remain held unexpectedly.

## Tests Added And Updated

### Lock integration tests

Updated `tests/Hangfire.PostgreSql.Tests/PostgreSqlDistributedLockFacts.cs` to validate:

- session-level lock exclusivity
- long-running locks are not expired just because `DistributedLockTimeout` elapsed
- lock release on explicit dispose
- lock release when the underlying database connection is closed

### Connection-level tests

Updated `tests/Hangfire.PostgreSql.Tests/PostgreSqlConnectionFacts.cs` to validate:

- re-entrant lock acquisition within the same `PostgreSqlConnection`
- release of the dedicated lock connection during storage connection disposal

### End-to-end Hangfire test

Added `tests/Hangfire.PostgreSql.Tests/PostgreSqlDistributedLockEndToEndFacts.cs`.

This is the closest automated reproduction of the production issue:

- `DistributedLockTimeout` is shortened to `2 seconds`
- a real `BackgroundJobServer` is started
- a job with `DisableConcurrentExecution` is enqueued twice
- the first execution intentionally runs longer than the configured lock timeout
- the test verifies the second execution does not start until the first execution finishes

This is a scaled-down equivalent of the "job exceeds 30 minutes" production case.

## Validation Performed

Validation was executed against PostgreSQL 15 running in Docker.

- Lock-focused tests passed.
- `PostgreSqlConnectionFacts` passed.
- Full test suite passed: `228/228`.
- The new end-to-end test passed once in isolation.
- The same end-to-end test then passed 5 consecutive runs.

## Operational Notes

- This implementation assumes a stable PostgreSQL session for the lifetime of the distributed lock.
- If the deployment uses PgBouncer in `transaction` pooling mode, advisory locks are not safe because session affinity is lost.
- The `hangfire.lock` table remains in the schema for compatibility with existing installs, but the distributed lock path no longer depends on it.

## Scope Of This Change

This change is intentionally focused on fixing distributed lock behavior in the `1.8.0` code line without taking a broader upstream upgrade.
