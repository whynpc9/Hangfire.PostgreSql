using System;

namespace Hangfire.PostgreSql.AspNetCore31E2E
{
    public sealed class E2EState
    {
        private readonly object _syncRoot = new object();
        private E2EStatusSnapshot _state = new E2EStatusSnapshot { Status = "waiting" };

        public void Begin(string runId, int longJobSeconds, int contenderJobSeconds)
        {
            lock (_syncRoot)
            {
                _state = new E2EStatusSnapshot
                {
                    RunId = runId,
                    Status = "starting",
                    StartedAtUtc = DateTime.UtcNow,
                    LongJobDurationSeconds = longJobSeconds,
                    ContenderJobDurationSeconds = contenderJobSeconds
                };
            }
        }

        public bool IsCurrentRun(string runId)
        {
            lock (_syncRoot)
            {
                return string.Equals(_state.RunId, runId, StringComparison.Ordinal);
            }
        }

        public void SetLongJobId(string jobId)
        {
            lock (_syncRoot)
            {
                _state.LongJobId = jobId;
            }
        }

        public void SetContenderJobId(string jobId)
        {
            lock (_syncRoot)
            {
                _state.ContenderJobId = jobId;
            }
        }

        public void JobStarted(string role)
        {
            lock (_syncRoot)
            {
                _state.RunningCount++;
                _state.MaxConcurrent = Math.Max(_state.MaxConcurrent, _state.RunningCount);
                _state.ConcurrentExecutionDetected = _state.MaxConcurrent > 1;
                _state.LastHeartbeatUtc = DateTime.UtcNow;
                _state.Status = "running";

                if (role == LongRunningJob.LongRole)
                {
                    _state.LongJobStartedAtUtc = DateTime.UtcNow;
                }
                else
                {
                    _state.ContenderJobStartedAtUtc = DateTime.UtcNow;
                }
            }
        }

        public void Heartbeat(string role)
        {
            lock (_syncRoot)
            {
                _state.LastHeartbeatUtc = DateTime.UtcNow;
                _state.LastHeartbeatRole = role;
            }
        }

        public void JobCompleted(string role)
        {
            lock (_syncRoot)
            {
                _state.RunningCount--;
                _state.CompletedCount++;
                _state.LastHeartbeatUtc = DateTime.UtcNow;

                if (role == LongRunningJob.LongRole)
                {
                    _state.LongJobCompletedAtUtc = DateTime.UtcNow;
                }
                else
                {
                    _state.ContenderJobCompletedAtUtc = DateTime.UtcNow;
                }
            }
        }

        public void Succeed()
        {
            lock (_syncRoot)
            {
                _state.Status = "succeeded";
                _state.FinishedAtUtc = DateTime.UtcNow;
            }
        }

        public void Fail(string reason)
        {
            lock (_syncRoot)
            {
                _state.Status = "failed";
                _state.Failure = reason;
                _state.FinishedAtUtc = DateTime.UtcNow;
            }
        }

        public E2EStatusSnapshot GetSnapshot()
        {
            lock (_syncRoot)
            {
                return _state.Clone();
            }
        }
    }

    public sealed class E2EStatusSnapshot
    {
        public string RunId { get; set; }
        public string Status { get; set; }
        public string Failure { get; set; }
        public string LongJobId { get; set; }
        public string ContenderJobId { get; set; }
        public string LastHeartbeatRole { get; set; }
        public int LongJobDurationSeconds { get; set; }
        public int ContenderJobDurationSeconds { get; set; }
        public int RunningCount { get; set; }
        public int MaxConcurrent { get; set; }
        public int CompletedCount { get; set; }
        public bool ConcurrentExecutionDetected { get; set; }
        public DateTime? StartedAtUtc { get; set; }
        public DateTime? FinishedAtUtc { get; set; }
        public DateTime? LongJobStartedAtUtc { get; set; }
        public DateTime? LongJobCompletedAtUtc { get; set; }
        public DateTime? ContenderJobStartedAtUtc { get; set; }
        public DateTime? ContenderJobCompletedAtUtc { get; set; }
        public DateTime? LastHeartbeatUtc { get; set; }

        public E2EStatusSnapshot Clone()
        {
            return (E2EStatusSnapshot)MemberwiseClone();
        }
    }
}
