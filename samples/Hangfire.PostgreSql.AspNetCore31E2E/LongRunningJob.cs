using System;
using System.Threading;
using Hangfire;
using Microsoft.Extensions.Logging;

namespace Hangfire.PostgreSql.AspNetCore31E2E
{
    public sealed class LongRunningJob
    {
        public const string LongRole = "long";
        public const string ContenderRole = "contender";

        private readonly ILogger<LongRunningJob> _logger;
        private readonly E2EState _state;

        public LongRunningJob(E2EState state, ILogger<LongRunningJob> logger)
        {
            _state = state;
            _logger = logger;
        }

        [AutomaticRetry(Attempts = 0)]
        [DisableConcurrentExecution(timeoutInSeconds: 2400)]
        public void Execute(string runId, string role, int durationSeconds)
        {
            if (!_state.IsCurrentRun(runId))
            {
                throw new InvalidOperationException("The job does not belong to the active E2E run.");
            }

            _state.JobStarted(role);
            DateTime startedAt = DateTime.UtcNow;
            DateTime nextProgressLog = startedAt;
            DateTime deadline = startedAt.AddSeconds(durationSeconds);

            _logger.LogInformation(
                "E2E job {Role} started for run {RunId}; duration={DurationSeconds}s.",
                role,
                runId,
                durationSeconds);

            try
            {
                while (DateTime.UtcNow < deadline)
                {
                    DateTime now = DateTime.UtcNow;
                    _state.Heartbeat(role);

                    if (now >= nextProgressLog)
                    {
                        _logger.LogInformation(
                            "E2E job {Role} heartbeat for run {RunId}; elapsed={ElapsedMinutes:F1}m.",
                            role,
                            runId,
                            (now - startedAt).TotalMinutes);
                        nextProgressLog = now.AddMinutes(1);
                    }

                    TimeSpan remaining = deadline - now;
                    Thread.Sleep(remaining > TimeSpan.FromSeconds(5) ? TimeSpan.FromSeconds(5) : remaining);
                }
            }
            finally
            {
                _state.JobCompleted(role);
                _logger.LogInformation(
                    "E2E job {Role} completed for run {RunId}; elapsed={ElapsedMinutes:F1}m.",
                    role,
                    runId,
                    (DateTime.UtcNow - startedAt).TotalMinutes);
            }
        }
    }
}
