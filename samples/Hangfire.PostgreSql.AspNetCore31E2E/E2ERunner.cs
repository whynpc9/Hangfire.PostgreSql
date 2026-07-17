using System;
using System.Threading;
using System.Threading.Tasks;
using Hangfire;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Hangfire.PostgreSql.AspNetCore31E2E
{
    public sealed class E2ERunner : BackgroundService
    {
        private readonly IBackgroundJobClient _client;
        private readonly IHostApplicationLifetime _lifetime;
        private readonly ILogger<E2ERunner> _logger;
        private readonly E2EState _state;

        public E2ERunner(
            IBackgroundJobClient client,
            IHostApplicationLifetime lifetime,
            ILogger<E2ERunner> logger,
            E2EState state)
        {
            _client = client;
            _lifetime = lifetime;
            _logger = logger;
            _state = state;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            if (!GetBoolean("E2E_AUTO_START", false))
            {
                _logger.LogInformation("E2E auto-start is disabled.");
                return;
            }

            int longJobSeconds = GetPositiveInteger("E2E_LONG_JOB_SECONDS", 31 * 60);
            int contenderJobSeconds = GetPositiveInteger("E2E_CONTENDER_JOB_SECONDS", 5);
            string runId = DateTime.UtcNow.ToString("yyyyMMddHHmmss");

            _state.Begin(runId, longJobSeconds, contenderJobSeconds);
            _logger.LogInformation(
                "Starting E2E run {RunId}; long={LongSeconds}s, contender={ContenderSeconds}s.",
                runId,
                longJobSeconds,
                contenderJobSeconds);

            try
            {
                string longJobId = _client.Enqueue<LongRunningJob>(job =>
                    job.Execute(runId, LongRunningJob.LongRole, longJobSeconds));
                _state.SetLongJobId(longJobId);

                if (!await WaitForAsync(
                    snapshot => snapshot.LongJobStartedAtUtc.HasValue,
                    TimeSpan.FromMinutes(2),
                    stoppingToken))
                {
                    Fail("The long-running job did not start within two minutes.");
                    return;
                }

                string contenderJobId = _client.Enqueue<LongRunningJob>(job =>
                    job.Execute(runId, LongRunningJob.ContenderRole, contenderJobSeconds));
                _state.SetContenderJobId(contenderJobId);

                DateTime deadline = DateTime.UtcNow.AddSeconds(longJobSeconds + contenderJobSeconds + 300);
                DateTime nextProgressLog = DateTime.UtcNow;

                while (!stoppingToken.IsCancellationRequested && DateTime.UtcNow < deadline)
                {
                    E2EStatusSnapshot snapshot = _state.GetSnapshot();

                    if (snapshot.ConcurrentExecutionDetected)
                    {
                        Fail("The contender started before the long-running job released its distributed lock.");
                        return;
                    }

                    if (snapshot.CompletedCount >= 2)
                    {
                        ValidateAndComplete(snapshot);
                        return;
                    }

                    if (DateTime.UtcNow >= nextProgressLog)
                    {
                        _logger.LogInformation(
                            "E2E run {RunId}: completed={Completed}, running={Running}, maxConcurrent={MaxConcurrent}, contenderStarted={ContenderStarted}.",
                            runId,
                            snapshot.CompletedCount,
                            snapshot.RunningCount,
                            snapshot.MaxConcurrent,
                            snapshot.ContenderJobStartedAtUtc.HasValue);
                        nextProgressLog = DateTime.UtcNow.AddMinutes(1);
                    }

                    await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
                }

                Fail("The E2E run did not finish before its deadline.");
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, "The E2E runner failed.");
                Fail(exception.ToString());
            }
        }

        private async Task<bool> WaitForAsync(
            Func<E2EStatusSnapshot, bool> predicate,
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            DateTime deadline = DateTime.UtcNow.Add(timeout);
            while (!cancellationToken.IsCancellationRequested && DateTime.UtcNow < deadline)
            {
                if (predicate(_state.GetSnapshot()))
                {
                    return true;
                }

                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            }

            return false;
        }

        private void ValidateAndComplete(E2EStatusSnapshot snapshot)
        {
            if (!snapshot.LongJobStartedAtUtc.HasValue || !snapshot.LongJobCompletedAtUtc.HasValue)
            {
                Fail("The long-running job timestamps are incomplete.");
                return;
            }

            if (!snapshot.ContenderJobStartedAtUtc.HasValue || !snapshot.ContenderJobCompletedAtUtc.HasValue)
            {
                Fail("The contender job timestamps are incomplete.");
                return;
            }

            TimeSpan longDuration = snapshot.LongJobCompletedAtUtc.Value - snapshot.LongJobStartedAtUtc.Value;
            if (longDuration < TimeSpan.FromSeconds(snapshot.LongJobDurationSeconds - 2))
            {
                Fail("The long-running job completed earlier than configured.");
                return;
            }

            if (snapshot.ContenderJobStartedAtUtc.Value < snapshot.LongJobCompletedAtUtc.Value)
            {
                Fail("The contender started before the long-running job completed.");
                return;
            }

            if (snapshot.MaxConcurrent != 1)
            {
                Fail("Expected max concurrency to be exactly one.");
                return;
            }

            _state.Succeed();
            _logger.LogInformation(
                "E2E verification succeeded. Long job duration={DurationMinutes:F2}m; maxConcurrent={MaxConcurrent}.",
                longDuration.TotalMinutes,
                snapshot.MaxConcurrent);
            StopIfConfigured();
        }

        private void Fail(string reason)
        {
            _state.Fail(reason);
            _logger.LogError("E2E verification failed: {Reason}", reason);
            Environment.ExitCode = 1;
            StopIfConfigured();
        }

        private void StopIfConfigured()
        {
            if (GetBoolean("E2E_STOP_ON_COMPLETION", true))
            {
                _lifetime.StopApplication();
            }
        }

        private static bool GetBoolean(string name, bool defaultValue)
        {
            string value = Environment.GetEnvironmentVariable(name);
            return bool.TryParse(value, out bool parsed) ? parsed : defaultValue;
        }

        private static int GetPositiveInteger(string name, int defaultValue)
        {
            string value = Environment.GetEnvironmentVariable(name);
            return int.TryParse(value, out int parsed) && parsed > 0 ? parsed : defaultValue;
        }
    }
}
