using System;
using System.Threading;
using Hangfire;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class PostgreSqlDistributedLockEndToEndFacts
    {
        [Fact, CleanDatabase]
        public void DisableConcurrentExecution_DoesNotOverlap_WhenJobRunsLongerThanDistributedLockTimeout()
        {
            LongRunningJobHarness.Reset();

            var options = new PostgreSqlStorageOptions
            {
                DistributedLockTimeout = TimeSpan.FromSeconds(2),
                InvisibilityTimeout = TimeSpan.FromMinutes(5),
                PrepareSchemaIfNecessary = false,
                QueuePollInterval = TimeSpan.FromMilliseconds(100),
                SchemaName = ConnectionUtils.GetSchemaName()
            };

            var storage = new PostgreSqlStorage(ConnectionUtils.GetConnectionString(), options);

            using (var server = new BackgroundJobServer(
                new BackgroundJobServerOptions
                {
                    Queues = new[] { "default" },
                    WorkerCount = 2
                },
                storage))
            {
                var client = new BackgroundJobClient(storage);

                client.Enqueue(() => LongRunningJobHarness.Execute());

                Assert.True(
                    LongRunningJobHarness.FirstStarted.Wait(TimeSpan.FromSeconds(10)),
                    "The first job did not start in time.");

                Thread.Sleep(TimeSpan.FromSeconds(3));

                client.Enqueue(() => LongRunningJobHarness.Execute());

                Assert.False(
                    LongRunningJobHarness.ConcurrentExecutionDetected.Wait(TimeSpan.FromSeconds(4)),
                    "The second job started while the first job was still running.");

                Assert.False(
                    LongRunningJobHarness.SecondStarted.IsSet,
                    "The second job started before the first job released its lock.");

                LongRunningJobHarness.AllowFirstToFinish.Set();

                Assert.True(
                    LongRunningJobHarness.SecondStarted.Wait(TimeSpan.FromSeconds(10)),
                    "The second job did not start after the first job finished.");

                Assert.True(
                    LongRunningJobHarness.BothCompleted.Wait(TimeSpan.FromSeconds(10)),
                    "Both jobs did not complete in time.");

                Assert.False(
                    LongRunningJobHarness.ConcurrentExecutionDetected.IsSet,
                    "Concurrent execution was detected.");
            }
        }

        public static class LongRunningJobHarness
        {
            private static int _completedCount;
            private static int _executionCount;
            private static int _runningCount;

            public static ManualResetEventSlim AllowFirstToFinish { get; } = new ManualResetEventSlim(false);
            public static ManualResetEventSlim BothCompleted { get; } = new ManualResetEventSlim(false);
            public static ManualResetEventSlim ConcurrentExecutionDetected { get; } = new ManualResetEventSlim(false);
            public static ManualResetEventSlim FirstStarted { get; } = new ManualResetEventSlim(false);
            public static ManualResetEventSlim SecondStarted { get; } = new ManualResetEventSlim(false);

            public static void Reset()
            {
                Interlocked.Exchange(ref _completedCount, 0);
                Interlocked.Exchange(ref _executionCount, 0);
                Interlocked.Exchange(ref _runningCount, 0);

                AllowFirstToFinish.Reset();
                BothCompleted.Reset();
                ConcurrentExecutionDetected.Reset();
                FirstStarted.Reset();
                SecondStarted.Reset();
            }

            [AutomaticRetry(Attempts = 0)]
            [DisableConcurrentExecution(timeoutInSeconds: 30)]
            public static void Execute()
            {
                var executionNumber = Interlocked.Increment(ref _executionCount);
                var runningNow = Interlocked.Increment(ref _runningCount);

                if (runningNow > 1)
                {
                    ConcurrentExecutionDetected.Set();
                }

                try
                {
                    if (executionNumber == 1)
                    {
                        FirstStarted.Set();
                        AllowFirstToFinish.Wait(TimeSpan.FromSeconds(30));
                        Thread.Sleep(TimeSpan.FromMilliseconds(200));
                    }
                    else if (executionNumber == 2)
                    {
                        SecondStarted.Set();
                        Thread.Sleep(TimeSpan.FromMilliseconds(200));
                    }
                }
                finally
                {
                    Interlocked.Decrement(ref _runningCount);

                    if (Interlocked.Increment(ref _completedCount) >= 2)
                    {
                        BothCompleted.Set();
                    }
                }
            }
        }
    }
}
