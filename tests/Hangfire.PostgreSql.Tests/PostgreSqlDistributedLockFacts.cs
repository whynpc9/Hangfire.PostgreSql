using System;
using System.Data;
using System.Threading;
using Moq;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class PostgreSqlDistributedLockFacts
    {
        private readonly TimeSpan _timeout = TimeSpan.FromMilliseconds(250);

        [Fact]
        public void Ctor_ThrowsAnException_WhenResourceIsNullOrEmpty()
        {
            var options = new PostgreSqlStorageOptions();

            var exception = Assert.Throws<ArgumentNullException>(
                () => new PostgreSqlDistributedLock("", _timeout, new Mock<IDbConnection>().Object, options));

            Assert.Equal("resource", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var options = new PostgreSqlStorageOptions();

            var exception = Assert.Throws<ArgumentNullException>(
                () => new PostgreSqlDistributedLock("hello", _timeout, null, options));

            Assert.Equal("connection", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new PostgreSqlDistributedLock("hi", _timeout, new Mock<IDbConnection>().Object, null));

            Assert.Equal("options", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsClosed()
        {
            var options = new PostgreSqlStorageOptions();
            var connection = new Mock<IDbConnection>();
            connection.SetupGet(x => x.State).Returns(ConnectionState.Closed);

            Assert.Throws<InvalidOperationException>(
                () => new PostgreSqlDistributedLock("hi", _timeout, connection.Object, options));
        }

        [Theory, InlineData(true), InlineData(false)]
        [CleanDatabase]
        public void Ctor_AcquiresExclusiveApplicationLock_OnSession(bool useNativeDatabaseTransactions)
        {
            var options = new PostgreSqlStorageOptions
            {
                SchemaName = GetSchemaName(),
                UseNativeDatabaseTransactions = useNativeDatabaseTransactions
            };

            UseConnection(connection1 =>
            {
                using (new PostgreSqlDistributedLock("exclusive", TimeSpan.FromSeconds(1), connection1, options))
                {
                    UseConnection(connection2 =>
                        Assert.Throws<PostgreSqlDistributedLockException>(
                            () => new PostgreSqlDistributedLock("exclusive", _timeout, connection2, options)));
                }
            });
        }

        [Theory, InlineData(true), InlineData(false)]
        [CleanDatabase]
        public void Ctor_DoesNotExpireActiveLock_WhenDistributedLockTimeoutElapses(bool useNativeDatabaseTransactions)
        {
            var options = new PostgreSqlStorageOptions
            {
                SchemaName = GetSchemaName(),
                DistributedLockTimeout = TimeSpan.FromMilliseconds(1),
                UseNativeDatabaseTransactions = useNativeDatabaseTransactions
            };

            UseConnection(connection1 =>
            {
                using (new PostgreSqlDistributedLock("exclusive", TimeSpan.FromSeconds(1), connection1, options))
                {
                    Thread.Sleep(100);

                    UseConnection(connection2 =>
                        Assert.Throws<PostgreSqlDistributedLockException>(
                            () => new PostgreSqlDistributedLock("exclusive", _timeout, connection2, options)));
                }
            });
        }

        [Theory, InlineData(true), InlineData(false)]
        [CleanDatabase]
        public void Dispose_ReleasesExclusiveApplicationLock(bool useNativeDatabaseTransactions)
        {
            var options = new PostgreSqlStorageOptions
            {
                SchemaName = GetSchemaName(),
                UseNativeDatabaseTransactions = useNativeDatabaseTransactions
            };

            UseConnection(connection1 =>
            {
                var distributedLock = new PostgreSqlDistributedLock("hello", TimeSpan.FromSeconds(1), connection1, options);
                distributedLock.Dispose();

                UseConnection(connection2 =>
                {
                    using (var anotherLock = new PostgreSqlDistributedLock("hello", TimeSpan.FromSeconds(1), connection2, options))
                    {
                        Assert.NotNull(anotherLock);
                    }
                });
            });
        }

        [Theory, InlineData(true), InlineData(false)]
        [CleanDatabase]
        public void ClosingConnection_ReleasesExclusiveApplicationLock(bool useNativeDatabaseTransactions)
        {
            var options = new PostgreSqlStorageOptions
            {
                SchemaName = GetSchemaName(),
                UseNativeDatabaseTransactions = useNativeDatabaseTransactions
            };

            var connection1 = ConnectionUtils.CreateConnection();
            var distributedLock = new PostgreSqlDistributedLock("hello", TimeSpan.FromSeconds(1), connection1, options);

            connection1.Dispose();

            UseConnection(connection2 =>
            {
                using (var anotherLock = new PostgreSqlDistributedLock("hello", TimeSpan.FromSeconds(1), connection2, options))
                {
                    Assert.NotNull(anotherLock);
                }
            });

            GC.KeepAlive(distributedLock);
        }

        private void UseConnection(Action<NpgsqlConnection> action)
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                action(connection);
            }
        }

        private static string GetSchemaName()
        {
            return ConnectionUtils.GetSchemaName();
        }
    }
}
