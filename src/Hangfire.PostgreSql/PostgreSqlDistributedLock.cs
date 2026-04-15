// This file is part of Hangfire.PostgreSql.
// Copyright © 2014 Frank Hommers <http://hmm.rs/Hangfire.PostgreSql>.
// 
// Hangfire.PostgreSql is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as 
// published by the Free Software Foundation, either version 3 
// of the License, or any later version.
// 
// Hangfire.PostgreSql  is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public 
// License along with Hangfire.PostgreSql. If not, see <http://www.gnu.org/licenses/>.
//
// This work is based on the work of Sergey Odinokov, author of 
// Hangfire. <http://hangfire.io/>
//   
//    Special thanks goes to him.

using System;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using Dapper;

namespace Hangfire.PostgreSql
{
    public sealed class PostgreSqlDistributedLock : IDisposable
    {
        private readonly string _resource;
        private readonly IDbConnection _connection;
        private bool _completed;

        public PostgreSqlDistributedLock(string resource, TimeSpan timeout, IDbConnection connection,
            PostgreSqlStorageOptions options)
        {
            if (string.IsNullOrEmpty(resource)) throw new ArgumentNullException(nameof(resource));

            _resource = resource;
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            Acquire(connection, resource, timeout, options ?? throw new ArgumentNullException(nameof(options)));
        }

        internal static void Acquire(IDbConnection connection, string resource, TimeSpan timeout, PostgreSqlStorageOptions options)
        {
            if (string.IsNullOrEmpty(resource)) throw new ArgumentNullException(nameof(resource));
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            if (options == null) throw new ArgumentNullException(nameof(options));
            if (connection.State != ConnectionState.Open)
                throw new InvalidOperationException("Connection must be open before acquiring a distributed lock.");

            var lockAcquiringTime = Stopwatch.StartNew();
            var key = GetResourceKey(resource);

            bool tryAcquireLock = true;

            while (tryAcquireLock)
            {
                if (connection.Query<bool>("SELECT pg_try_advisory_lock(@key)", new { key }).Single())
                {
                    return;
                }

                if (lockAcquiringTime.ElapsedMilliseconds > timeout.TotalMilliseconds)
                {
                    tryAcquireLock = false;
                }
                else
                {
                    int sleepDuration = (int)(timeout.TotalMilliseconds - lockAcquiringTime.ElapsedMilliseconds);
                    if (sleepDuration > 1000) sleepDuration = 1000;
                    if (sleepDuration > 0)
                    {
                        Thread.Sleep(sleepDuration);
                    }
                    else
                    {
                        tryAcquireLock = false;
                    }
                }
            }

            throw new PostgreSqlDistributedLockException(
                $"Could not place a lock on the resource \'{resource}\': Lock timeout.");
        }

        internal static void Release(IDbConnection connection, string resource, PostgreSqlStorageOptions options)
        {
            if (string.IsNullOrEmpty(resource)) throw new ArgumentNullException(nameof(resource));
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            if (connection.State != ConnectionState.Open) return;

            var key = GetResourceKey(resource);
            var released = connection.Query<bool>("SELECT pg_advisory_unlock(@key)", new { key }).Single();

            if (!released)
            {
                throw new PostgreSqlDistributedLockException(
                    $"Could not release a lock on the resource '{resource}'. Lock does not exists.");
            }
        }

        private static long GetResourceKey(string resource)
        {
            unchecked
            {
                const ulong offsetBasis = 14695981039346656037UL;
                const ulong prime = 1099511628211UL;

                ulong hash = offsetBasis;
                byte[] bytes = Encoding.UTF8.GetBytes(resource);

                for (int i = 0; i < bytes.Length; i++)
                {
                    hash ^= bytes[i];
                    hash *= prime;
                }

                return (long)hash;
            }
        }

        public void Dispose()
        {
            if (_completed) return;

            _completed = true;
            Release(_connection, _resource, null);
        }
    }
}
