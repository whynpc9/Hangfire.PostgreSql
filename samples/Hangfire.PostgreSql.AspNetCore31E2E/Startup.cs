using System;
using System.Text.Json;
using System.Threading.Tasks;
using Hangfire;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Hangfire.PostgreSql.AspNetCore31E2E
{
    public sealed class Startup
    {
        public void ConfigureServices(IServiceCollection services)
        {
            string connectionString = Environment.GetEnvironmentVariable("KINGBASE_E2E_CONNECTION_STRING");
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new InvalidOperationException("KINGBASE_E2E_CONNECTION_STRING is required.");
            }

            services.AddSingleton<E2EState>();

            services.AddHangfire(configuration => configuration
                .SetDataCompatibilityLevel(CompatibilityLevel.Version_170)
                .UseSimpleAssemblyNameTypeSerializer()
                .UseRecommendedSerializerSettings()
                .UsePostgreSqlStorage(connectionString, new PostgreSqlStorageOptions
                {
                    DistributedLockTimeout = TimeSpan.FromMinutes(10),
                    InvisibilityTimeout = TimeSpan.FromHours(2),
                    PrepareSchemaIfNecessary = true,
                    QueuePollInterval = TimeSpan.FromSeconds(1),
                    SchemaName = "hangfire"
                }));

            services.AddHangfireServer(options =>
            {
                options.Queues = new[] { "default" };
                options.ShutdownTimeout = TimeSpan.FromMinutes(2);
                options.WorkerCount = 2;
            });

            services.AddHostedService<E2ERunner>();
        }

        public void Configure(IApplicationBuilder app, IWebHostEnvironment environment, E2EState state)
        {
            if (environment.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseRouting();
            app.UseHangfireDashboard("/hangfire");

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapGet("/", context => context.Response.WriteAsync(
                    "Hangfire PostgreSQL advisory-lock E2E is running. Status: /e2e/status, dashboard: /hangfire"));

                endpoints.MapGet("/e2e/status", async context =>
                {
                    context.Response.ContentType = "application/json; charset=utf-8";
                    await context.Response.WriteAsync(JsonSerializer.Serialize(
                        state.GetSnapshot(),
                        new JsonSerializerOptions { WriteIndented = true }));
                });
            });
        }
    }
}
