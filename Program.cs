using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

// .NET 8 isolated worker host. Triggers self-wire via [Function] attributes;
// clients (HttpClient, Blob, Cosmos) are created as cached statics in each function.
// Worker-side Application Insights is wired explicitly — without it the isolated
// model emits almost no request telemetry (host-only), which made ingest health
// invisible. IP masking stays on (Azure default).
var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices(services =>
    {
        services.AddApplicationInsightsTelemetryWorkerService();
        services.ConfigureFunctionsApplicationInsights();
    })
    .Build();

host.Run();
