using Microsoft.Extensions.Hosting;

// .NET 8 isolated worker host. Triggers self-wire via [Function] attributes;
// clients (HttpClient, Blob, Cosmos) are created as cached statics in each function
// to keep this entrypoint minimal and dependency-injection-free for now.
var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .Build();

host.Run();
