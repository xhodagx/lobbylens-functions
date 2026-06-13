using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using Azure.Identity;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;

namespace LobbyLens.Functions;

// Receives an anonymized match summary from the plugin and stores it in Cosmos for
// future aggregate stats (hero/comp winrates, lobby difficulty, etc). Partitioned by region.
// NOTE: Anonymous so the public plugin needs no embedded key (which would be extractable
// anyway). Ingest is therefore low-trust: validate shape, cap size, and never trust
// client-supplied ids/timestamps. Rate limiting / HMAC is a later hardening pass.
public class IngestMatch
{
    private const int MaxBodyBytes = 64 * 1024;

    private static readonly Lazy<Container> Matches = new(() =>
    {
        string endpoint = Environment.GetEnvironmentVariable("COSMOS_ENDPOINT")
                          ?? throw new InvalidOperationException("COSMOS_ENDPOINT not set");
        string db = Environment.GetEnvironmentVariable("COSMOS_DATABASE") ?? "lobbylens";
        var client = new CosmosClient(endpoint, new DefaultAzureCredential());
        return client.GetContainer(db, "matches");
    });

    private readonly ILogger _log;
    public IngestMatch(ILoggerFactory lf) => _log = lf.CreateLogger<IngestMatch>();

    [Function("IngestMatch")]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "match")] HttpRequestData req)
    {
        string body = await new StreamReader(req.Body).ReadToEndAsync();
        if (string.IsNullOrWhiteSpace(body) || body.Length > MaxBodyBytes)
            return await Text(req, HttpStatusCode.BadRequest, "empty or oversized body");

        JsonObject? obj;
        try { obj = JsonNode.Parse(body) as JsonObject; }
        catch { return await Text(req, HttpStatusCode.BadRequest, "invalid json"); }
        if (obj is null) return await Text(req, HttpStatusCode.BadRequest, "expected json object");

        // Server owns identity and timestamps; never trust the client's.
        string region = (obj["region"]?.GetValue<string>() ?? "US").ToUpperInvariant();
        if (region is not ("US" or "EU" or "AP")) region = "US";
        obj["id"] = Guid.NewGuid().ToString("n");
        obj["region"] = region;
        obj["ingestedUtc"] = DateTimeOffset.UtcNow.ToString("O");

        try
        {
            using var ms = new MemoryStream(Encoding.UTF8.GetBytes(obj.ToJsonString()));
            using ResponseMessage resp = await Matches.Value.CreateItemStreamAsync(ms, new PartitionKey(region));
            if (!resp.IsSuccessStatusCode)
            {
                _log.LogError("Cosmos write failed: {Status}", resp.StatusCode);
                return await Text(req, HttpStatusCode.BadGateway, "store failed");
            }
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "ingest failed");
            return await Text(req, HttpStatusCode.InternalServerError, "error");
        }

        return await Text(req, HttpStatusCode.Accepted, "ok");
    }

    private static async Task<HttpResponseData> Text(HttpRequestData req, HttpStatusCode code, string msg)
    {
        HttpResponseData r = req.CreateResponse(code);
        await r.WriteStringAsync(msg);
        return r;
    }
}
