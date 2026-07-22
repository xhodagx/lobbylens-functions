using System.Collections.Concurrent;
using System.Net;
using System.Text.Json;
using System.Text.RegularExpressions;
using Azure.Identity;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;

namespace LobbyLens.Functions;

// Community form stats: per-player average placement over the recent window,
// aggregated from the anonymized match reports. Lookups are BY HASH ONLY — the
// plugin hashes battletags locally and asks about the pseudonyms, so no name
// ever reaches this endpoint and the response reveals nothing that isn't
// already an aggregate of consented, anonymized data.
public class FormStats
{
    private const int MaxIds = 24;
    private const int WindowDays = 60;
    private const int RateLimitPerHour = 120;
    private static readonly TimeSpan CacheTtl = TimeSpan.FromMinutes(15);

    private static readonly Regex HashRx = new("^[0-9a-f]{1,64}$", RegexOptions.Compiled);
    private static readonly ConcurrentDictionary<string, (DateTime Expiry, int N, double Avg)> Cache = new();
    private static readonly TimeSpan RateWindow = TimeSpan.FromHours(1);
    private static readonly ConcurrentDictionary<string, (DateTime Start, int Count)> RateWindows = new();

    private static readonly Lazy<Container> Matches = new(() =>
    {
        string endpoint = Environment.GetEnvironmentVariable("COSMOS_ENDPOINT")
                          ?? throw new InvalidOperationException("COSMOS_ENDPOINT not set");
        string db = Environment.GetEnvironmentVariable("COSMOS_DATABASE") ?? "lobbylens";
        var client = new CosmosClient(endpoint, new DefaultAzureCredential());
        return client.GetContainer(db, "matches");
    });

    private readonly ILogger _log;
    public FormStats(ILoggerFactory lf) => _log = lf.CreateLogger<FormStats>();

    private sealed record Row(string? i, string? a, int pl);

    [Function("FormStats")]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "form")] HttpRequestData req)
    {
        string? ip = ClientIp(req);
        if (ip != null && !AllowRequest(ip))
            return await Text(req, HttpStatusCode.TooManyRequests, "rate limited");

        string idsRaw = System.Web.HttpUtility.ParseQueryString(req.Url.Query).Get("ids") ?? "";
        string[] ids = idsRaw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(s => s.ToLowerInvariant()).Distinct().ToArray();
        if (ids.Length == 0 || ids.Length > MaxIds || ids.Any(s => !HashRx.IsMatch(s)))
            return await Text(req, HttpStatusCode.BadRequest, "bad ids");

        var result = new Dictionary<string, (int N, double Avg)>();
        var misses = new List<string>();
        DateTime now = DateTime.UtcNow;
        foreach (string id in ids)
        {
            if (Cache.TryGetValue(id, out var hit) && hit.Expiry > now)
            {
                if (hit.N > 0) result[id] = (hit.N, hit.Avg);
            }
            else misses.Add(id);
        }

        if (misses.Count > 0)
        {
            try
            {
                var places = new Dictionary<string, List<int>>();
                var q = new QueryDefinition(
                        "SELECT p.id AS i, p.aid AS a, p.p AS pl FROM c JOIN p IN c.players " +
                        "WHERE c.ingestedUtc >= @since AND p.p > 0 AND " +
                        "(ARRAY_CONTAINS(@ids, p.id) OR ARRAY_CONTAINS(@ids, p.aid))")
                    .WithParameter("@since", now.AddDays(-WindowDays).ToString("O"))
                    .WithParameter("@ids", misses);
                using FeedIterator<Row> it = Matches.Value.GetItemQueryIterator<Row>(q);
                while (it.HasMoreResults)
                {
                    foreach (Row r in await it.ReadNextAsync())
                    {
                        foreach (string? key in new[] { r.i, r.a })
                        {
                            if (key == null || !misses.Contains(key)) continue;
                            if (!places.TryGetValue(key, out var list)) places[key] = list = new List<int>();
                            list.Add(r.pl);
                        }
                    }
                }
                foreach (string id in misses)
                {
                    if (places.TryGetValue(id, out var list) && list.Count > 0)
                    {
                        var entry = (list.Count, Math.Round(list.Average(), 2));
                        result[id] = entry;
                        Cache[id] = (now + CacheTtl, entry.Item1, entry.Item2);
                    }
                    else Cache[id] = (now + CacheTtl, 0, 0); // negative-cache unknowns
                }
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "form query failed");
                return await Text(req, HttpStatusCode.InternalServerError, "error");
            }
        }

        if (Cache.Count > 50_000) // opportunistic prune keeps the map bounded
            foreach (var kv in Cache)
                if (kv.Value.Expiry <= now) Cache.TryRemove(kv.Key, out _);

        var payload = result.ToDictionary(kv => kv.Key, kv => new { n = kv.Value.N, a = kv.Value.Avg });
        HttpResponseData resp = req.CreateResponse(HttpStatusCode.OK);
        resp.Headers.Add("Content-Type", "application/json");
        resp.Headers.Add("Cache-Control", "public, max-age=300");
        await resp.WriteStringAsync(JsonSerializer.Serialize(payload));
        return resp;
    }

    private static string? ClientIp(HttpRequestData req)
    {
        if (!req.Headers.TryGetValues("X-Forwarded-For", out var values)) return null;
        string? first = values.FirstOrDefault()?.Split(',')[0].Trim();
        if (string.IsNullOrEmpty(first)) return null;
        int colon = first.LastIndexOf(':');
        if (colon > 0 && first.Contains('.')) first = first[..colon];
        return first;
    }

    private static bool AllowRequest(string ip)
    {
        DateTime now = DateTime.UtcNow;
        if (RateWindows.Count > 10_000)
            foreach (var kv in RateWindows)
                if (now - kv.Value.Start > RateWindow) RateWindows.TryRemove(kv.Key, out _);
        (DateTime Start, int Count) entry = RateWindows.AddOrUpdate(ip,
            _ => (now, 1),
            (_, cur) => now - cur.Start > RateWindow ? (now, 1) : (cur.Start, cur.Count + 1));
        return entry.Count <= RateLimitPerHour;
    }

    private static async Task<HttpResponseData> Text(HttpRequestData req, HttpStatusCode code, string msg)
    {
        HttpResponseData r = req.CreateResponse(code);
        await r.WriteStringAsync(msg);
        return r;
    }
}
