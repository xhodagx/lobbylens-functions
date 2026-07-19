using System.Collections.Concurrent;
using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Azure.Identity;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;

namespace LobbyLens.Functions;

// Receives an anonymized match summary from the plugin and stores it in Cosmos for
// future aggregate stats (hero/comp winrates, lobby difficulty, etc). Partitioned by region.
// NOTE: Anonymous so the public plugin needs no embedded key (which would be extractable
// anyway). Ingest is therefore low-trust and hardened accordingly: the body is read
// through a hard size cap, validated field-by-field, and REBUILT into a normalized
// document — nothing client-supplied is stored verbatim, and ids/timestamps are always
// server-owned. A per-IP rate limit (in-memory per instance — best-effort, not a
// security boundary) blunts bulk junk; HMAC would add nothing an extracted key
// wouldn't defeat.
public class IngestMatch
{
    private const int MaxBodyBytes = 64 * 1024;
    private const int MaxPlayers = 16;        // a duos lobby has 8 players; headroom only
    private const int RateLimitPerHour = 60;  // several times the humanly possible BG match rate

    private static readonly TimeSpan RateWindow = TimeSpan.FromHours(1);
    private static readonly ConcurrentDictionary<string, (DateTime Start, int Count)> RateWindows = new();

    // truncated-SHA-256 hex pseudonyms from the plugin's HashName (all schema versions)
    private static readonly Regex HashRx = new("^[0-9a-f]{1,64}$", RegexOptions.Compiled);

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
        string? ip = ClientIp(req);
        if (ip != null && !AllowRequest(ip))
            return await Text(req, HttpStatusCode.TooManyRequests, "rate limited");

        string? body = await ReadCapped(req.Body, MaxBodyBytes);
        if (body == null)
            return await Text(req, HttpStatusCode.RequestEntityTooLarge, "oversized body");
        if (string.IsNullOrWhiteSpace(body))
            return await Text(req, HttpStatusCode.BadRequest, "empty body");

        JsonObject? doc = Normalize(body, out string region, out string? error);
        if (doc is null)
            return await Text(req, HttpStatusCode.BadRequest, error ?? "invalid body");

        try
        {
            using var ms = new MemoryStream(Encoding.UTF8.GetBytes(doc.ToJsonString()));
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

    // Reads at most maxBytes from the stream; null = the body exceeded the cap. Never
    // buffers past the cap regardless of what Content-Length claims.
    private static async Task<string?> ReadCapped(Stream stream, int maxBytes)
    {
        using var ms = new MemoryStream();
        byte[] buf = new byte[16 * 1024];
        int read;
        while ((read = await stream.ReadAsync(buf)) > 0)
        {
            if (ms.Length + read > maxBytes) return null;
            ms.Write(buf, 0, read);
        }
        return Encoding.UTF8.GetString(ms.GetBuffer(), 0, (int)ms.Length);
    }

    // Validate field-by-field and rebuild: the stored document can only ever contain
    // what the schema names, no matter what arrives.
    private static JsonObject? Normalize(string body, out string region, out string? error)
    {
        region = "US";
        error = null;
        JsonObject? src;
        try { src = JsonNode.Parse(body) as JsonObject; }
        catch { error = "invalid json"; return null; }
        if (src is null) { error = "expected json object"; return null; }

        if (!TryInt(src["schema"], 1, 99, out int schema)) { error = "bad schema"; return null; }
        if (!TryStr(src["region"], 8, out string? reg) || reg is null) { error = "bad region"; return null; }
        region = reg.ToUpperInvariant();
        if (region is not ("US" or "EU" or "AP" or "CN")) { error = "bad region"; return null; }
        if (!TryBool(src["duos"], out bool duos)) { error = "bad duos"; return null; }
        if (src["players"] is not JsonArray srcPlayers || srcPlayers.Count is < 1 or > MaxPlayers)
        { error = "bad players"; return null; }

        var players = new JsonArray();
        foreach (JsonNode? node in srcPlayers)
        {
            if (node is not JsonObject p) { error = "bad player"; return null; }
            if (!TryStr(p["h"], 64, out string? hero)) { error = "bad h"; return null; }
            if (!TryInt(p["p"], -1, 8, out int place)) { error = "bad p"; return null; }  // 0 = alive, -1 = unknown
            if (!TryInt(p["t"], 0, 10, out int tier)) { error = "bad t"; return null; }
            if (!TryInt(p["r"], 0, 1_000_000, out int rating)) { error = "bad r"; return null; }
            if (!TryInt(p["k"], 0, 10_000_000, out int rank)) { error = "bad k"; return null; }
            if (!TryStr(p["c"], 120, out string? comp)) { error = "bad c"; return null; }
            if (!TryStr(p["id"], 64, out string? id) || id is null || !HashRx.IsMatch(id)) { error = "bad id"; return null; }
            if (!TryStr(p["aid"], 64, out string? aid) || (aid != null && !HashRx.IsMatch(aid))) { error = "bad aid"; return null; }
            if (!TryBool(p["me"], out bool me)) { error = "bad me"; return null; }
            players.Add(new JsonObject
            {
                ["h"] = hero, ["p"] = place, ["t"] = tier, ["r"] = rating, ["k"] = rank,
                ["c"] = comp, ["id"] = id, ["aid"] = aid, ["me"] = me
            });
        }

        return new JsonObject
        {
            ["id"] = Guid.NewGuid().ToString("n"),
            ["schema"] = schema,
            ["region"] = region,
            ["duos"] = duos,
            ["players"] = players,
            ["ingestedUtc"] = DateTimeOffset.UtcNow.ToString("O")
        };
    }

    // ---- tiny typed readers: absent/null = null slot, wrong type or range = reject ----

    private static bool TryStr(JsonNode? n, int maxLen, out string? value)
    {
        value = null;
        if (n is null) return true; // absent or literal null — caller decides if required
        if (n is not JsonValue v || !v.TryGetValue(out string? s) || s.Length > maxLen) return false;
        value = s;
        return true;
    }

    private static bool TryInt(JsonNode? n, int min, int max, out int value)
    {
        value = 0;
        return n is JsonValue v && v.TryGetValue(out value) && value >= min && value <= max;
    }

    private static bool TryBool(JsonNode? n, out bool value)
    {
        value = false;
        return n is JsonValue v && v.TryGetValue(out value);
    }

    // ---- best-effort per-IP fixed-window limiter (per instance; no extra infra) ----

    private static string? ClientIp(HttpRequestData req)
    {
        if (!req.Headers.TryGetValues("X-Forwarded-For", out var values)) return null;
        string? first = values.FirstOrDefault()?.Split(',')[0].Trim();
        if (string.IsNullOrEmpty(first)) return null;
        int colon = first.LastIndexOf(':');
        if (colon > 0 && first.Contains('.')) first = first[..colon]; // strip IPv4 port
        return first;
    }

    private static bool AllowRequest(string ip)
    {
        DateTime now = DateTime.UtcNow;
        if (RateWindows.Count > 10_000) // opportunistic prune keeps the map bounded
        {
            foreach (var kv in RateWindows)
                if (now - kv.Value.Start > RateWindow) RateWindows.TryRemove(kv.Key, out _);
        }
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
