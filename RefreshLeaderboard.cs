using System.Text;
using System.Text.Json;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace LobbyLens.Functions;

// Pulls Blizzard's official BG leaderboards and publishes one compact JSON file per
// region/mode to the public blob container. Plugin clients then fetch a single cached
// file instead of each hammering Blizzard with ~25-60 paged requests.
public class RefreshLeaderboard
{
    private const string BaseUrl = "https://hearthstone.blizzard.com/en-us/api/community/leaderboardsData";
    private const int MaxPages = 200;
    private static readonly string[] Regions = { "US", "EU", "AP" };

    private static readonly HttpClient Http = CreateHttp();

    private static readonly Lazy<BlobContainerClient> Container = new(() =>
    {
        string endpoint = Environment.GetEnvironmentVariable("DATA_BLOB_ENDPOINT")
                          ?? throw new InvalidOperationException("DATA_BLOB_ENDPOINT not set");
        return new BlobContainerClient(new Uri($"{endpoint}public"), new DefaultAzureCredential());
    });

    private readonly ILogger _log;
    public RefreshLeaderboard(ILoggerFactory lf) => _log = lf.CreateLogger<RefreshLeaderboard>();

    private static HttpClient CreateHttp()
    {
        var h = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
        h.DefaultRequestHeaders.Add("User-Agent", "LobbyLens-Backend/1.0");
        return h;
    }

    [Function("RefreshLeaderboard")]
    public async Task Run([TimerTrigger("0 */30 * * * *")] TimerInfo timer)
    {
        BlobContainerClient container = Container.Value;

        foreach (string region in Regions)
        {
            foreach (bool duo in new[] { false, true })
            {
                string label = $"{region}{(duo ? "_duo" : "")}";
                try
                {
                    (string json, int count) = await BuildLeaderboardJson(region, duo);
                    if (count == 0) { _log.LogWarning("Empty leaderboard {Label}, not overwriting", label); continue; }

                    BlobClient blob = container.GetBlobClient($"leaderboard_{label}.json");
                    using var ms = new MemoryStream(Encoding.UTF8.GetBytes(json));
                    await blob.UploadAsync(ms, new BlobUploadOptions
                    {
                        HttpHeaders = new BlobHttpHeaders
                        {
                            ContentType = "application/json",
                            CacheControl = "public, max-age=1800"
                        }
                    });
                    _log.LogInformation("Published leaderboard_{Label}.json ({Count} players)", label, count);
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "Failed to refresh {Label}", label);
                }
            }
        }
    }

    // Compact output: {"ts":<unix>,"players":[{"n":name,"r":rating,"k":rank},...]}
    // Pages are parsed as real JSON (shape: leaderboard.rows[{rank,accountid,rating}],
    // leaderboard.pagination.totalPages). GetString() fully unescapes names, so the
    // re-serialize can never double-escape — the plugin then unescapes exactly once.
    private static async Task<(string json, int count)> BuildLeaderboardJson(string region, bool duo)
    {
        string board = duo ? "battlegroundsduo" : "battlegrounds";

        string first = await Http.GetStringAsync($"{BaseUrl}?region={region}&leaderboardId={board}&page=1");
        int totalPages = Math.Min(TotalPages(first), MaxPages);

        var bodies = new List<string> { first };
        if (totalPages > 1)
        {
            var gate = new SemaphoreSlim(6);
            var tasks = new List<Task<string>>();
            for (int p = 2; p <= totalPages; p++)
            {
                int page = p;
                tasks.Add(Task.Run(async () =>
                {
                    await gate.WaitAsync();
                    try { return await Http.GetStringAsync($"{BaseUrl}?region={region}&leaderboardId={board}&page={page}"); }
                    finally { gate.Release(); }
                }));
            }
            bodies.AddRange(await Task.WhenAll(tasks));
        }

        var seen = new HashSet<string>();
        var sb = new StringBuilder();
        sb.Append("{\"ts\":").Append(DateTimeOffset.UtcNow.ToUnixTimeSeconds()).Append(",\"players\":[");
        int count = 0;
        foreach (string body in bodies)
        {
            foreach ((string name, int rank, int rating) in Rows(body))
            {
                if (!seen.Add(name)) continue;
                if (count++ > 0) sb.Append(',');
                sb.Append("{\"n\":").Append(JsonSerializer.Serialize(name))
                  .Append(",\"r\":").Append(rating)
                  .Append(",\"k\":").Append(rank).Append('}');
            }
        }
        sb.Append("]}");
        return (sb.ToString(), count);
    }

    private static int TotalPages(string body)
    {
        try
        {
            using JsonDocument doc = JsonDocument.Parse(body);
            if (doc.RootElement.TryGetProperty("leaderboard", out JsonElement lb)
                && lb.TryGetProperty("pagination", out JsonElement pg)
                && pg.TryGetProperty("totalPages", out JsonElement tp)
                && tp.TryGetInt32(out int pages)) { return pages; }
        }
        catch (JsonException) { }
        return 1;
    }

    // Defensive row walk: a malformed page contributes nothing rather than throwing,
    // and the caller's count==0 guard keeps a good blob from being overwritten.
    private static List<(string Name, int Rank, int Rating)> Rows(string body)
    {
        var rows = new List<(string, int, int)>();
        try
        {
            using JsonDocument doc = JsonDocument.Parse(body);
            if (!doc.RootElement.TryGetProperty("leaderboard", out JsonElement lb)
                || !lb.TryGetProperty("rows", out JsonElement arr)
                || arr.ValueKind != JsonValueKind.Array) { return rows; }
            foreach (JsonElement row in arr.EnumerateArray())
            {
                if (row.ValueKind != JsonValueKind.Object
                    || !row.TryGetProperty("accountid", out JsonElement acc) || acc.ValueKind != JsonValueKind.String
                    || !row.TryGetProperty("rank", out JsonElement rk) || !rk.TryGetInt32(out int rank)
                    || !row.TryGetProperty("rating", out JsonElement rt) || !rt.TryGetInt32(out int rating)) { continue; }
                string? name = acc.GetString();
                if (string.IsNullOrWhiteSpace(name) || rating <= 0) { continue; }
                rows.Add((name, rank, rating));
            }
        }
        catch (JsonException) { }
        return rows;
    }
}
