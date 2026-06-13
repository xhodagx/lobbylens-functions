using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
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

    private static readonly Regex RowRx = new(
        "\\{\"rank\":(\\d+),\"accountid\":\"((?:[^\"\\\\]|\\\\.)*)\",\"rating\":(\\d+)",
        RegexOptions.Compiled);
    private static readonly Regex TotalPagesRx = new("\"totalPages\":(\\d+)", RegexOptions.Compiled);

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
    private static async Task<(string json, int count)> BuildLeaderboardJson(string region, bool duo)
    {
        string board = duo ? "battlegroundsduo" : "battlegrounds";

        string first = await Http.GetStringAsync($"{BaseUrl}?region={region}&leaderboardId={board}&page=1");
        int totalPages = 1;
        Match tp = TotalPagesRx.Match(first);
        if (tp.Success) totalPages = Math.Min(int.Parse(tp.Groups[1].Value), MaxPages);

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
            foreach (Match m in RowRx.Matches(body))
            {
                string name = m.Groups[2].Value;
                if (!seen.Add(name)) continue;
                int rank = int.Parse(m.Groups[1].Value);
                int rating = int.Parse(m.Groups[3].Value);
                if (count++ > 0) sb.Append(',');
                sb.Append("{\"n\":").Append(JsonSerializer.Serialize(name))
                  .Append(",\"r\":").Append(rating)
                  .Append(",\"k\":").Append(rank).Append('}');
            }
        }
        sb.Append("]}");
        return (sb.ToString(), count);
    }
}
