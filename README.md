# lobbylens-functions

The backend service for [LobbyLens](https://github.com/xhodagx/LobbyLens) — an Azure
Functions app (.NET 8 isolated) that mirrors Blizzard's Battlegrounds leaderboards for
plugin clients and ingests anonymized match telemetry. Infrastructure lives in
[lobbylens-infra](https://github.com/xhodagx/lobbylens-infra).

## Functions

### `RefreshLeaderboard` (timer, every 30 min)

Fetches Blizzard's official leaderboards for **US / EU / AP × solo / duos** (six boards),
de-duplicates, and publishes one compact JSON file per board to the `public` blob
container. Plugin clients then fetch a single cached ~50 KB file instead of each paging the
Blizzard API directly — the whole point of the backend at scale.

Published as `leaderboard_{REGION}{_duo}.json` (e.g. `leaderboard_US.json`,
`leaderboard_EU_duo.json`), anonymously readable, `Cache-Control: max-age=1800`.

### `IngestMatch` (HTTP `POST /api/match`, anonymous)

Accepts an anonymized match summary from the plugin, validates and size-caps it, stamps a
server-owned `id` + `ingestedUtc` (never trusts the client's), and writes it to the Cosmos
`matches` container (partitioned by `region`). Anonymous on purpose — a function key
embedded in a public plugin DLL is extractable, so ingest is treated as low-trust:
validated, capped, and (future) rate-limited.

## Data contracts

**Published leaderboard** (`leaderboard_{REGION}{_duo}.json`):

```json
{ "ts": 1718284800, "players": [ { "n": "hsmt", "r": 18227, "k": 1 }, … ] }
```
`ts` = unix seconds of the refresh · `n` = battletag · `r` = rating · `k` = ladder rank.

**Match summary** (`POST /api/match` request body):

```json
{
  "schema": 1,
  "region": "US",
  "duos": false,
  "players": [
    { "h": "<heroCardId>", "p": 4, "t": 6, "r": 12000, "k": 150,
      "c": "4 Mech · 2 Beast", "id": "<sha256(name)[..16]>", "me": false }
  ]
}
```
`h` hero card id · `p` final place · `t` tech tier · `r` rating · `k` ladder rank ·
`c` board comp · `id` **one-way hashed** battletag (no raw names) · `me` local player.
The server adds `id` (document id), `region` partition key, and `ingestedUtc`.

## App settings (provided by the Bicep deployment)

| Setting | Purpose |
|---|---|
| `DATA_BLOB_ENDPOINT` | data storage blob endpoint (leaderboard output) |
| `COSMOS_ENDPOINT` / `COSMOS_DATABASE` | match ingest target |
| `AzureWebJobsStorage` | Functions runtime storage |
| `APPLICATIONINSIGHTS_CONNECTION_STRING` | telemetry |

Blob and Cosmos access use the Function App's **managed identity** (`DefaultAzureCredential`)
— no data-plane keys. Locally, `DefaultAzureCredential` falls back to your `az login`.

## Build & deploy

```powershell
dotnet build LobbyLens.Functions.csproj -c Release        # build

# publish to the live app (no Azure Functions Core Tools needed):
dotnet publish LobbyLens.Functions.csproj -c Release -o publish
Compress-Archive -Path publish\* -DestinationPath deploy.zip -Force
az functionapp deployment source config-zip -g rg-lobbylens-prod -n <funcAppName> --src deploy.zip
```

> The Cosmos SDK requires an explicit `Newtonsoft.Json` package reference or the build fails.

Run locally with `func start` (requires Azure Functions Core Tools and a `local.settings.json`
— git-ignored; see the env-vars table).

## License

MIT — Copyright (c) 2026 xhodagx
