# mbd-history

Single source of historical market data for the MomentumBreakoutDetector platform.
A standalone gRPC service that owns the cache layer in front of Polygon.io,
FRED, and other upstream providers. Consumers (backtest engine, live engine
NBBO at order time, AI service) call this service instead of hitting upstreams
directly so each unique key is fetched exactly once across the fleet.

> **Status: Phase 1, micro-PR #1 — deployable shell only.**
> The service builds, starts, exposes `/health`, and registers all RPCs.
> Every gRPC method currently returns `Unimplemented`; the real provider
> lifts land in PRs #2-#7 (see [Phase 1 status](#phase-1-status) below).

---

## Service purpose

Today, the MBD engine fetches the same historical bars / option chains /
NBBO quotes / FRED observations every time a backtest cold-starts.
mbd-history flips that on its head:

- **Single-flight coalescing** — concurrent identical fetches share one
  upstream call.
- **Write-through cache** — first fetch hits the upstream and persists to
  TimescaleDB; every subsequent fetch is a DB read.
- **Miss markers** — known-empty keys (holidays, halts, pre-listing) are
  recorded so cold-starts skip the re-fetch.
- **Range warmup** — backtests call `EnsureRangeCached` once up-front;
  the service identifies gaps, batches upstream calls, streams progress.

The contract is `src/MomentumBreakoutDetector.HistoryService.Contracts/Protos/history.proto`.

---

## Run locally

```bash
# from the repo root:
docker compose up -d
docker compose ps         # both services should show healthy
curl http://localhost:30006/health
```

Ports:

| Service                | Host port | Container port | Purpose                |
|------------------------|----------:|---------------:|------------------------|
| `mbd-history` (gRPC)   |     30005 |           8080 | gRPC h2c               |
| `mbd-history` (HTTP)   |     30006 |           8081 | `/health`, JSON banner |
| `mbd-history-postgres` |     35432 |           5432 | TimescaleDB            |

The split ports exist because cleartext gRPC requires HTTP/2-only on the
endpoint (no ALPN to negotiate from), and HTTP/1 health probes need their
own HTTP/1-only endpoint. The host ports avoid the dev clone (`15432` /
`10005`) and paper clone (`25433` / `20005`) of the main MBD project. `mbd_history_pgdata` is a named volume
so `docker compose down` does not wipe the cache; `docker compose down -v`
does.

### Probing the gRPC surface

The service registers gRPC reflection in Development, so:

```bash
grpcurl -plaintext localhost:30005 list
# -> mbd.history.v1.HistoryService

grpcurl -plaintext -d '{"symbol":"TSLA"}' localhost:30005 mbd.history.v1.HistoryService/GetBars
# -> rpc error: code = Unimplemented desc = TODO: micro-PR #2 ...
```

---

## gRPC endpoint contract

See [`Protos/history.proto`](src/MomentumBreakoutDetector.HistoryService.Contracts/Protos/history.proto)
for the full contract. Phase 1 surface:

- `GetBars(GetBarsRequest) → GetBarsResponse`
- `GetNbbo(GetNbboRequest) → GetNbboResponse`
- `GetOptionChain(GetOptionChainRequest) → GetOptionChainResponse`
- `GetMacro(GetMacroRequest) → GetMacroResponse`
- `EnsureRangeCached(...) → stream EnsureRangeCachedProgress`
- `GetCacheStats(...) → GetCacheStatsResponse`

Auth is **MVP-unauthenticated**. Phase 2 adds M2M client_credentials
against `https://sts.motuzko.com` (scope: `mbd-history`).

---

## Migration plan (5 phases)

| Phase | Goal                                                                             |
|------:|----------------------------------------------------------------------------------|
|     1 | **Stand up the service.** New repo, gRPC contract, providers, single-flight, warmup, tests. (We are here.) |
|     2 | **Adopt by backtest engine.** `IHistoryClient` flag-gated swap from in-process providers to the gRPC client. |
|     3 | **Cut over data ownership.** `pg_dump` from the MBD dev DB into mbd-history; flip the production reads. |
|     4 | **Live engine NBBO at order time.** Live engine queries mbd-history for fresh-quote-at-decision for spreads. |
|     5 | **Decommission in-process cache.** Delete duplicate fetcher / provider code from the main MBD repo. |

---

## Phase 1 status

Phase 1 is split into 8 micro-PRs so each one is independently reviewable
and revertable. **PR #1 is the deployable shell**; the rest lift one
upstream/provider pair at a time.

| #  | Title                                                  | Status     |
|---:|--------------------------------------------------------|------------|
|  1 | Scaffold (repo, compose, Dockerfile, stub gRPC)        | **Done**   |
|  2 | Lift bars: PolygonBarFetcher + bars provider + GetBars | Not started |
|  3 | Lift macro: FredFetcher + macro provider + GetMacro    | Not started |
|  4 | Lift chains: PolygonChainFetcher + GetOptionChain      | Not started |
|  5 | Lift NBBO: NBBO fetcher + miss-markers + GetNbbo       | Not started |
|  6 | SingleFlight coalescer + cache-stats accumulator       | Not started |
|  7 | EnsureRangeCached warmup orchestrator                  | Not started |
|  8 | Testcontainers integration suite                       | Not started |

Each subsequent PR opens against `develop` and merges back into it.
`main` lights up at the end of Phase 1.

---

## Build / test (without Docker)

```bash
dotnet restore HistoryService.slnx
dotnet build HistoryService.slnx --configuration Release
dotnet test HistoryService.slnx --configuration Release
```

---

## Layout

```
.
├── HistoryService.slnx                      # solution file (XML format)
├── Directory.Packages.props                 # central package versions
├── Dockerfile                               # multi-stage SDK→runtime
├── docker-compose.yml                       # postgres + service
├── docker/postgres/init.sql                 # first-boot bootstrap
├── docs/                                    # design notes, ADRs (TBD)
├── src/
│   ├── MomentumBreakoutDetector.HistoryService/
│   │   ├── Program.cs                       # host bootstrap
│   │   ├── HistoryServiceImpl.cs            # gRPC stub (PRs #2-#7 fill in)
│   │   └── HistoryServiceOptions.cs
│   └── MomentumBreakoutDetector.HistoryService.Contracts/
│       └── Protos/history.proto             # gRPC contract
├── tests/
│   └── MomentumBreakoutDetector.HistoryService.Tests/
└── tools/
    ├── build.ps1 / build.sh                 # docker build with git metadata
    └── migrations/
        ├── 001-create-historical-bars.sql
        ├── 002-create-historical-options-quotes.sql
        ├── 003-create-historical-options-contracts.sql
        ├── 004-create-macro-data.sql
        ├── 005-create-miss-markers.sql
        └── 006-create-cache-stats.sql
```
