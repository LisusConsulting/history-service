# Changelog

All notable changes to the history-service. Format: keep-a-changelog,
versions track Phase / micro-PR until 1.0.

## [Unreleased]

### µPR #8 — Observability + comprehensive tests (Phase 1 complete)

Added:
- `Observability/MetricsCollector.cs` — process-wide counter + ring-buffer
  store. Tracks per-`DataClass` (Bars/Nbbo/Chains/Macro) `CacheHits`,
  `UpstreamFetches`, `MissMarkers`, plus latency p50/p95/p99 over the
  most recent 1024 wire calls. Lock-free counters
  (`Interlocked.Increment`); per-kind in-flight count summed via
  registered probes (each fetcher self-registers its
  `SingleFlight.InFlightCount`).
- `GetCacheStats` RPC implementation — bridges the internal
  `MetricKind` taxonomy to the proto `DataClass` enum, supports the
  optional `data_class` filter on the request.
- `EndToEndIntegrationTests.cs` — 6 integration tests covering
  cold-start (all 4 kinds populate), warm-cache (zero upstream),
  coalesce proof (50 concurrent → 1 upstream), 5xx fail-loud, 404 →
  miss-marker, and `GetCacheStats` accuracy. Tagged
  `[Trait("Category","Integration")]` for CI filtering.
- `MetricsCollectorTests.cs` — 5 hermetic unit tests for the collector
  primitive (counter arithmetic, percentile math, in-flight aggregation,
  thread-safety, snapshot timestamp).
- README "Observability" section + structured-log samples + Phase 1
  status updated to **complete**.

Changed:
- `PolygonBarFetcher`, `PolygonNbboFetcher`, `PolygonChainFetcher`,
  `FredFetcher` — accept optional `MetricsCollector` ctor param,
  record `UpstreamFetches` + latency at the wire boundary, emit
  consistent INFO-level structured logs.
- `HistoricalBarsProvider`, `OptionQuotesProvider`, `OptionChainProvider`,
  `MacroDataProvider` — accept optional `MetricsCollector`, record
  `CacheHits` / `MissMarkers` at the cache-served / miss-write paths.
- `HistoryServiceImpl` — accepts optional `MetricsCollector`; replaces
  the `Unimplemented` throw in `GetCacheStats` with a real snapshot
  response.
- `Program.cs` — registers `MetricsCollector` as a singleton and
  threads it through the FRED + Macro provider factories.

Test counts: 27 passing (16 prior + 6 integration + 5 metrics unit).

### µPR #7 — `EnsureRangeCached` server-streaming warmup (#7, sha 9dd898c)
### µPR #6 — `SingleFlight` coalescer + wrap 4 fetchers (#6, sha 2c7c322)
### Phase E — lift fetchers onto polygon-net-client SDK 0.10.0 (#5, sha d974440)
### µPR #2 — `PolygonBarFetcher` + bars provider + `GetBars` (#4, sha 0a138fe)
### µPR #4 — `PolygonChainFetcher` + chain provider + `GetOptionChain` (#3, sha 404b764)
### µPR #5 — `FredFetcher` + macro provider + `GetMacro` (#2, sha 2b13431)
### µPR #3 — NBBO quotes + miss-markers + `GetNbbo` (#1, sha 766f549)
### µPR #1 — Scaffold (repo, compose, Dockerfile, stub gRPC) (sha c2177cf)
