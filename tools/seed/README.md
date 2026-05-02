# mbd-history seed driver

A one-shot console app (`MomentumBreakoutDetector.HistoryService.Seeder`)
that drives the running `mbd-history` gRPC service through a
deterministic backfill of one symbol's bars / chains / macro / minute-NBBO
across a date window.

The seeder is the **canonical way** to bootstrap mbd-history's TimescaleDB
cache before a backtest cold-start; everything it pulls is write-through-
cached and miss-markered, so a backtest covering the same window costs
zero upstream API calls.

---

## When to use it

- **Phase 3 cutover prep** — backfill the production history-service DB
  with the data MBD's backtest engine needs before you flip the
  `IHistoryClient` flag from in-process providers to gRPC.
- **Local development** — pre-warm a fresh TimescaleDB volume so iterating
  on backtest logic doesn't cost Polygon API quota.

This tool is **not** a daemon. Run it once, watch it drain, hand control
back to the operator.

---

## Quickstart

Bring up the service (Postgres + the .NET host) on the standard ports:

```bash
docker compose up -d
docker compose ps        # mbd-history + mbd-history-postgres healthy
curl http://localhost:30006/health
```

Then run the seeder for a 6-month window:

```bash
dotnet run \
  --project tools/seed/MomentumBreakoutDetector.HistoryService.Seeder/MomentumBreakoutDetector.HistoryService.Seeder.csproj \
  -c Release \
  -- \
    --symbol TSLA \
    --from 2025-11-02 \
    --to   2026-05-02 \
    --concurrency 32 \
    --checkpoint-file ./tools/seed/checkpoint.tsla.json \
    --history-grpc-host localhost \
    --history-grpc-port 30005 \
    --log-file ./tools/seed/seed.tsla.log
```

**Expected wall-clock:** ~5–6 hours at concurrency=32 against a Polygon
Advanced ($199/mo) tier and a clean cache. Smoke test on a single trading
day (2026-04-15) ran 49,140 NBBO calls in 2m 23s (~342 calls/s). Six
months ≈ 125 trading days × ~2m/day ≈ 4–5 hours; budget 8 hours for
slack on chain-fetch slowness and edge cases.

---

## What it actually does

1. **Range warmup** (single up-front gRPC call):
   `EnsureRangeCached(symbols=[TSLA], data_classes=[BARS, CHAINS, MACRO], from, to)`
   — server-streams progress until the cache covers the full range.
   NBBO is **not** in the warmup because per-minute NBBO for ~120
   contracts × 390 minutes per day is too granular for the warmup path.

2. **Per-trading-day loop** (skips weekends + US market holidays via
   `TradingCalendar.cs`):
   1. `GetOptionChain(TSLA, day)` → ~5,000 contracts.
   2. Filter to **strike band ATM ± 5%** (using TSLA close on that day
      pulled from the warmed minute bars) and **DTE ≤ 10 days**.
      Typical filtered count: ~120 contracts.
   3. For every RTH minute (9:30 ET → 16:00 ET = 390 minutes; half-days
      truncated at 13:00 ET), and every contract:
      `GetNbbo(contract.ticker, ts)` — concurrency-bounded by
      `--concurrency` (default 32).
   4. Each NBBO call wrapped in a `try { } catch { retry(500ms) } catch { log+continue }` —
      one bad contract never aborts a day.
   5. After the day finishes, **checkpoint**.

3. **Final report** prints total NBBO calls, wall-clock, observed rate,
   p50/p95 latency from `GetCacheStats`.

---

## Checkpoint behavior

A JSON file at `--checkpoint-file` records:

```json
{
  "symbol": "TSLA",
  "lastCompletedDate": "2026-04-15",
  "totalDaysFetched": 87,
  "totalKeysFetched": 4275180,
  "startedAtUtc": "2026-05-02T01:48:17Z",
  "updatedAtUtc": "2026-05-02T01:50:40Z"
}
```

Written **after every fully-completed trading day** (atomic via
write-temp-then-rename). On resume, all dates `<= lastCompletedDate` are
skipped; the seeder picks up at `lastCompletedDate + 1 trading day`.

The cache layer's UNIQUE constraints (e.g. `historical_options_quotes
(ticker, ts) PRIMARY KEY`) make the seed **idempotent** — even if a crash
left a half-written day, re-running over that day silently no-ops on
collisions and re-fills the gaps.

If you change `--symbol` mid-stream, the seeder refuses to load a
checkpoint with a different symbol — delete the file or pass a different
`--checkpoint-file`.

---

## Aborting + resuming

- **Ctrl+C** during a run: the in-flight NBBO calls finish, the per-day
  checkpoint is preserved (last fully-completed day, not the in-flight
  one). The process exits with code 130.
- **Resume**: re-run with the **same** `--checkpoint-file`. The seeder
  re-runs `EnsureRangeCached` (idempotent — completes in seconds when the
  cache already covers the range) and continues at the next pending day.
- **Force restart**: delete the checkpoint file. The seeder starts at
  `--from`. The cache layer means redundant work is cheap (cache hits
  cost a postgres SELECT, not a Polygon call).

---

## Monitoring

While running:

- **stdout** prints a structured line per day plus a heartbeat once a
  minute (`[heartbeat] day=N/M calls=… rate=…/s eta=…`).
- **`--log-file`** mirrors stdout to a file you can `tail -f`.
- **`GetCacheStats`** is the operator's source of truth. From a second
  shell:

  ```bash
  grpcurl -plaintext localhost:30005 mbd.history.v1.HistoryService/GetCacheStats
  ```

  Watch `Nbbo.upstream_fetches` rise (Polygon calls), `Nbbo.cache_hits`
  rise (SingleFlight + intra-day reuse). At 100% cold start, `up`:`hit`
  ≈ 1:3 — SingleFlight folds concurrent identical calls and adjacent
  minutes often share a NBBO.

  **Smoke test snapshot (1 day, 2026-04-15, TSLA):**
  ```
  Bars  (req=1 hit=1 up=2)
  Chains(req=1 hit=1 up=6)            # multi-page sweep
  Macro (req=1 hit=0 up=1 miss=1)
  Nbbo  (req=49140 hit=35658 up=13482)  # ~3.6× cache:upstream
  ```

---

## Tunables

| Flag                  | Default       | Notes                                                              |
| --------------------- | ------------- | ------------------------------------------------------------------ |
| `--symbol`            | _required_    | Underlying ticker. Currently only TSLA is exercised in production. |
| `--from` / `--to`     | _required_    | `YYYY-MM-DD`. Inclusive on both ends.                              |
| `--concurrency`       | 32            | In-flight NBBO calls. Polygon Advanced supports more; 32 is the safe-default Lisus has used elsewhere. |
| `--checkpoint-file`   | `./checkpoint.json` | Path to the JSON checkpoint. Atomic writes.                  |
| `--history-grpc-host` | `localhost`   | Hostname the seeder dials.                                         |
| `--history-grpc-port` | `30005`       | gRPC h2c port. Matches `docker-compose.yml` default.               |
| `--log-file`          | _unset_       | If set, mirrors stdout to a file.                                  |
| `--strike-band-pct`   | `0.05`        | ATM ± 5%. Matches MBD's strategy band.                             |
| `--dte-max-days`      | `10`          | Filter contracts with DTE in `[0, N]` from each as-of date.        |

---

## Known sharp edges

- **Service must be on Phase 1 µPRs #1–#8.** The current `develop` HEAD
  satisfies this. An older deployed image will return `Unimplemented`
  for `EnsureRangeCached` and the seeder fails at the warmup step.
  Verify with `grpcurl -plaintext localhost:30005 list`.
- **Holiday list is hardcoded** in `TradingCalendar.cs` for 2025-2026.
  Extend the list before seeding past 2026-12-31.
- **Half-days are honored** (early close at 13:00 ET — see Black Friday
  and Christmas Eve in 2025). Half-day RTH = 9:30 → 13:00 ET = 210
  minutes. The seeder handles this transparently.
- **Concurrency above 32 risks Polygon 429s.** The history-service has
  its own `ConcurrencyLimitingHandler` (default 8 per Polygon endpoint)
  that backs off, so the bottleneck shifts to the service rather than
  to wire calls. 32 is empirically the sweet spot.
- **Bars warmup chunks 1m bars in one request.** A six-month range is
  served as ~10 chunked Polygon calls — fast and within Advanced rate
  limits.

---

## Files

```
tools/seed/
├── README.md                                                  # this
└── MomentumBreakoutDetector.HistoryService.Seeder/
    ├── MomentumBreakoutDetector.HistoryService.Seeder.csproj  # .NET 10 console
    ├── Program.cs                                             # arg parse + bootstrap
    ├── SeedOptions.cs                                         # strongly-typed args
    ├── Checkpoint.cs                                          # JSON checkpoint, atomic save
    ├── TradingCalendar.cs                                     # US holiday + half-day lists
    └── SeedEngine.cs                                          # the actual loop
```
