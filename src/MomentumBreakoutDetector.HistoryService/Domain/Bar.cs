namespace MomentumBreakoutDetector.HistoryService.Domain;

/// <summary>
/// Internal bar record used by the cache + fetcher layer. Mirrors the
/// MBD `Domain.Bar` record (PR #129) verbatim — kept as a separate
/// type rather than reusing the proto-generated `Bar` so the data path
/// (DB ↔ fetcher ↔ provider) stays free of proto/Google.Protobuf
/// dependencies. The gRPC layer maps Domain.Bar → V1.Bar at the edge
/// (HistoryServiceImpl.GetBars).
///
/// Decimals not doubles: we keep the Polygon return on the wire as
/// double (per proto), but inside the cache we store DECIMAL(18,4) so
/// repeated read/write cycles can't accumulate fp drift across runs.
/// </summary>
public sealed record Bar(
    string Symbol,
    DateTime Timestamp,
    decimal Open,
    decimal High,
    decimal Low,
    decimal Close,
    decimal Volume,
    decimal VWAP);
