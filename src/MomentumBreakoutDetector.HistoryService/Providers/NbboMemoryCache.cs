using System.Collections.Concurrent;

namespace MomentumBreakoutDetector.HistoryService.Providers;

/// <summary>
/// Process-wide in-memory NBBO cache (the "in-memory NBBO cache pattern
/// from PR #98"). Sits in front of the postgres cache so a backtest's
/// per-bar monitor loop doesn't even pay the DB round-trip on the warm
/// path. Bounded by an LRU-ish eviction once <see cref="MaxEntries"/>
/// is exceeded.
///
/// We keep three maps:
///   - hits:     (ticker, requestedTs)  → quote
///   - misses:   (ticker, requestedTs)  → unit
/// Both share a single eviction queue so memory stays bounded under a
/// long-running service. Concurrent dictionaries — there's no critical
/// section that requires stronger locking; cache-line races at the
/// boundary are benign (worst case: one extra DB round-trip).
/// </summary>
public sealed class NbboMemoryCache
{
    public const int DefaultMaxEntries = 200_000;

    private readonly int m_MaxEntries;
    private readonly ConcurrentDictionary<NbboKey, OptionQuoteRecord> m_Hits = new();
    private readonly ConcurrentDictionary<NbboKey, byte> m_Misses = new();
    private readonly ConcurrentQueue<NbboKey> m_HitOrder = new();
    private readonly ConcurrentQueue<NbboKey> m_MissOrder = new();

    public NbboMemoryCache() : this(DefaultMaxEntries) { }

    public NbboMemoryCache(int inMaxEntries)
    {
        m_MaxEntries = inMaxEntries > 0 ? inMaxEntries : DefaultMaxEntries;
    }

    public int MaxEntries => m_MaxEntries;
    public int HitCount => m_Hits.Count;
    public int MissCount => m_Misses.Count;

    public bool TryGetHit(string inTicker, DateTime inTsUtc, out OptionQuoteRecord? outRecord)
    {
        if (m_Hits.TryGetValue(new NbboKey(inTicker, inTsUtc), out var tmpVal))
        {
            outRecord = tmpVal;
            return true;
        }
        outRecord = null;
        return false;
    }

    public bool IsMiss(string inTicker, DateTime inTsUtc)
        => m_Misses.ContainsKey(new NbboKey(inTicker, inTsUtc));

    public void PutHit(OptionQuoteRecord inRecord)
    {
        var tmpKey = new NbboKey(inRecord.Ticker, inRecord.RequestedTsUtc);
        if (m_Hits.TryAdd(tmpKey, inRecord))
        {
            m_HitOrder.Enqueue(tmpKey);
            EvictIfNeeded(m_Hits, m_HitOrder);
        }
        else
        {
            m_Hits[tmpKey] = inRecord;
        }
    }

    public void PutMiss(string inTicker, DateTime inTsUtc)
    {
        var tmpKey = new NbboKey(inTicker, inTsUtc);
        if (m_Misses.TryAdd(tmpKey, 0))
        {
            m_MissOrder.Enqueue(tmpKey);
            EvictIfNeeded(m_Misses, m_MissOrder);
        }
    }

    private void EvictIfNeeded<TVal>(
        ConcurrentDictionary<NbboKey, TVal> inMap,
        ConcurrentQueue<NbboKey> inOrder)
    {
        while (inMap.Count > m_MaxEntries && inOrder.TryDequeue(out var tmpKey))
        {
            inMap.TryRemove(tmpKey, out _);
        }
    }

    private readonly record struct NbboKey(string Ticker, DateTime Ts);
}
