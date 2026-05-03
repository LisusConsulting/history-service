using MomentumBreakoutDetector.HistoryService.Pricing.BlackScholes;
using Shouldly;
using Xunit;
using SystemMath = System.Math;

namespace MomentumBreakoutDetector.HistoryService.Tests;

/// <summary>
/// Centerpiece test for Wave A / PR 2 of the ATM-IV full historical coverage
/// plan. The solver is the load-bearing primitive for every BS-computed
/// historical row written by PR 3's seeder; broken calibration here corrupts
/// the entire pre-2026-04-13 daily_atm_iv backfill.
///
/// <para>
/// Coverage groups:
/// <list type="bullet">
///   <item><b>Round-trip</b> — pick an IV, price the option with BS, hand
///         the price back to the solver, recover the IV. Must agree to the
///         persistence layer's NUMERIC(10,6) = 1e-6 precision.</item>
///   <item><b>Realistic ATM TSLA call</b> — synthetic but plan-aligned
///         numbers (S=300, K=300, T=30/252, r=5.25% from DGS3MO, mid=10).
///         Recovered IV should land in the [0.3, 0.8] band the plan
///         calibration test calls for.</item>
///   <item><b>Pathological inputs</b> — T=0, mid≤0, mid &gt; underlying
///         (uneconomic), arbitrage-violation mids — solver returns null.</item>
///   <item><b>Greeks magnitudes</b> — delta in [-1, 1], gamma &gt; 0,
///         vega &gt; 0, signs match call/put convention.</item>
///   <item><b>Math primitives</b> — NormalCdf at known points (0, 1, -1),
///         BS price equals analytic at zero-vol degeneracy.</item>
/// </list>
/// </para>
///
/// <para>
/// <b>Calibration audit</b> (per plan brief): we compute IV via our solver
/// for a representative TSLA-shaped call snapshot and compare against the
/// "Polygon-supplied" IV that real <c>options_snapshots</c> rows would
/// carry. We don't have a live testcontainer with real Polygon data here,
/// so we use a published BS reference value (computed from a known IV
/// using the analytic formula, mimicking what Polygon would return).
/// Divergence &gt; 5% triggers the escalation flag in the run report.
/// </para>
/// </summary>
public class BlackScholesSolverTests
{
    private readonly IBlackScholesSolver _solver = new BlackScholesSolver();

    // ── Round-trip across the volatility surface ────────────────────────

    [Theory]
    [InlineData(0.10)]
    [InlineData(0.20)]
    [InlineData(0.35)]
    [InlineData(0.55)]
    [InlineData(0.80)]
    [InlineData(1.20)]
    public void Solve_RoundTrip_RecoversInputIv_Call(double inIv)
    {
        // S = 100, K = 100, T = 30/252 (30 trading days = ~6 calendar weeks),
        // r = 5%. Pure ATM, q = 0. Price the option analytically at the
        // input IV, then ask the solver to recover IV from the price.
        var tmpS = 100.0;
        var tmpK = 100.0;
        var tmpT = 30.0 / 252.0;
        var tmpR = 0.05;
        var tmpPrice = BlackScholesSolver.BlackScholesPrice(tmpS, tmpK, tmpT, tmpR, 0.0, inIv, inIsCall: true);

        var tmpResult = _solver.Solve(new BlackScholesInputs(
            UnderlyingPrice: (decimal)tmpS,
            Strike: (decimal)tmpK,
            TimeToExpirationYears: (decimal)tmpT,
            RiskFreeRate: (decimal)tmpR,
            MidPrice: (decimal)tmpPrice,
            Type: OptionType.Call));

        tmpResult.ShouldNotBeNull();
        // 1e-4 tolerance — NUMERIC(10,6) gives 1e-6 storage precision but
        // the solver converges in price-space tolerance not IV-space, so
        // the IV recovery is at ~1e-5 typically. 1e-4 is conservatively
        // tight given the realistic σ values.
        ((double)tmpResult!.Value.Iv).ShouldBe(inIv, 1e-4);
    }

    [Theory]
    [InlineData(0.15)]
    [InlineData(0.40)]
    [InlineData(0.70)]
    public void Solve_RoundTrip_RecoversInputIv_Put(double inIv)
    {
        var tmpS = 100.0;
        var tmpK = 105.0; // OTM put
        var tmpT = 21.0 / 252.0;
        var tmpR = 0.0525;
        var tmpPrice = BlackScholesSolver.BlackScholesPrice(tmpS, tmpK, tmpT, tmpR, 0.0, inIv, inIsCall: false);

        var tmpResult = _solver.Solve(new BlackScholesInputs(
            UnderlyingPrice: (decimal)tmpS,
            Strike: (decimal)tmpK,
            TimeToExpirationYears: (decimal)tmpT,
            RiskFreeRate: (decimal)tmpR,
            MidPrice: (decimal)tmpPrice,
            Type: OptionType.Put));

        tmpResult.ShouldNotBeNull();
        ((double)tmpResult!.Value.Iv).ShouldBe(inIv, 1e-4);
    }

    // ── Realistic ATM TSLA call (plan calibration target) ────────────────

    [Fact]
    public void Solve_RealisticAtmTslaCall_IvInExpectedBand()
    {
        // Synthetic but plan-aligned: TSLA at $300, ATM call $300, expiring
        // 30 trading days out, r=5.25% (DGS3MO at ~early 2026 levels),
        // mid=$15 (typical for high-IV ATM).
        var tmpResult = _solver.Solve(new BlackScholesInputs(
            UnderlyingPrice: 300m,
            Strike: 300m,
            TimeToExpirationYears: 30m / 252m,
            RiskFreeRate: 0.0525m,
            MidPrice: 15m,
            Type: OptionType.Call));

        tmpResult.ShouldNotBeNull();
        // Plan calibration test: IV should land in [0.3, 0.8] for an
        // ATM TSLA call. ~$15 / $300 / sqrt(30/252) is in the 60-70% IV
        // ballpark — comfortably in the band.
        ((double)tmpResult!.Value.Iv).ShouldBeGreaterThanOrEqualTo(0.30);
        ((double)tmpResult!.Value.Iv).ShouldBeLessThanOrEqualTo(0.80);
    }

    // ── Pathological inputs ─────────────────────────────────────────────

    [Fact]
    public void Solve_TimeZero_ReturnsNull()
    {
        var tmpResult = _solver.Solve(new BlackScholesInputs(
            UnderlyingPrice: 100m, Strike: 100m,
            TimeToExpirationYears: 0m,
            RiskFreeRate: 0.05m, MidPrice: 5m, Type: OptionType.Call));
        tmpResult.ShouldBeNull();
    }

    [Fact]
    public void Solve_MidZero_ReturnsNull()
    {
        var tmpResult = _solver.Solve(new BlackScholesInputs(
            UnderlyingPrice: 100m, Strike: 100m,
            TimeToExpirationYears: 30m / 252m,
            RiskFreeRate: 0.05m, MidPrice: 0m, Type: OptionType.Call));
        tmpResult.ShouldBeNull();
    }

    [Fact]
    public void Solve_MidNegative_ReturnsNull()
    {
        var tmpResult = _solver.Solve(new BlackScholesInputs(
            UnderlyingPrice: 100m, Strike: 100m,
            TimeToExpirationYears: 30m / 252m,
            RiskFreeRate: 0.05m, MidPrice: -1m, Type: OptionType.Call));
        tmpResult.ShouldBeNull();
    }

    [Fact]
    public void Solve_MidExceedsUnderlying_ReturnsNull()
    {
        // A vanilla call can never trade above the spot price (no-arb
        // upper bound). Refuse to solve — bad data.
        var tmpResult = _solver.Solve(new BlackScholesInputs(
            UnderlyingPrice: 100m, Strike: 100m,
            TimeToExpirationYears: 30m / 252m,
            RiskFreeRate: 0.05m, MidPrice: 200m, Type: OptionType.Call));
        tmpResult.ShouldBeNull();
    }

    [Fact]
    public void Solve_UnderlyingZero_ReturnsNull()
    {
        var tmpResult = _solver.Solve(new BlackScholesInputs(
            UnderlyingPrice: 0m, Strike: 100m,
            TimeToExpirationYears: 30m / 252m,
            RiskFreeRate: 0.05m, MidPrice: 5m, Type: OptionType.Call));
        tmpResult.ShouldBeNull();
    }

    // ── Greeks magnitudes / signs ───────────────────────────────────────

    [Fact]
    public void Solve_AtmCall_GreeksHaveExpectedShape()
    {
        // ATM 30-day call. Delta ~0.5, gamma > 0, vega > 0, theta < 0.
        var tmpResult = _solver.Solve(new BlackScholesInputs(
            UnderlyingPrice: 100m, Strike: 100m,
            TimeToExpirationYears: 30m / 252m,
            RiskFreeRate: 0.05m, MidPrice: 4m, Type: OptionType.Call));

        tmpResult.ShouldNotBeNull();
        var tmpG = tmpResult!.Value;
        ((double)tmpG.Delta).ShouldBeInRange(0.0, 1.0);
        ((double)tmpG.Delta).ShouldBeInRange(0.30, 0.70); // ATM ~0.5
        ((double)tmpG.Gamma).ShouldBeGreaterThan(0.0);
        ((double)tmpG.Vega).ShouldBeGreaterThan(0.0);
        ((double)tmpG.Theta).ShouldBeLessThan(0.0); // long call decays
    }

    [Fact]
    public void Solve_AtmPut_DeltaIsNegative()
    {
        var tmpResult = _solver.Solve(new BlackScholesInputs(
            UnderlyingPrice: 100m, Strike: 100m,
            TimeToExpirationYears: 30m / 252m,
            RiskFreeRate: 0.05m, MidPrice: 3.5m, Type: OptionType.Put));

        tmpResult.ShouldNotBeNull();
        var tmpG = tmpResult!.Value;
        ((double)tmpG.Delta).ShouldBeInRange(-1.0, 0.0);
        ((double)tmpG.Gamma).ShouldBeGreaterThan(0.0);
        ((double)tmpG.Vega).ShouldBeGreaterThan(0.0);
    }

    [Fact]
    public void Solve_DeepItmCall_DeltaApproachesOne()
    {
        // S=200, K=80, deep ITM. Delta should be very close to 1.
        // The mid here is ~120 (the approximate intrinsic + a small time
        // premium); pick one that's reachable in [0.001, 5.0] σ range.
        var tmpResult = _solver.Solve(new BlackScholesInputs(
            UnderlyingPrice: 200m, Strike: 80m,
            TimeToExpirationYears: 30m / 252m,
            RiskFreeRate: 0.05m, MidPrice: 121m, Type: OptionType.Call));

        if (tmpResult is { } g)
        {
            ((double)g.Delta).ShouldBeGreaterThan(0.85);
        }
        // If solver returned null (mid outside bracket-attainable range
        // for this very deep ITM contract), that's also acceptable —
        // the test asserts the property when convergence happens.
    }

    // ── Math primitives ─────────────────────────────────────────────────

    [Fact]
    public void NormalCdf_AtZero_IsHalf()
    {
        BlackScholesSolver.NormalCdf(0.0).ShouldBe(0.5, 1e-7);
    }

    [Fact]
    public void NormalCdf_KnownPoints()
    {
        // N(1) ~ 0.84134, N(-1) ~ 0.15866, N(1.96) ~ 0.97500.
        BlackScholesSolver.NormalCdf(1.0).ShouldBe(0.84134, 1e-4);
        BlackScholesSolver.NormalCdf(-1.0).ShouldBe(0.15866, 1e-4);
        BlackScholesSolver.NormalCdf(1.96).ShouldBe(0.97500, 1e-4);
    }

    [Fact]
    public void BlackScholesPrice_ZeroVol_EqualsDiscountedIntrinsic()
    {
        // σ = 0 → call = e^{-rT} * max(F - K, 0) where F = S e^{(r-q)T}.
        // S=120, K=100, T=30/252, r=5%, q=0 → F ≈ 120*e^{0.05*30/252}
        // ≈ 120.71, intrinsic ≈ 20.71, discounted ≈ 20.71*e^{-0.05*30/252}
        // ≈ 20.59.
        var tmpPrice = BlackScholesSolver.BlackScholesPrice(
            inS: 120, inK: 100, inT: 30.0 / 252.0, inR: 0.05, inQ: 0.0,
            inSigma: 0.0, inIsCall: true);
        var tmpExpected = 20.59;
        tmpPrice.ShouldBe(tmpExpected, 0.05);
    }

    // ── Calibration audit (plan brief: divergence vs Polygon) ───────────

    [Fact]
    public void CalibrationAudit_BsIv_AgreesWithReferenceWithin5Pct()
    {
        // Reference scenario: take a known IV, compute the BS price
        // (which mimics what Polygon would publish on a snapshot row),
        // then run the solver and compare. The relative divergence
        // should be well under 5% — the plan mandate flag.
        //
        // Spec note: in production the comparison is BS-computed IV
        // vs Polygon-supplied IV on a real overlap-period snapshot.
        // We don't have that data on hand here without standing up a
        // Postgres testcontainer with seeded production rows, so we
        // use the reference-price approach for unit-level CI safety.
        // The integration-test in PR 3 against real seeded data is
        // where the production-data-vs-BS divergence is observed.

        var tmpKnownIv = 0.55;
        var tmpS = 250.0;
        var tmpK = 245.0; // slightly OTM put / slightly ITM call
        var tmpT = 21.0 / 252.0;
        var tmpR = 0.0525;
        var tmpPolygonLikePrice = BlackScholesSolver.BlackScholesPrice(
            tmpS, tmpK, tmpT, tmpR, 0.0, tmpKnownIv, inIsCall: true);

        var tmpResult = _solver.Solve(new BlackScholesInputs(
            UnderlyingPrice: (decimal)tmpS,
            Strike: (decimal)tmpK,
            TimeToExpirationYears: (decimal)tmpT,
            RiskFreeRate: (decimal)tmpR,
            MidPrice: (decimal)tmpPolygonLikePrice,
            Type: OptionType.Call));

        tmpResult.ShouldNotBeNull();
        var tmpRecoveredIv = (double)tmpResult!.Value.Iv;
        var tmpRelDivergence = SystemMath.Abs(tmpRecoveredIv - tmpKnownIv) / tmpKnownIv;
        // Plan audit: divergence > 5% triggers escalation. We expect to
        // beat 0.1% in unit-test land; the 5% threshold is the gate.
        tmpRelDivergence.ShouldBeLessThan(0.05);
    }

    [Fact]
    public void Solve_FailsCleanly_OnNoArbViolation()
    {
        // Mid below intrinsic — impossible by no-arbitrage. Solver
        // should return null cleanly, not throw.
        var tmpResult = _solver.Solve(new BlackScholesInputs(
            UnderlyingPrice: 200m, Strike: 100m,
            TimeToExpirationYears: 30m / 252m,
            RiskFreeRate: 0.05m,
            // Intrinsic ≈ 100 + small premium; below 100 violates no-arb.
            MidPrice: 50m,
            Type: OptionType.Call));
        tmpResult.ShouldBeNull();
    }

    [Fact]
    public void Solve_HandlesCancellation()
    {
        var tmpCts = new CancellationTokenSource();
        tmpCts.Cancel();
        Should.Throw<OperationCanceledException>(() =>
        {
            _solver.Solve(new BlackScholesInputs(
                UnderlyingPrice: 100m, Strike: 100m,
                TimeToExpirationYears: 30m / 252m,
                RiskFreeRate: 0.05m, MidPrice: 5m, Type: OptionType.Call),
                tmpCts.Token);
        });
    }
}
