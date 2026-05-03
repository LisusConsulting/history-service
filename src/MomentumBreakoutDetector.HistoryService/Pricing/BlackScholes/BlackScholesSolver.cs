using SystemMath = System.Math;

namespace MomentumBreakoutDetector.HistoryService.Pricing.BlackScholes;

/// <summary>
/// Option type for the Black-Scholes solver. Mirrors the call/put split on
/// <c>OptionContractRow.ContractType</c> in the chain provider.
/// </summary>
public enum OptionType
{
    Call = 1,
    Put = 2,
}

/// <summary>
/// Inputs to the IV solver. All money figures use <c>decimal</c> (matches the
/// rest of the persistence layer's pricing precision); time-to-expiration
/// is in years computed via 252 trading days/year per the calibration locked
/// in the Wave A / PR 2 plan.
/// </summary>
/// <param name="UnderlyingPrice">Spot S of the underlying at the snapshot timestamp.</param>
/// <param name="Strike">Strike K of the option contract.</param>
/// <param name="TimeToExpirationYears">T = trading-days-to-expiry / 252. Caller is responsible for the day-count math; the solver clamps T &gt;= 1/252 to avoid the 0-day-to-expiry numerical singularity.</param>
/// <param name="RiskFreeRate">r as a decimal fraction (e.g. 0.0525 for 5.25% — DGS3MO from FRED).</param>
/// <param name="MidPrice">Mid-price (bid+ask)/2 of the contract. Caller is responsible for the bid/ask sanity-check (skip if invalid).</param>
/// <param name="Type">Call or Put.</param>
/// <param name="DividendYield">q as a decimal fraction. Default 0 (TSLA pays none); plan calibration locks q=0 for now and adds a corp-action lookup later when extending to dividend underlyings.</param>
public readonly record struct BlackScholesInputs(
    decimal UnderlyingPrice,
    decimal Strike,
    decimal TimeToExpirationYears,
    decimal RiskFreeRate,
    decimal MidPrice,
    OptionType Type,
    decimal DividendYield = 0m);

/// <summary>
/// Solver outputs: implied volatility plus the four greeks Lisus's signal
/// stack consumes (delta / gamma / theta / vega). Theta is the first-order
/// per-year derivative — consumers that want per-day theta divide by 252
/// (or 365 — the choice is the consumer's, not the solver's).
/// </summary>
/// <param name="Iv">Annualized implied volatility (decimal — 0.45 = 45%).</param>
/// <param name="Delta">∂V/∂S — sensitivity of price to spot. Range [-1, 1].</param>
/// <param name="Gamma">∂²V/∂S² — convexity. Always &gt; 0 for vanilla European options.</param>
/// <param name="Theta">∂V/∂T per year. Negative for long calls/puts (price decays toward expiration).</param>
/// <param name="Vega">∂V/∂σ per unit of vol. Always &gt; 0 for vanilla European options.</param>
public readonly record struct BlackScholesOutputs(
    decimal Iv,
    decimal Delta,
    decimal Gamma,
    decimal Theta,
    decimal Vega);

/// <summary>
/// IV solver contract. Implementations return <c>null</c> on convergence
/// failure so callers can persist NULL IV/greeks per the calibration spec
/// (matches Polygon's "deep ITM/OTM not computable" pattern).
/// </summary>
public interface IBlackScholesSolver
{
    /// <summary>
    /// Solve for IV from the given inputs and compute the analytical greeks
    /// at the converged IV. Returns <c>null</c> when neither Newton-Raphson
    /// nor the Brent fallback converged within tolerance, when the input
    /// shape is degenerate (T=0, mid&lt;=0, mid &gt; underlying for a call,
    /// etc.), or when no IV in the search range produces a price within
    /// tolerance of <see cref="BlackScholesInputs.MidPrice"/>.
    /// </summary>
    BlackScholesOutputs? Solve(BlackScholesInputs in_, CancellationToken inCt = default);
}

/// <summary>
/// Black-Scholes implied-volatility solver implementing the calibration
/// locked in Wave A / PR 2 of the ATM-IV full historical coverage plan
/// (docs/research/atm-iv-full-historical-coverage-plan-2026-05-03.md):
///
/// <list type="bullet">
///   <item>Newton-Raphson on the BS price equation, 50 iterations max,
///         convergence on |price(σ) - mid| &lt; 1e-6.</item>
///   <item>Brent's method fallback over the bracket [0.001, 5.0] when
///         NR fails to converge (off-bracket starting guesses, vega
///         near zero, oscillation, etc.).</item>
///   <item>Day count: caller-provided T in years (caller passes
///         tradingDays / 252).</item>
///   <item>Greeks computed analytically at the converged IV.</item>
/// </list>
///
/// <para>
/// <b>Decimal vs double</b>. Input + output use <c>decimal</c> for parity
/// with the rest of the pricing pipeline, but the inner numerical loop
/// works in <c>double</c>. <c>Math.Exp</c> / <c>Math.Log</c> /
/// <c>Math.Sqrt</c> exist only on <c>double</c>, and the volatility
/// search converges to ~7 sig figs which fits comfortably inside the
/// 15-decimal-digit precision of <c>double</c>. Conversion at the
/// boundary loses no precision the persistence layer can store
/// (<c>NUMERIC(10,6)</c> is 6 fractional digits).
/// </para>
///
/// <para>
/// <b>Stateless</b>. The solver is a pure function of its inputs; the
/// class can safely be registered as a Singleton in DI and called from
/// any number of threads concurrently.
/// </para>
/// </summary>
public sealed class BlackScholesSolver : IBlackScholesSolver
{
    /// <summary>Newton-Raphson iteration cap.</summary>
    public const int NewtonRaphsonMaxIterations = 50;

    /// <summary>Convergence tolerance (price-space) — |price(σ) - mid| &lt; 1e-6.</summary>
    public const double Tolerance = 1e-6;

    /// <summary>Brent-method bracket lower bound (0.1% IV — practically zero).</summary>
    public const double BrentLowerBound = 0.001;

    /// <summary>Brent-method bracket upper bound (500% IV — practically capped).</summary>
    public const double BrentUpperBound = 5.0;

    /// <summary>Brent iteration cap.</summary>
    public const int BrentMaxIterations = 100;

    /// <summary>
    /// Minimum T to avoid the t→0 singularity. 1 trading day = 1/252 years
    /// is the smallest sensible time-to-expiration; smaller values clamp
    /// here. Same-day expiry callers should treat T=0 as a degenerate
    /// input and not call the solver.
    /// </summary>
    public const double MinTimeYears = 1.0 / 252.0;

    /// <inheritdoc/>
    public BlackScholesOutputs? Solve(BlackScholesInputs in_, CancellationToken inCt = default)
    {
        // ── Degenerate-input guards ──────────────────────────────────────
        // Same-day expiry → BS undefined. Callers should pre-filter, but
        // double-guard here.
        if (in_.TimeToExpirationYears <= 0m) return null;
        if (in_.MidPrice <= 0m) return null;
        if (in_.UnderlyingPrice <= 0m) return null;
        if (in_.Strike <= 0m) return null;

        var tmpS = (double)in_.UnderlyingPrice;
        var tmpK = (double)in_.Strike;
        var tmpT = SystemMath.Max((double)in_.TimeToExpirationYears, MinTimeYears);
        var tmpR = (double)in_.RiskFreeRate;
        var tmpQ = (double)in_.DividendYield;
        var tmpMid = (double)in_.MidPrice;
        var tmpIsCall = in_.Type == OptionType.Call;

        // No-arbitrage bounds: a call must trade ≥ max(S e^{-qT} - K e^{-rT}, 0)
        // and ≤ S e^{-qT}. A put: ≥ max(K e^{-rT} - S e^{-qT}, 0) and
        // ≤ K e^{-rT}. Mid prices outside these bounds are bad data
        // (crossed quotes after freshness gate, end-of-life contracts,
        // mis-printed prices). Refuse to solve — the surface is
        // unrecoverable rather than mis-attributed.
        var tmpDiscS = tmpS * SystemMath.Exp(-tmpQ * tmpT);
        var tmpDiscK = tmpK * SystemMath.Exp(-tmpR * tmpT);
        var tmpLow = tmpIsCall ? SystemMath.Max(tmpDiscS - tmpDiscK, 0.0)
                                : SystemMath.Max(tmpDiscK - tmpDiscS, 0.0);
        var tmpHigh = tmpIsCall ? tmpDiscS : tmpDiscK;
        // Allow a small floating tolerance around the bounds — quote
        // mid prices can sit microscopically below intrinsic on
        // deep-ITM contracts due to rounding.
        if (tmpMid < tmpLow - 1e-4) return null;
        if (tmpMid > tmpHigh + 1e-4) return null;

        // ── Newton-Raphson with Manaster-Koehler initial guess ──────────
        var tmpSigma = ManasterKoehlerInitialGuess(tmpS, tmpK, tmpR, tmpQ, tmpT);
        var tmpConverged = false;
        for (var i = 0; i < NewtonRaphsonMaxIterations; i++)
        {
            inCt.ThrowIfCancellationRequested();
            var tmpPrice = BlackScholesPrice(tmpS, tmpK, tmpT, tmpR, tmpQ, tmpSigma, tmpIsCall);
            var tmpDiff = tmpPrice - tmpMid;
            if (SystemMath.Abs(tmpDiff) < Tolerance) { tmpConverged = true; break; }

            var tmpVega = AnalyticVega(tmpS, tmpK, tmpT, tmpR, tmpQ, tmpSigma);
            if (tmpVega < 1e-10)
            {
                // Vega vanishes — deep ITM/OTM. NR can't make progress.
                // Bail to Brent.
                tmpConverged = false;
                break;
            }

            var tmpNext = tmpSigma - tmpDiff / tmpVega;
            // Clamp into the search range so we don't wander negative
            // or beyond Brent's upper bound during the iteration.
            if (tmpNext < BrentLowerBound) tmpNext = BrentLowerBound;
            else if (tmpNext > BrentUpperBound) tmpNext = BrentUpperBound;
            // No progress → bail.
            if (SystemMath.Abs(tmpNext - tmpSigma) < 1e-10) { tmpConverged = false; break; }
            tmpSigma = tmpNext;
        }

        if (!tmpConverged)
        {
            var tmpBrentSigma = BrentSolve(tmpS, tmpK, tmpT, tmpR, tmpQ, tmpMid, tmpIsCall, inCt);
            if (tmpBrentSigma is null) return null;
            tmpSigma = tmpBrentSigma.Value;
        }

        return BuildOutputs(tmpS, tmpK, tmpT, tmpR, tmpQ, tmpSigma, tmpIsCall);
    }

    /// <summary>
    /// Manaster-Koehler closed-form initial guess for IV, derived from
    /// the assumption that |ln(S/K) + (r-q)T| is small (i.e. near-ATM).
    /// Provides much faster NR convergence than a fixed 0.20 starting
    /// guess on liquid contracts. For deep ITM/OTM the formula degrades
    /// gracefully — clamp the result into the Brent bracket.
    /// </summary>
    internal static double ManasterKoehlerInitialGuess(
        double inS, double inK, double inR, double inQ, double inT)
    {
        // Manaster-Koehler: σ₀ = sqrt(2 * |ln(S/K) + (r-q)T| / T).
        // On exact ATM with r=q this collapses to 0; clamp to a sensible
        // lower bound so NR has somewhere to go.
        var tmpInner = SystemMath.Abs(SystemMath.Log(inS / inK) + (inR - inQ) * inT);
        var tmpGuess = SystemMath.Sqrt(2.0 * tmpInner / inT);
        if (tmpGuess < 0.05) tmpGuess = 0.20;
        if (tmpGuess > 2.0) tmpGuess = 1.0;
        return tmpGuess;
    }

    /// <summary>
    /// Brent's method — bracketed root finder over [a, b]. Given that the
    /// BS price is monotonically increasing in σ, a sign change between
    /// f(a) = BSPrice(a) - mid and f(b) = BSPrice(b) - mid guarantees a
    /// root in [a, b]. If both endpoints have the same sign we have no
    /// root in range and return null (mid is impossible to attain).
    /// </summary>
    internal static double? BrentSolve(
        double inS, double inK, double inT, double inR, double inQ,
        double inMid, bool inIsCall, CancellationToken inCt)
    {
        var tmpA = BrentLowerBound;
        var tmpB = BrentUpperBound;
        var tmpFa = BlackScholesPrice(inS, inK, inT, inR, inQ, tmpA, inIsCall) - inMid;
        var tmpFb = BlackScholesPrice(inS, inK, inT, inR, inQ, tmpB, inIsCall) - inMid;

        if (tmpFa * tmpFb > 0)
        {
            // No sign change → no root in bracket. Mid is unreachable.
            return null;
        }
        if (SystemMath.Abs(tmpFa) < Tolerance) return tmpA;
        if (SystemMath.Abs(tmpFb) < Tolerance) return tmpB;

        // Standard Brent — combination of bisection, secant, and inverse
        // quadratic interpolation. Adapted from Numerical Recipes 3e §9.3.
        if (SystemMath.Abs(tmpFa) < SystemMath.Abs(tmpFb))
        {
            (tmpA, tmpB) = (tmpB, tmpA);
            (tmpFa, tmpFb) = (tmpFb, tmpFa);
        }

        var tmpC = tmpA;
        var tmpFc = tmpFa;
        var tmpMflag = true;
        var tmpD = 0.0; // initialized after the first iteration

        for (var i = 0; i < BrentMaxIterations; i++)
        {
            inCt.ThrowIfCancellationRequested();
            if (SystemMath.Abs(tmpFb) < Tolerance) return tmpB;
            if (SystemMath.Abs(tmpA - tmpB) < 1e-12) return tmpB;

            double tmpSGuess;
            if (tmpFa != tmpFc && tmpFb != tmpFc)
            {
                // Inverse quadratic interpolation.
                tmpSGuess = tmpA * tmpFb * tmpFc / ((tmpFa - tmpFb) * (tmpFa - tmpFc))
                          + tmpB * tmpFa * tmpFc / ((tmpFb - tmpFa) * (tmpFb - tmpFc))
                          + tmpC * tmpFa * tmpFb / ((tmpFc - tmpFa) * (tmpFc - tmpFb));
            }
            else
            {
                // Secant.
                tmpSGuess = tmpB - tmpFb * (tmpB - tmpA) / (tmpFb - tmpFa);
            }

            // Conditions to fall back to bisection.
            var tmpUpper = (3 * tmpA + tmpB) / 4.0;
            var tmpCond1 = (tmpSGuess - tmpUpper) * (tmpSGuess - tmpB) > 0;
            var tmpCond2 = tmpMflag && SystemMath.Abs(tmpSGuess - tmpB) >= SystemMath.Abs(tmpB - tmpC) / 2.0;
            var tmpCond3 = !tmpMflag && SystemMath.Abs(tmpSGuess - tmpB) >= SystemMath.Abs(tmpC - tmpD) / 2.0;
            var tmpCond4 = tmpMflag && SystemMath.Abs(tmpB - tmpC) < 1e-12;
            var tmpCond5 = !tmpMflag && SystemMath.Abs(tmpC - tmpD) < 1e-12;
            if (tmpCond1 || tmpCond2 || tmpCond3 || tmpCond4 || tmpCond5)
            {
                tmpSGuess = (tmpA + tmpB) / 2.0;
                tmpMflag = true;
            }
            else
            {
                tmpMflag = false;
            }

            var tmpFs = BlackScholesPrice(inS, inK, inT, inR, inQ, tmpSGuess, inIsCall) - inMid;
            tmpD = tmpC;
            tmpC = tmpB;
            tmpFc = tmpFb;

            if (tmpFa * tmpFs < 0)
            {
                tmpB = tmpSGuess;
                tmpFb = tmpFs;
            }
            else
            {
                tmpA = tmpSGuess;
                tmpFa = tmpFs;
            }

            if (SystemMath.Abs(tmpFa) < SystemMath.Abs(tmpFb))
            {
                (tmpA, tmpB) = (tmpB, tmpA);
                (tmpFa, tmpFb) = (tmpFb, tmpFa);
            }
        }
        return null;
    }

    /// <summary>
    /// European Black-Scholes price for a call or put, with continuous
    /// dividend yield q. Internal so unit tests can pin the price math
    /// directly.
    /// </summary>
    internal static double BlackScholesPrice(
        double inS, double inK, double inT, double inR, double inQ, double inSigma, bool inIsCall)
    {
        if (inSigma <= 0)
        {
            // Zero-vol degenerate: option = max(forward intrinsic, 0).
            var tmpF0 = inS * SystemMath.Exp((inR - inQ) * inT);
            var tmpDisc0 = SystemMath.Exp(-inR * inT);
            return inIsCall
                ? tmpDisc0 * SystemMath.Max(tmpF0 - inK, 0)
                : tmpDisc0 * SystemMath.Max(inK - tmpF0, 0);
        }
        var tmpD1 = (SystemMath.Log(inS / inK) + (inR - inQ + 0.5 * inSigma * inSigma) * inT)
                  / (inSigma * SystemMath.Sqrt(inT));
        var tmpD2 = tmpD1 - inSigma * SystemMath.Sqrt(inT);
        var tmpDiscS = inS * SystemMath.Exp(-inQ * inT);
        var tmpDiscK = inK * SystemMath.Exp(-inR * inT);
        if (inIsCall)
        {
            return tmpDiscS * NormalCdf(tmpD1) - tmpDiscK * NormalCdf(tmpD2);
        }
        return tmpDiscK * NormalCdf(-tmpD2) - tmpDiscS * NormalCdf(-tmpD1);
    }

    /// <summary>Analytical vega — kept separate so NR can call without
    /// re-computing the price. Always &gt;= 0.</summary>
    internal static double AnalyticVega(
        double inS, double inK, double inT, double inR, double inQ, double inSigma)
    {
        if (inSigma <= 0 || inT <= 0) return 0;
        var tmpD1 = (SystemMath.Log(inS / inK) + (inR - inQ + 0.5 * inSigma * inSigma) * inT)
                  / (inSigma * SystemMath.Sqrt(inT));
        var tmpDiscS = inS * SystemMath.Exp(-inQ * inT);
        return tmpDiscS * NormalPdf(tmpD1) * SystemMath.Sqrt(inT);
    }

    /// <summary>Build the BlackScholesOutputs record at a converged σ.</summary>
    internal static BlackScholesOutputs BuildOutputs(
        double inS, double inK, double inT, double inR, double inQ, double inSigma, bool inIsCall)
    {
        var tmpSqrtT = SystemMath.Sqrt(inT);
        var tmpD1 = (SystemMath.Log(inS / inK) + (inR - inQ + 0.5 * inSigma * inSigma) * inT)
                  / (inSigma * tmpSqrtT);
        var tmpD2 = tmpD1 - inSigma * tmpSqrtT;
        var tmpDiscS = inS * SystemMath.Exp(-inQ * inT);
        var tmpDiscK = inK * SystemMath.Exp(-inR * inT);

        // Delta. Call: e^{-qT} N(d1). Put: -e^{-qT} N(-d1).
        var tmpDelta = inIsCall
            ? SystemMath.Exp(-inQ * inT) * NormalCdf(tmpD1)
            : -SystemMath.Exp(-inQ * inT) * NormalCdf(-tmpD1);

        // Gamma — same for call and put.
        var tmpGamma = (SystemMath.Exp(-inQ * inT) * NormalPdf(tmpD1)) / (inS * inSigma * tmpSqrtT);

        // Vega — same for call and put. Per unit of vol (i.e. per 1.0,
        // not per 1%).
        var tmpVega = tmpDiscS * NormalPdf(tmpD1) * tmpSqrtT;

        // Theta (per year). Both forms use the same first term. Sign:
        // negative for long calls/puts (price decays toward expiration)
        // when q ≈ 0 and r ≈ 0.
        var tmpTermA = -(tmpDiscS * NormalPdf(tmpD1) * inSigma) / (2.0 * tmpSqrtT);
        double tmpTheta;
        if (inIsCall)
        {
            tmpTheta = tmpTermA - inR * tmpDiscK * NormalCdf(tmpD2)
                       + inQ * tmpDiscS * NormalCdf(tmpD1);
        }
        else
        {
            tmpTheta = tmpTermA + inR * tmpDiscK * NormalCdf(-tmpD2)
                       - inQ * tmpDiscS * NormalCdf(-tmpD1);
        }

        return new BlackScholesOutputs(
            Iv: ToDecimal(inSigma),
            Delta: ToDecimal(tmpDelta),
            Gamma: ToDecimal(tmpGamma),
            Theta: ToDecimal(tmpTheta),
            Vega: ToDecimal(tmpVega));
    }

    /// <summary>
    /// Standard normal CDF. Uses Abramowitz &amp; Stegun 26.2.17 — accuracy
    /// ~1e-7 absolute, more than enough for IV solving (we converge to
    /// 1e-6 in price space, and ∂price/∂N is bounded).
    /// </summary>
    internal static double NormalCdf(double inX)
    {
        // erf-based form: N(x) = 0.5 * (1 + erf(x/sqrt(2))).
        return 0.5 * (1.0 + Erf(inX / SystemMath.Sqrt(2.0)));
    }

    /// <summary>Standard normal PDF — closed form.</summary>
    internal static double NormalPdf(double inX)
    {
        return SystemMath.Exp(-0.5 * inX * inX) / SystemMath.Sqrt(2.0 * SystemMath.PI);
    }

    /// <summary>
    /// Error function via Abramowitz &amp; Stegun 7.1.26 (max error ~1.5e-7).
    /// Postman validators reproduce the same accuracy. Sufficient for IV
    /// solving to 1e-6 in price space.
    /// </summary>
    internal static double Erf(double inX)
    {
        // Abramowitz & Stegun 7.1.26.
        const double a1 = 0.254829592;
        const double a2 = -0.284496736;
        const double a3 = 1.421413741;
        const double a4 = -1.453152027;
        const double a5 = 1.061405429;
        const double p = 0.3275911;

        var tmpSign = inX < 0 ? -1 : 1;
        var tmpAbs = SystemMath.Abs(inX);
        var tmpT = 1.0 / (1.0 + p * tmpAbs);
        var tmpY = 1.0 - (((((a5 * tmpT + a4) * tmpT) + a3) * tmpT + a2) * tmpT + a1) * tmpT
                          * SystemMath.Exp(-tmpAbs * tmpAbs);
        return tmpSign * tmpY;
    }

    /// <summary>Clamp a double to the decimal range and round to 6
    /// fractional digits — matches the persistence layer's
    /// <c>NUMERIC(10,6)</c> precision.</summary>
    private static decimal ToDecimal(double inValue)
    {
        if (double.IsNaN(inValue) || double.IsInfinity(inValue)) return 0m;
        // decimal range is roughly ±7.9e28; clamp to a safe range first.
        if (inValue > 1e15) inValue = 1e15;
        if (inValue < -1e15) inValue = -1e15;
        return SystemMath.Round((decimal)inValue, 6, MidpointRounding.AwayFromZero);
    }
}
