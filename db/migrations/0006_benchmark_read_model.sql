-- =====================================================================
-- Migration 0006 — Benchmark READ MODEL (Block 3, display layer)
--                   PURELY ADDITIVE: creates only NEW objects.
--
-- Depends on baseline (db/schema.sql), 0001, 0002, 0003, 0004, 0005.
-- Apply order: db/schema.sql -> 0001 -> 0002 -> 0003 -> 0004 -> 0005 -> 0006.
--
-- WHY THIS MIGRATION EXISTS
--   0005 created the DATA layer (benchmarks / portfolio_benchmarks /
--   benchmark_prices) and the sync fills benchmark_prices with NATIVE
--   (USD) EOD closes. This migration adds the READ layer that turns that
--   raw data into two comparable, rebased-to-100 index curves per
--   portfolio (own TWR vs. each mapped benchmark) plus a scalar summary.
--   It creates THREE new objects and touches NOTHING that already exists:
--     * v_portfolio_twr_series      — the portfolio's own TWR curve rebased
--                                     to 100 (running geometric product).
--     * get_benchmark_comparison()  — per mapped benchmark: FX native->base
--                                     per price_date, weekend/holiday
--                                     carry-forward, common anchor S, BOTH
--                                     curves rebased to 100 on the SAME day.
--     * get_benchmark_summary()     — portfolio_twr / benchmark_return /
--                                     excess (alpha) per benchmark.
--
-- DESIGN DECISION — PURELY ADDITIVE, ZERO-TOUCH ON THE PERFORMANCE CHAIN
--   An earlier draft extracted a shared base view (v_portfolio_daily_returns)
--   and refactored v_portfolio_performance_public onto it (structural
--   single-source-of-truth). That works, but it REDEFINES the live
--   performance view that feeds the dashboard. To keep that chain
--   completely untouched (same principle as the Block-2 display wrappers:
--   never modify a base view, only stack new objects on top), this final
--   version instead derives the per-day TWR return INLINE inside
--   v_portfolio_twr_series, straight from the SAME source tables the
--   performance view uses (v_portfolio_daily_nav + portfolio_cashflows)
--   with the IDENTICAL formula. Consequences:
--     - v_portfolio_performance_public is NOT redefined -> twr_percent /
--       mwr_* cannot change -> before/after verification is trivial.
--     - No v_portfolio_daily_returns view is created -> nothing existing is
--       dropped, replaced, or CASCADE-affected.
--     - Consistency between v_portfolio_twr_series.twr_index (last day) and
--       v_portfolio_performance_public.twr_percent is EMPIRICAL: the return
--       formula is copied verbatim and PROVEN equal by the footer queries
--       (1) and (2), not guaranteed by a shared definition. Accepted on
--       purpose in exchange for not touching the performance chain.
--
-- ---------------------------------------------------------------------
-- FOUR CRITICAL CHECKPOINTS (all visible inline below, marked [a]..[d])
--
--   [a] FX PER price_date, NOT SPOT.
--       benchmark close (native USD) -> portfolio base is converted with
--       the SAME LATERAL fx_rates pattern the NAV path uses
--       (v_portfolio_nav_snapshots_base): fx_before = newest rate with
--       rate_date <= the curve day, then fx_latest as a last-resort
--       fallback, then identity if native == base. We never multiply the
--       whole history by one spot rate.
--
--   [b] CARRY-FORWARD over weekend/holiday gaps.
--       BTC trades 7 days/week (has Saturdays/Sundays); the ETFs/indices
--       (DBC, GUNR, URTH, SP500, DJIA) do not. Both curves are evaluated
--       on the SAME portfolio NAV calendar (one row per calendar day). For
--       any day with no exact benchmark row we take the last close <= day
--       (LATERAL ORDER BY price_date DESC LIMIT 1). So a Saturday where DBC
--       has no print reuses Friday's close; no NULL holes, no gaps.
--
--   [c] COMMON ANCHOR S on the SAME day for BOTH curves.
--       S = MAX(first portfolio NAV date, first benchmark price date), per
--       benchmark. We keep only days d >= S, then rebase BOTH curves by
--       their value at S using first_value() over the same window. The
--       benchmark's value at S itself uses the carry-forward rule ([b]):
--       if a benchmark has no exact close on S (S is a weekend/holiday),
--       the last close <= S is the denominator. => both curves = 100 on
--       exactly the same calendar day, no artificial alpha offset.
--
--   [d] TWR CONSISTENCY — empirical, proven by the footer queries.
--       v_portfolio_twr_series derives daily_twr_return inline with the
--       EXACT formula v_portfolio_performance_public uses:
--         (nav_base - COALESCE(cashflow_base,0)) / prev_nav_base - 1
--       (skip rows where prev NULL/0 or return <= -1). The running product
--       100*exp(SUM(ln(1+r))) therefore lands on the same aggregate TWR the
--       performance view reports at the last date. Footer queries (1)/(2)
--       verify this at early/mid/end and at the endpoint.
--
-- IDEMPOTENCY
--   CREATE OR REPLACE VIEW / FUNCTION only, all on NEW names. Safe to
--   re-run. No data writes, no DDL on existing tables/views. schema.sql
--   and the performance chain stay untouched.
-- =====================================================================

BEGIN;

-- ---------------------------------------------------------------------
-- 1) v_portfolio_twr_series — portfolio's own TWR curve, rebased to 100
--
--    [d] Daily returns are derived INLINE from the same source tables the
--    performance view uses (v_portfolio_daily_nav + portfolio_cashflows),
--    with the identical cashflow-neutral formula. No base view is touched
--    or created. At the FIRST NAV date daily_twr_return is NULL ->
--    contribution ln(1)=0 -> exp(0)=1 -> index = 100. The CASE skip-set
--    (NULL / <= -1) matches the performance view's WHERE filter, so at the
--    LAST date the running SUM(ln(1+r)) equals that aggregate and
--    twr_index_last = 100*(1 + twr_percent/100).
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW public.v_portfolio_twr_series AS
 WITH daily_nav AS (
         SELECT v.portfolio_id,
            v.portfolio_name,
            v.base_currency,
            v.snapshot_date,
            v.nav_base,
            lag(v.nav_base) OVER (PARTITION BY v.portfolio_id ORDER BY v.snapshot_date) AS prev_nav_base
           FROM v_portfolio_daily_nav v
        ), daily_cashflows AS (
         SELECT c.portfolio_id,
            c.cashflow_date,
            sum(c.amount_base) AS cashflow_base
           FROM portfolio_cashflows c
          GROUP BY c.portfolio_id, c.cashflow_date
        ), daily_returns AS (
         SELECT n.portfolio_id,
            n.portfolio_name,
            n.base_currency,
            n.snapshot_date,
            n.nav_base,
                CASE
                    WHEN n.prev_nav_base IS NULL OR n.prev_nav_base = 0::numeric THEN NULL::numeric
                    ELSE (n.nav_base - COALESCE(c.cashflow_base, 0::numeric)) / n.prev_nav_base - 1::numeric
                END AS daily_twr_return
           FROM daily_nav n
             LEFT JOIN daily_cashflows c ON c.portfolio_id = n.portfolio_id AND c.cashflow_date = n.snapshot_date
        )
 SELECT r.portfolio_id,
    r.portfolio_name,
    r.base_currency,
    r.snapshot_date,
    r.nav_base,
    r.daily_twr_return,
    round(100::numeric * exp(sum(
        CASE
            WHEN r.daily_twr_return IS NOT NULL AND r.daily_twr_return > '-1'::integer::numeric
                THEN ln(1::numeric + r.daily_twr_return)
            ELSE 0::numeric
        END) OVER (PARTITION BY r.portfolio_id ORDER BY r.snapshot_date ROWS UNBOUNDED PRECEDING)), 6) AS twr_index
   FROM daily_returns r;

COMMENT ON VIEW public.v_portfolio_twr_series IS
    'Portfolio TWR curve rebased to 100 at first NAV date. Daily returns derived inline from v_portfolio_daily_nav + portfolio_cashflows (same formula as v_portfolio_performance_public, which is left untouched); last twr_index equals v_portfolio_performance_public.twr_percent (verified empirically).';

-- ---------------------------------------------------------------------
-- 2) get_benchmark_comparison(p_portfolio) — two comparable curves
--
--    Returns, for the named portfolio, one row per (mapped benchmark, day)
--    from the common anchor S onward, with BOTH the portfolio TWR curve and
--    the benchmark curve rebased to 100 on that benchmark's S. STABLE /
--    read-only. All checkpoints [a][b][c] live in the body below.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_benchmark_comparison(p_portfolio text)
RETURNS TABLE (
    benchmark_key       text,
    display_name        text,
    sort_order          integer,
    price_date          date,
    portfolio_twr_index numeric,
    benchmark_index     numeric
)
LANGUAGE sql
STABLE
AS $$
    WITH pf AS (
        SELECT p.id AS portfolio_id, p.name AS portfolio_name, p.base_currency
          FROM public.portfolios p
         WHERE p.name = p_portfolio
    ),
    -- portfolio TWR curve (already rebased to 100 at first NAV date); this
    -- also IS the shared daily calendar (one row per calendar day).
    twr AS (
        SELECT s.snapshot_date, s.twr_index
          FROM public.v_portfolio_twr_series s
          JOIN pf ON pf.portfolio_id = s.portfolio_id
    ),
    first_nav AS (
        SELECT min(snapshot_date) AS first_nav_date FROM twr
    ),
    -- benchmarks mapped to this portfolio (both sides active)
    maps AS (
        SELECT b.benchmark_key, b.display_name, b.native_currency, pb.sort_order
          FROM public.portfolio_benchmarks pb
          JOIN public.benchmarks b ON b.benchmark_key = pb.benchmark_key
          JOIN pf ON pf.portfolio_id = pb.portfolio_id
         WHERE pb.active = true AND b.active = true
    ),
    -- first stored price per mapped benchmark (for the anchor)
    bench_first AS (
        SELECT bp.benchmark_key, min(bp.price_date) AS first_price_date
          FROM public.benchmark_prices bp
          JOIN maps m ON m.benchmark_key = bp.benchmark_key
         GROUP BY bp.benchmark_key
    ),
    -- [c] COMMON ANCHOR S (per benchmark) = MAX(first NAV date, first price date)
    anchor AS (
        SELECT m.benchmark_key, m.display_name, m.sort_order, m.native_currency,
               GREATEST((SELECT first_nav_date FROM first_nav), bf.first_price_date) AS s_date
          FROM maps m
          JOIN bench_first bf ON bf.benchmark_key = m.benchmark_key
    ),
    -- shared calendar: cross each benchmark with the portfolio NAV days,
    -- keep only d >= S so both curves start on the SAME day ([c]).
    grid AS (
        SELECT a.benchmark_key, a.display_name, a.sort_order, a.native_currency,
               t.snapshot_date AS d, t.twr_index
          FROM anchor a
          CROSS JOIN twr t
         WHERE t.snapshot_date >= a.s_date
    ),
    priced AS (
        SELECT g.benchmark_key, g.display_name, g.sort_order, g.d, g.twr_index,
               -- [b] CARRY-FORWARD: last close <= day bridges weekend/holiday
               --     gaps (e.g. DBC has no Saturday print -> reuse Friday).
               cf.close AS close_native,
               -- [a] FX PER price_date (NOT spot): fx_before = newest rate
               --     with rate_date <= the curve day; fx_latest fallback;
               --     identity if native == base. Same pattern as
               --     v_portfolio_nav_snapshots_base.
               COALESCE(fxb.rate, fxl.rate,
                        CASE WHEN g.native_currency = pf.base_currency THEN 1 ELSE NULL::numeric END) AS fx_rate
          FROM grid g
          CROSS JOIN pf
          LEFT JOIN LATERAL (                                   -- [b] carry-forward
                 SELECT bp.close
                   FROM public.benchmark_prices bp
                  WHERE bp.benchmark_key = g.benchmark_key
                    AND bp.price_date <= g.d
                  ORDER BY bp.price_date DESC
                  LIMIT 1
          ) cf ON true
          LEFT JOIN LATERAL (                                   -- [a] fx_before (per date)
                 SELECT fx.rate
                   FROM public.fx_rates fx
                  WHERE fx.from_currency = g.native_currency
                    AND fx.to_currency   = pf.base_currency
                    AND fx.rate_date    <= g.d
                  ORDER BY fx.rate_date DESC
                  LIMIT 1
          ) fxb ON true
          LEFT JOIN LATERAL (                                   -- [a] fx_latest (fallback only)
                 SELECT fx.rate
                   FROM public.fx_rates fx
                  WHERE fx.from_currency = g.native_currency
                    AND fx.to_currency   = pf.base_currency
                  ORDER BY fx.rate_date DESC
                  LIMIT 1
          ) fxl ON true
    ),
    based AS (
        SELECT p.benchmark_key, p.display_name, p.sort_order, p.d, p.twr_index,
               p.close_native * p.fx_rate AS close_base
          FROM priced p
    )
    -- [c] rebase BOTH curves to 100 by their value at S (first row of the
    --     window, which is exactly day S). first_value() over the same
    --     partition/order => identical anchor day for portfolio & benchmark.
    SELECT b.benchmark_key,
           b.display_name,
           b.sort_order,
           b.d AS price_date,
           round(100::numeric * b.twr_index
                 / first_value(b.twr_index) OVER w, 4) AS portfolio_twr_index,
           round(100::numeric * b.close_base
                 / first_value(b.close_base) OVER w, 4) AS benchmark_index
      FROM based b
    WINDOW w AS (PARTITION BY b.benchmark_key ORDER BY b.d ROWS UNBOUNDED PRECEDING)
    ORDER BY b.sort_order, b.benchmark_key, b.d;
$$;

COMMENT ON FUNCTION public.get_benchmark_comparison(text) IS
    'Per mapped benchmark: portfolio TWR curve vs benchmark curve, both rebased to 100 at the common anchor S=MAX(first NAV, first price). FX native->base per price_date (LATERAL fx_rates, not spot); weekend/holiday carry-forward.';

-- ---------------------------------------------------------------------
-- 3) get_benchmark_summary(p_portfolio) — scalar alpha per benchmark
--
--    Collapses get_benchmark_comparison to the last day per benchmark and
--    reports the total-period figures over [S, last]:
--      portfolio_twr_percent   = last portfolio_twr_index - 100
--      benchmark_return_percent= last benchmark_index     - 100
--      excess_return_percent   = portfolio - benchmark  (alpha)
--    Because both curves are 100 at S ([c]), the two percentages are
--    directly comparable and the excess is a clean start-aligned alpha.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_benchmark_summary(p_portfolio text)
RETURNS TABLE (
    benchmark_key            text,
    display_name             text,
    sort_order               integer,
    as_of_date               date,
    portfolio_twr_percent    numeric,
    benchmark_return_percent numeric,
    excess_return_percent    numeric
)
LANGUAGE sql
STABLE
AS $$
    WITH cmp AS (
        SELECT * FROM public.get_benchmark_comparison(p_portfolio)
    ),
    last_row AS (
        SELECT DISTINCT ON (c.benchmark_key)
               c.benchmark_key, c.display_name, c.sort_order,
               c.price_date, c.portfolio_twr_index, c.benchmark_index
          FROM cmp c
         ORDER BY c.benchmark_key, c.price_date DESC
    )
    SELECT lr.benchmark_key,
           lr.display_name,
           lr.sort_order,
           lr.price_date AS as_of_date,
           round(lr.portfolio_twr_index - 100::numeric, 4) AS portfolio_twr_percent,
           round(lr.benchmark_index     - 100::numeric, 4) AS benchmark_return_percent,
           round(lr.portfolio_twr_index - lr.benchmark_index, 4) AS excess_return_percent
      FROM last_row lr
     ORDER BY lr.sort_order, lr.benchmark_key;
$$;

COMMENT ON FUNCTION public.get_benchmark_summary(text) IS
    'Total-period TWR vs benchmark return and excess (alpha) per mapped benchmark over [S, last]. Both legs anchored at 100 on the same day S, so excess is start-aligned.';

COMMIT;

-- =====================================================================
-- POST-APPLY VERIFICATION (run manually, read-only; not part of the txn)
-- =====================================================================
--
-- (0) PERFORMANCE UNCHANGED: this migration does NOT touch
--     v_portfolio_performance_public, so this fingerprint must be BYTE
--     IDENTICAL before and after the apply. (Run it before too.)
-- SELECT md5(string_agg(
--          portfolio_name || '|' ||
--          round(twr_percent,6)::text || '|' ||
--          COALESCE(round(mwr_annualized_percent,6)::text,'NULL') || '|' ||
--          COALESCE(round(mwr_period_percent,6)::text,'NULL') || '|' ||
--          round(current_nav,6)::text,
--          ';' ORDER BY portfolio_name)) AS perf_fingerprint
--   FROM v_portfolio_performance_public;
--
-- (1) TWR CONSISTENCY at 3 points: new series vs. the source-of-truth
--     formula evaluated inline up to X. Expect diff ~ 0 at early/mid/end.
-- WITH ranked AS (
--     SELECT portfolio_id, portfolio_name, snapshot_date,
--            row_number() OVER (PARTITION BY portfolio_id ORDER BY snapshot_date) AS rn,
--            count(*)     OVER (PARTITION BY portfolio_id)                        AS n
--       FROM v_portfolio_twr_series
-- ),
-- sample AS (
--     SELECT portfolio_id, portfolio_name, snapshot_date,
--            CASE WHEN rn = GREATEST(2, n/4) THEN 'early'
--                 WHEN rn = GREATEST(2, n/2) THEN 'mid'
--                 WHEN rn = n                THEN 'end'  END AS point
--       FROM ranked
--      WHERE rn IN (GREATEST(2, n/4), GREATEST(2, n/2), n)
-- ),
-- series_at AS (
--     SELECT sm.portfolio_name, sm.point, sm.snapshot_date,
--            round(ts.twr_index - 100, 4) AS series_twr_pct
--       FROM sample sm
--       JOIN v_portfolio_twr_series ts
--         ON ts.portfolio_id = sm.portfolio_id AND ts.snapshot_date = sm.snapshot_date
-- ),
-- truth_at AS (
--     SELECT sm.portfolio_name, sm.point, sm.snapshot_date,
--            round(100 * (exp(sum(ln(1 + ts.daily_twr_return))) - 1), 4) AS truth_twr_pct
--       FROM sample sm
--       JOIN v_portfolio_twr_series ts
--         ON ts.portfolio_id = sm.portfolio_id
--        AND ts.snapshot_date <= sm.snapshot_date
--        AND ts.daily_twr_return IS NOT NULL AND ts.daily_twr_return > -1
--      GROUP BY sm.portfolio_name, sm.point, sm.snapshot_date
-- )
-- SELECT s.portfolio_name, s.point, s.snapshot_date,
--        s.series_twr_pct, t.truth_twr_pct,
--        round(s.series_twr_pct - t.truth_twr_pct, 6) AS diff
--   FROM series_at s JOIN truth_at t USING (portfolio_name, point, snapshot_date)
--  ORDER BY s.portfolio_name, s.snapshot_date;
--
-- (2) ENDPOINT REGRESSION: published view vs. new series (must be equal).
-- SELECT pp.portfolio_name,
--        round(pp.twr_percent, 4) AS published_twr_pct,
--        round((SELECT ts.twr_index - 100 FROM v_portfolio_twr_series ts
--                WHERE ts.portfolio_name = pp.portfolio_name
--                ORDER BY ts.snapshot_date DESC LIMIT 1), 4) AS series_last_pct
--   FROM v_portfolio_performance_public pp;
--
-- (3) Both curves start at exactly 100 on the same day S:
-- SELECT benchmark_key, min(price_date) AS s_day,
--        (array_agg(portfolio_twr_index ORDER BY price_date))[1] AS pf_at_s,
--        (array_agg(benchmark_index     ORDER BY price_date))[1] AS bm_at_s
--   FROM get_benchmark_comparison('Global')
--  GROUP BY benchmark_key;   -- expect pf_at_s = bm_at_s = 100.0000
--
-- (4) Scalar alpha:
-- SELECT * FROM get_benchmark_summary('Global');
-- SELECT * FROM get_benchmark_summary('Day Trading');
-- SELECT * FROM get_benchmark_summary('Alternatives');
