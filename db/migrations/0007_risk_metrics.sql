-- ---------------------------------------------------------------------------
-- 0007_risk_metrics.sql  —  Block 4: portfolio RISK METRICS (read model)
-- ---------------------------------------------------------------------------
-- WHY
--   Standard risk-adjusted metrics per portfolio: annualized volatility,
--   Sharpe, Max Drawdown, Sortino, Calmar. Same job as Block 3: a pure READ
--   MODEL stacked on top of the existing performance chain.
--
-- DESIGN DECISION — PURELY ADDITIVE / ZERO-TOUCH (same rule as 0006)
--   This migration creates ONLY new objects:
--     * get_portfolio_risk_metrics(...)  — the calculator (from/to window)
--     * v_portfolio_risk_metrics         — convenience view (all portfolios,
--                                          inception..latest, defaults)
--   It DROPs / ALTERs NOTHING. The daily-return series and the performance
--   view are only READ, never modified:
--     - public.v_portfolio_twr_series          (the daily-return + TWR source)
--     - public.v_portfolio_performance_public  (untouched)
--   No CASCADE risk to the dashboard/valuation chain.
--
-- DATA BASE (consistency principle)
--   Everything is derived from public.v_portfolio_twr_series — the SAME series
--   that already feeds Performance (TWR) and the Benchmark comparison. Its
--   column daily_twr_return IS the "daily returns" series (there is no view
--   literally named v_portfolio_daily_returns); twr_index is the cumulative
--   curve used for the drawdown.
--
-- ANNUALIZATION — sqrt(252) TRADING DAYS  (explicit below: * sqrt(252))
--   v_portfolio_twr_series is CALENDAR-daily with carry-forward: weekends and
--   holidays are real rows with daily_twr_return = 0. To honour sqrt(252) the
--   risk stats run on REAL trading days only = daily_twr_return <> 0. A crypto
--   sleeve that genuinely moves on a weekend has a non-zero return and is kept;
--   a carry-forward filler day is exactly 0 and is dropped. (A genuine 0.00%
--   trading day would also be dropped — statistically immaterial.)
--
-- SKIP-SET CONSISTENCY — identical day-set to Performance / Benchmark / Index
--   The trading filter also drops any daily_twr_return <= -1 (an economically
--   IMPOSSIBLE <= -100% day, e.g. a cashflow >= NAV). This mirrors the twr_index
--   compounding guard EXACTLY (0006: WHEN daily_twr_return > -1 ... ELSE skip)
--   and the performance view's WHERE (0001: daily_twr_return > -1), so Risk
--   measures the SAME days the TWR curve and the Performance view measure — no
--   more, no less. Deliberately NO upper bound: the index caps nothing on the
--   high side (ln(1+r) is finite for ALL r > -1), so an absurdly-high mis-booked
--   return would flow into the index/performance too; capping it only in Risk
--   would break the shared-series guarantee. Outlier capping, if ever wanted,
--   belongs at the SERIES level (index+performance+risk together), never here.
--
-- RISK-FREE RATE
--   p_risk_free_annual is a PERCENT, DEFAULT 0. With rf = 0 the Sharpe/Sortino
--   are "excess over ZERO", NOT over a real risk-free rate. Pass a real annual
--   percent (e.g. 2.0) later for a proper risk-adjusted figure — no redesign.
--
-- WINDOW
--   p_from / p_to are the general case. NULL,NULL = inception..latest ("since
--   inception"). Rolling windows drop in later as p_from = current_date - N,
--   SAME function, no rebuild.
--
-- SUFFICIENCY  (statistical honesty — no Scheinzahlen)
--   < p_min_obs (default 20) trading days -> ALL metrics NULL, status
--   'insufficient_data'. 20-39 -> 'indicative'. 40+ -> 'robust'. data_points is
--   ALWAYS returned so the caller can judge maturity without a hidden number.
--
-- NULL-SAFETY (every degenerate input yields NULL, never a crash / inf)
--   - 0 trading days    : 252.0/NULLIF(n,0) -> NULL -> ann_return NULL; all NULL.
--   - 1 trading day     : stddev_samp -> NULL (n-1 denom); also < threshold.
--   - vol = 0           : sharpe = x / NULLIF(vol,0) -> NULL.
--   - no negative days  : downside_dev = 0 -> sortino = x / NULLIF(0,0) -> NULL.
--   - no drawdown (0)   : calmar = x / NULLIF(abs(0),0) -> NULL.
--   - empty window      : index boundaries NULL -> ann_return NULL; one all-NULL
--                         row still returned (status insufficient_data).
--
-- UNITS
--   *_percent columns are percentages (e.g. 12.27 = 12.27%). max_drawdown is a
--   NEGATIVE percent (a loss, e.g. -12.5). sharpe/sortino/calmar are
--   dimensionless ratios (the 100-factors cancel: (ret% - rf%)/vol%).
--
-- IDEMPOTENCY
--   CREATE OR REPLACE only, on NEW names. Safe to re-run.
-- ---------------------------------------------------------------------------

BEGIN;

-- ---------------------------------------------------------------------
-- 1) get_portfolio_risk_metrics(portfolio, from, to, rf%, min_obs)
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_portfolio_risk_metrics(
    p_portfolio         text,
    p_from              date    DEFAULT NULL,
    p_to                date    DEFAULT NULL,
    p_risk_free_annual  numeric DEFAULT 0,      -- PERCENT (0 = 0%, 2.0 = 2%)
    p_min_obs           integer DEFAULT 20
)
RETURNS TABLE (
    portfolio_name                 text,
    base_currency                  text,
    period_start                   date,
    period_end                     date,
    data_points                    integer,
    status                         text,
    confidence                     text,
    risk_free_rate_percent         numeric,
    annualized_return_percent      numeric,
    annualized_volatility_percent  numeric,
    max_drawdown_percent           numeric,
    sharpe_ratio                   numeric,
    sortino_ratio                  numeric,
    calmar_ratio                   numeric,
    downside_deviation_percent     numeric
)
LANGUAGE sql
STABLE
AS $$
    WITH bounds AS (
        SELECT p.id AS portfolio_id,
               p.name AS portfolio_name,
               p.base_currency,
               COALESCE(p_from, (SELECT min(s.snapshot_date)
                                   FROM public.v_portfolio_twr_series s
                                  WHERE s.portfolio_id = p.id)) AS eff_from,
               COALESCE(p_to,   (SELECT max(s.snapshot_date)
                                   FROM public.v_portfolio_twr_series s
                                  WHERE s.portfolio_id = p.id)) AS eff_to
          FROM public.portfolios p
         WHERE p.name = p_portfolio
    ),
    -- Full calendar rows inside the window (carry-forward days INCLUDED):
    -- used for the index boundaries and the drawdown curve.
    windowed AS (
        SELECT s.snapshot_date, s.daily_twr_return, s.twr_index
          FROM public.v_portfolio_twr_series s
          JOIN bounds b ON b.portfolio_id = s.portfolio_id
         WHERE s.snapshot_date >= b.eff_from
           AND s.snapshot_date <= b.eff_to
    ),
    -- REAL trading days only (drops carry-forward zero-return filler) so that
    -- sqrt(252) is the correct annualization factor. The `> -1` guard mirrors
    -- the twr_index compounding (0006) and the performance view (0001) EXACTLY:
    -- an impossible <= -100% day (a data artifact, e.g. a cashflow >= NAV) is
    -- skipped by the index and MUST be skipped here too, so Risk consumes the
    -- identical day-set as Performance/Benchmark. No UPPER bound (see header):
    -- the shared series caps nothing on the high side, so neither do we.
    trading AS (
        SELECT snapshot_date, daily_twr_return
          FROM windowed
         WHERE daily_twr_return IS NOT NULL
           AND daily_twr_return <> 0
           AND daily_twr_return > -1        -- skip-set identical to twr_index / performance
    ),
    -- Running peak -> per-day drawdown on the cumulative index (peak-to-trough,
    -- NOT the largest single-day loss). Weekend flat rows do not move the peak.
    dd AS (
        SELECT twr_index,
               max(twr_index) OVER (ORDER BY snapshot_date
                                    ROWS UNBOUNDED PRECEDING) AS peak
          FROM windowed
    ),
    agg AS (
        SELECT
            (SELECT count(*)            FROM trading)  AS n,
            (SELECT min(snapshot_date)  FROM windowed) AS pstart,
            (SELECT max(snapshot_date)  FROM windowed) AS pend,
            -- index at first / last day of the window (geometric CAGR base)
            (SELECT (array_agg(twr_index ORDER BY snapshot_date))[1]
               FROM windowed)                          AS i_from,
            (SELECT (array_agg(twr_index ORDER BY snapshot_date DESC))[1]
               FROM windowed)                          AS i_to,
            -- daily dispersion over trading days (sample stddev; NULL for n<2)
            (SELECT stddev_samp(daily_twr_return) FROM trading) AS vol_daily,
            -- downside deviation, MAR = 0, averaged over ALL trading days
            -- (positives contribute 0). NULLIF guards the empty-set /0.
            (SELECT sqrt( COALESCE(sum(CASE WHEN daily_twr_return < 0
                                            THEN daily_twr_return * daily_twr_return
                                            ELSE 0 END), 0)
                          / NULLIF((SELECT count(*) FROM trading), 0) )
               FROM trading)                           AS dnside_daily,
            -- worst peak-to-trough (fraction, <= 0); 0 if monotonic up
            (SELECT min(twr_index / NULLIF(peak, 0) - 1) FROM dd) AS mdd_frac
    ),
    calc AS (
        SELECT
            b.portfolio_name,
            b.base_currency,
            a.pstart, a.pend,
            COALESCE(a.n, 0) AS n,
            -- annualized return, geometric (percent); /0 and /NULL guarded
            ( power( a.i_to / NULLIF(a.i_from, 0), 252.0 / NULLIF(a.n, 0) ) - 1 ) * 100
                AS ann_ret_pct,
            a.vol_daily    * sqrt(252::numeric) * 100 AS ann_vol_pct,      -- sqrt(252)
            a.dnside_daily * sqrt(252::numeric) * 100 AS ann_dnside_pct,   -- sqrt(252)
            a.mdd_frac * 100 AS mdd_pct
          FROM agg a
          CROSS JOIN bounds b
    )
    SELECT
        c.portfolio_name,
        c.base_currency,
        c.pstart AS period_start,
        c.pend   AS period_end,
        c.n::integer AS data_points,
        CASE WHEN c.n < p_min_obs THEN 'insufficient_data' ELSE 'ok' END AS status,
        CASE WHEN c.n < p_min_obs THEN NULL
             WHEN c.n < 40        THEN 'indicative'
             ELSE                      'robust' END AS confidence,
        p_risk_free_annual AS risk_free_rate_percent,
        -- Below the threshold EVERY metric is NULL (no Scheinzahlen).
        CASE WHEN c.n < p_min_obs THEN NULL ELSE round(c.ann_ret_pct,    6) END AS annualized_return_percent,
        CASE WHEN c.n < p_min_obs THEN NULL ELSE round(c.ann_vol_pct,    6) END AS annualized_volatility_percent,
        CASE WHEN c.n < p_min_obs THEN NULL ELSE round(c.mdd_pct,        6) END AS max_drawdown_percent,
        CASE WHEN c.n < p_min_obs THEN NULL
             ELSE round( (c.ann_ret_pct - p_risk_free_annual) / NULLIF(c.ann_vol_pct, 0), 6) END AS sharpe_ratio,
        CASE WHEN c.n < p_min_obs THEN NULL
             ELSE round( (c.ann_ret_pct - p_risk_free_annual) / NULLIF(c.ann_dnside_pct, 0), 6) END AS sortino_ratio,
        CASE WHEN c.n < p_min_obs THEN NULL
             ELSE round( c.ann_ret_pct / NULLIF(abs(c.mdd_pct), 0), 6) END AS calmar_ratio,
        CASE WHEN c.n < p_min_obs THEN NULL ELSE round(c.ann_dnside_pct, 6) END AS downside_deviation_percent
    FROM calc c;
$$;

COMMENT ON FUNCTION public.get_portfolio_risk_metrics(text, date, date, numeric, integer) IS
    'Portfolio risk metrics (annualized vol, Sharpe, Max Drawdown, Sortino, Calmar) over [p_from,p_to] (NULL,NULL = inception..latest). Annualized sqrt(252) over REAL trading days (daily_twr_return<>0 on the calendar-daily carry-forward series v_portfolio_twr_series). rf in PERCENT, default 0 = excess-over-zero. < p_min_obs (default 20) trading days -> all metrics NULL, status insufficient_data; 20-39 indicative, 40+ robust. *_percent are percentages, max_drawdown negative; ratios dimensionless. Purely additive read-only; touches nothing.';

-- ---------------------------------------------------------------------
-- 2) v_portfolio_risk_metrics — convenience: every portfolio, defaults
--    (inception..latest, rf = 0, min_obs = 20).
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW public.v_portfolio_risk_metrics AS
    SELECT r.*
      FROM public.portfolios p
      CROSS JOIN LATERAL public.get_portfolio_risk_metrics(p.name) r;

COMMENT ON VIEW public.v_portfolio_risk_metrics IS
    'Convenience wrapper: get_portfolio_risk_metrics() with defaults (inception..latest, rf=0, min_obs=20) for every portfolio. Additive, read-only.';

COMMIT;

-- ---------------------------------------------------------------------------
-- POST-APPLY VERIFICATION (run manually; not part of the transaction)
-- ---------------------------------------------------------------------------
-- (0) ADDITIVITY / ZERO-TOUCH: the performance view must be byte-identical
--     before and after. Run BEFORE apply, then AFTER; md5 must match.
--       SELECT md5(string_agg(portfolio_name || ':' || twr_percent, ',' ORDER BY portfolio_name))
--         FROM public.v_portfolio_performance_public;
--
-- (1) SUFFICIENCY on today's short test data: every portfolio should be
--     'insufficient_data' with all metrics NULL but a real data_points count.
--       SELECT portfolio_name, data_points, status, confidence,
--              annualized_volatility_percent, sharpe_ratio, max_drawdown_percent
--         FROM public.v_portfolio_risk_metrics
--        ORDER BY portfolio_name;
--
-- (2) TRADING-DAY count sanity: data_points must equal the number of NON-zero
--     daily returns since inception (i.e. weekends/holidays excluded).
--       SELECT s.portfolio_name,
--              count(*) FILTER (WHERE s.daily_twr_return IS NOT NULL
--                                 AND s.daily_twr_return <> 0) AS trading_days
--         FROM public.v_portfolio_twr_series s
--        GROUP BY s.portfolio_name
--        ORDER BY s.portfolio_name;
--       -- compare to data_points from (1).
--
-- (3) ANNUALIZATION cross-check (once >= 20 trading days exist): hand-compute
--     annualized_volatility = daily_stddev * sqrt(252) * 100 for one portfolio
--     and confirm it equals the function output.
--       WITH t AS (SELECT daily_twr_return r FROM public.v_portfolio_twr_series
--                   WHERE portfolio_name = 'Global'
--                     AND daily_twr_return IS NOT NULL AND daily_twr_return <> 0)
--       SELECT round(stddev_samp(r) * sqrt(252) * 100, 6) AS hand_vol_pct FROM t;
--
-- (4) MAX DRAWDOWN sanity: the reported value must equal the worst peak-to-
--     trough of twr_index (negative). Spot-check against the raw curve.
--       WITH d AS (SELECT snapshot_date, twr_index,
--                         max(twr_index) OVER (ORDER BY snapshot_date
--                                              ROWS UNBOUNDED PRECEDING) peak
--                    FROM public.v_portfolio_twr_series
--                   WHERE portfolio_name = 'Global')
--       SELECT round(min(twr_index/peak - 1) * 100, 6) AS hand_mdd_pct FROM d;
--
-- (5) WINDOW parameter: a narrower from-date must reduce data_points.
--       SELECT data_points FROM public.get_portfolio_risk_metrics('Global');            -- inception
--       SELECT data_points FROM public.get_portfolio_risk_metrics('Global', CURRENT_DATE - 10); -- last 10d
--
-- (6) NULL-SAFETY smoke: no row should ever error or show 'Infinity'/'NaN'.
--       SELECT * FROM public.v_portfolio_risk_metrics;   -- must return cleanly
-- ---------------------------------------------------------------------------
