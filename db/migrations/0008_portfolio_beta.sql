-- ---------------------------------------------------------------------------
-- 0008_portfolio_beta.sql  —  Block 4b: portfolio BETA vs benchmark (read model)
-- ---------------------------------------------------------------------------
-- WHY
--   The sixth risk figure: Beta = Cov(r_portfolio, r_benchmark) / Var(r_benchmark),
--   one per (portfolio, mapped benchmark) pair, plus correlation and R^2. Beta
--   needs TWO aligned return streams, so it lives in its own function (multi-row
--   per portfolio) rather than inside get_portfolio_risk_metrics (single-row).
--
-- DESIGN DECISION — PURELY ADDITIVE / ZERO-TOUCH (same rule as 0006/0007)
--   Creates ONLY new objects:
--     * get_portfolio_beta(portfolio, from, to, min_obs)  — the calculator
--     * v_portfolio_beta                                  — convenience view
--   DROPs / ALTERs NOTHING. It only READS, never modifies:
--     - public.get_benchmark_comparison(text)  (Block 3, read-only reuse)
--     - public.portfolios
--   No CASCADE risk. 0006/0007 are untouched.
--
-- DATA BASE — reuse of the Block-3 comparison (FX / alignment / carry-forward
--             all already solved there; we do NOT rebuild any of it)
--   get_benchmark_comparison(portfolio) returns, per (benchmark_key, price_date),
--   BOTH curves on the SAME calendar:
--     - portfolio_twr_index : the portfolio TWR curve
--     - benchmark_index     : the benchmark curve, ALREADY FX-converted
--                             native->base PER price_date and carry-forward.
--   We derive daily returns from those two indices via lag(); rebasing to 100
--   cancels in the ratio, so the derived returns are the true daily returns and
--   the benchmark leg is the FX-converted BASE-CURRENCY return.
--
-- CURRENCY (base-currency beta — deliberate)
--   Both legs are in the portfolio's base_currency: the portfolio return is on
--   nav_base, the benchmark return carries the native->base FX per date. This is
--   the honest experience of a base-currency investor (FX applied consistently
--   to BOTH legs, no spurious cross-currency leak into the covariance). A
--   "local-currency beta" that strips FX from both sides is a different question
--   and can be added later without a redesign.
--
-- SKIP-SET CONSISTENCY — identical portfolio-day set to Risk (0007), for free
--   twr_index_t = 100*exp(SUM[ r>-1 ? ln(1+r) : 0 ]). Hence for r_t > -1 the
--   index-derived return twr_index_t/twr_index_{t-1}-1 == r_t EXACTLY, and for an
--   impossible r_t <= -1 the index stands still -> derived return = 0. So the
--   portfolio leg, filtered r_p <> 0, is bit-identical to Risk's
--   (daily_twr_return <> 0 AND > -1) WITHOUT re-writing the > -1 guard: the
--   impossible days are already flat -> dropped by <> 0. (Verification (5) proves
--   beta-days SUBSET risk-days.)
--
-- DAY-ACCURATE MATCHING (critical)
--   Cov/Var run ONLY over days where BOTH legs genuinely moved: r_p <> 0 AND
--   r_b <> 0 on the SAME grid row (the two indices share the row, so this IS the
--   inner join on date). A weekend crypto move has r_p <> 0 but r_b = 0 (the
--   equity benchmark carry-forwards Friday's close) -> the pair is dropped, never
--   mis-paired. A weekday where the portfolio NAV carried forward (r_p = 0) is
--   likewise dropped. Left over: only real common trading days.
--
-- NO ANNUALIZATION (unlike volatility)
--   Beta, correlation and R^2 are dimensionless and annualization-invariant: a
--   sqrt(252)/252 factor cancels in Cov/Var and in corr. So NO sqrt(252) here.
--
-- SUFFICIENCY (statistical honesty — no Scheinzahlen)
--   The threshold is on COMMON trading days PER benchmark: < p_min_obs (default
--   20) -> beta/correlation/r_squared NULL, status 'insufficient_data'.
--   common_data_points is ALWAYS returned. Every mapped benchmark present in the
--   comparison gets a row even at 0 common days (LEFT JOIN below).
--
-- NULL-SAFETY (never crash / never inf)
--   - < 2 common points   : regr_slope / corr / regr_r2 -> NULL (built-in).
--   - Var(r_benchmark) = 0 : regr_slope -> NULL (benchmark flat over the window).
--   - lag() first row      : NULL return -> excluded by "IS NOT NULL AND <> 0".
--   - no mapped benchmark  : no rows (empty betas array) -> not an error.
--   - no price overlap     : benchmark absent from comparison -> no row (nothing
--                            to report); if present but 0 common -> n=0 row.
--
-- UNITS
--   beta / correlation / r_squared are dimensionless. correlation in [-1, 1],
--   r_squared in [0, 1]. beta unbounded.
--
-- IDEMPOTENCY
--   CREATE OR REPLACE only, on NEW names. Safe to re-run.
-- ---------------------------------------------------------------------------

BEGIN;

-- ---------------------------------------------------------------------
-- 1) get_portfolio_beta(portfolio, from, to, min_obs)
--    One row per mapped benchmark: beta, correlation, R^2 over the common
--    trading days in [from, to] (NULL,NULL = full overlap since the anchor).
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_portfolio_beta(
    p_portfolio text,
    p_from      date    DEFAULT NULL,
    p_to        date    DEFAULT NULL,
    p_min_obs   integer DEFAULT 20
)
RETURNS TABLE (
    benchmark_key       text,
    display_name        text,
    sort_order          integer,
    common_data_points  integer,
    status              text,
    beta                numeric,
    correlation         numeric,
    r_squared           numeric
)
LANGUAGE sql
STABLE
AS $$
    -- [reuse] Block-3 comparison: two curves already aligned on one calendar,
    -- benchmark leg FX-converted per price_date + carry-forward. Read-only.
    WITH cmp AS (
        SELECT c.benchmark_key, c.display_name, c.sort_order,
               c.price_date, c.portfolio_twr_index, c.benchmark_index
          FROM public.get_benchmark_comparison(p_portfolio) c
    ),
    -- Daily returns from the two indices (lag within each benchmark). Rebasing to
    -- 100 cancels in the ratio -> true daily returns; benchmark leg is the
    -- FX-converted base-currency return. Portfolio leg == raw daily_twr_return
    -- for r>-1 and == 0 for impossible r<=-1 (index flat) => same skip-set as Risk.
    rets AS (
        SELECT
            benchmark_key, display_name, sort_order, price_date,
            portfolio_twr_index
              / NULLIF(lag(portfolio_twr_index)
                       OVER (PARTITION BY benchmark_key ORDER BY price_date), 0)
              - 1                                              AS r_p,
            benchmark_index
              / NULLIF(lag(benchmark_index)
                       OVER (PARTITION BY benchmark_key ORDER BY price_date), 0)
              - 1                                              AS r_b
          FROM cmp
    ),
    -- Window first (returns precomputed, THEN date-filtered -> the boundary day
    -- keeps its cross-boundary return, identical to Risk's windowing), then
    -- day-accurate matching: keep only days where BOTH legs genuinely moved.
    common AS (
        SELECT benchmark_key, r_p, r_b
          FROM rets
         WHERE (p_from IS NULL OR price_date >= p_from)
           AND (p_to   IS NULL OR price_date <= p_to)
           AND r_p IS NOT NULL AND r_p <> 0
           AND r_b IS NOT NULL AND r_b <> 0
    ),
    -- The identity set of benchmarks (so a 0-common benchmark still emits a row).
    bench AS (
        SELECT DISTINCT benchmark_key, display_name, sort_order FROM cmp
    ),
    agg AS (
        SELECT
            benchmark_key,
            count(*)                        AS n,
            regr_slope(r_p, r_b)::numeric   AS beta,         -- Cov/Var, null-safe (<2 or Var=0 -> NULL). ::numeric: aggregates return double precision, round() needs numeric
            corr(r_p, r_b)::numeric         AS correlation,  -- in [-1, 1]
            regr_r2(r_p, r_b)::numeric      AS r_squared     -- in [0, 1]
          FROM common
         GROUP BY benchmark_key
    )
    SELECT
        b.benchmark_key,
        b.display_name,
        b.sort_order,
        COALESCE(a.n, 0)::integer AS common_data_points,
        CASE WHEN COALESCE(a.n, 0) < p_min_obs THEN 'insufficient_data' ELSE 'ok' END AS status,
        -- Below the threshold every figure is NULL (no Scheinzahlen).
        CASE WHEN COALESCE(a.n, 0) < p_min_obs THEN NULL ELSE round(a.beta,        6) END AS beta,
        CASE WHEN COALESCE(a.n, 0) < p_min_obs THEN NULL ELSE round(a.correlation, 6) END AS correlation,
        CASE WHEN COALESCE(a.n, 0) < p_min_obs THEN NULL ELSE round(a.r_squared,   6) END AS r_squared
    FROM bench b
    LEFT JOIN agg a ON a.benchmark_key = b.benchmark_key
    ORDER BY b.sort_order, b.benchmark_key;
$$;

COMMENT ON FUNCTION public.get_portfolio_beta(text, date, date, integer) IS
    'Beta = Cov(r_p, r_b)/Var(r_b) per mapped benchmark (+ correlation, R^2) over the common trading days in [p_from,p_to] (NULL,NULL = full overlap since the benchmark anchor). Reuses get_benchmark_comparison read-only, so the benchmark leg is FX-converted native->base per price_date and carry-forward; returns derived from the two indices via lag(). Base-currency beta. Portfolio leg matches Risk''s skip-set (r_p<>0 == daily_twr_return<>0 AND >-1). Day-accurate: only days where BOTH legs moved (r_p<>0 AND r_b<>0). Dimensionless -> NO annualization. < p_min_obs (default 20) COMMON days -> figures NULL, status insufficient_data; common_data_points always returned. Purely additive read-only.';

-- ---------------------------------------------------------------------
-- 2) v_portfolio_beta — convenience: every portfolio x every mapped benchmark,
--    defaults (full overlap, min_obs = 20).
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW public.v_portfolio_beta AS
    SELECT p.name AS portfolio_name, r.*
      FROM public.portfolios p
      CROSS JOIN LATERAL public.get_portfolio_beta(p.name) r;

COMMENT ON VIEW public.v_portfolio_beta IS
    'Convenience wrapper: get_portfolio_beta() with defaults (full overlap, min_obs=20) for every portfolio x mapped benchmark. Additive, read-only.';

COMMIT;

-- ---------------------------------------------------------------------------
-- POST-APPLY VERIFICATION (run manually; not part of the transaction)
-- ---------------------------------------------------------------------------
-- (0) ADDITIVITY / ZERO-TOUCH: BOTH the performance view AND the benchmark
--     comparison must be byte-identical before and after (0008 only CREATEs new
--     objects). Run BEFORE apply, then AFTER; both md5 must match.
--       -- performance fingerprint
--       SELECT md5(string_agg(portfolio_name || ':' || twr_percent, ',' ORDER BY portfolio_name))
--         FROM public.v_portfolio_performance_public;
--       -- benchmark_comparison fingerprint (all portfolios x benchmarks x days)
--       SELECT md5(string_agg(x, '|' ORDER BY x)) FROM (
--         SELECT p.name || '/' || c.benchmark_key || '/' || c.price_date || ':'
--                || c.portfolio_twr_index || '/' || c.benchmark_index AS x
--           FROM public.portfolios p
--           CROSS JOIN LATERAL public.get_benchmark_comparison(p.name) c
--       ) t;
--
-- (1) SUFFICIENCY on today's short test data: every mapped benchmark should be
--     'insufficient_data' with beta/correlation/r_squared NULL but a real
--     common_data_points count.
--       SELECT portfolio_name, benchmark_key, sort_order, common_data_points,
--              status, beta, correlation, r_squared
--         FROM public.v_portfolio_beta
--        ORDER BY portfolio_name, sort_order, benchmark_key;
--
-- (2) COMMON-DAY sanity: common_data_points must equal the number of days where
--     BOTH derived returns are non-zero. Hand-count for one portfolio.
--       WITH cmp AS (SELECT * FROM public.get_benchmark_comparison('Global')),
--       rets AS (
--         SELECT benchmark_key, price_date,
--                portfolio_twr_index / NULLIF(lag(portfolio_twr_index)
--                    OVER (PARTITION BY benchmark_key ORDER BY price_date),0) - 1 AS r_p,
--                benchmark_index     / NULLIF(lag(benchmark_index)
--                    OVER (PARTITION BY benchmark_key ORDER BY price_date),0) - 1 AS r_b
--           FROM cmp)
--       SELECT benchmark_key,
--              count(*) FILTER (WHERE r_p IS NOT NULL AND r_p <> 0
--                                 AND r_b IS NOT NULL AND r_b <> 0) AS common_days
--         FROM rets GROUP BY benchmark_key ORDER BY benchmark_key;
--       -- compare to common_data_points from (1).
--
-- (3) BETA cross-check (once >= 20 common days exist): regr_slope must equal the
--     hand covar_samp/var_samp for one pair.
--       WITH cmp AS (SELECT * FROM public.get_benchmark_comparison('Global')),
--       rets AS (
--         SELECT benchmark_key,
--                portfolio_twr_index / NULLIF(lag(portfolio_twr_index)
--                    OVER (PARTITION BY benchmark_key ORDER BY price_date),0) - 1 AS r_p,
--                benchmark_index     / NULLIF(lag(benchmark_index)
--                    OVER (PARTITION BY benchmark_key ORDER BY price_date),0) - 1 AS r_b
--           FROM cmp),
--       common AS (SELECT benchmark_key, r_p, r_b FROM rets
--                   WHERE r_p IS NOT NULL AND r_p <> 0 AND r_b IS NOT NULL AND r_b <> 0)
--       SELECT benchmark_key,
--              round(regr_slope(r_p, r_b), 6)                          AS beta_fn,
--              round(covar_samp(r_p, r_b) / NULLIF(var_samp(r_b),0),6) AS beta_hand
--         FROM common GROUP BY benchmark_key ORDER BY benchmark_key;
--       -- beta_fn must equal beta_hand (covar_samp/var_samp == covar_pop/var_pop).
--
-- (4) WEEKEND exclusion: for a crypto sleeve vs an equity benchmark the common
--     days must fall on WEEKDAYS only (dow 1..5); Sat/Sun (6/0) must be ~empty.
--       WITH cmp AS (SELECT * FROM public.get_benchmark_comparison('Global')),
--       rets AS (
--         SELECT benchmark_key, price_date,
--                portfolio_twr_index / NULLIF(lag(portfolio_twr_index)
--                    OVER (PARTITION BY benchmark_key ORDER BY price_date),0) - 1 AS r_p,
--                benchmark_index     / NULLIF(lag(benchmark_index)
--                    OVER (PARTITION BY benchmark_key ORDER BY price_date),0) - 1 AS r_b
--           FROM cmp)
--       SELECT benchmark_key, extract(dow FROM price_date) AS dow, count(*)
--         FROM rets
--        WHERE r_p IS NOT NULL AND r_p <> 0 AND r_b IS NOT NULL AND r_b <> 0
--        GROUP BY benchmark_key, dow ORDER BY benchmark_key, dow;
--
-- (5) SKIP-SET SUBSET: every date that contributes to beta's PORTFOLIO leg must
--     also be a Risk trading day. This query must return ZERO rows.
--       WITH cmp AS (SELECT * FROM public.get_benchmark_comparison('Global')),
--       rets AS (
--         SELECT price_date,
--                portfolio_twr_index / NULLIF(lag(portfolio_twr_index)
--                    OVER (PARTITION BY benchmark_key ORDER BY price_date),0) - 1 AS r_p
--           FROM cmp),
--       beta_days AS (SELECT DISTINCT price_date FROM rets
--                      WHERE r_p IS NOT NULL AND r_p <> 0),
--       risk_days AS (SELECT snapshot_date FROM public.v_portfolio_twr_series
--                      WHERE portfolio_name = 'Global'
--                        AND daily_twr_return IS NOT NULL
--                        AND daily_twr_return <> 0
--                        AND daily_twr_return > -1)
--       SELECT * FROM beta_days
--        WHERE price_date NOT IN (SELECT snapshot_date FROM risk_days);
--       -- expect 0 rows (beta portfolio-days SUBSET risk-days).
-- ---------------------------------------------------------------------------
