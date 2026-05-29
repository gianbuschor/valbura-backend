-- =====================================================================
-- Migration 0001 — XIRR (money-weighted return) function +
--                   additive MWR columns on v_portfolio_performance_public
--
-- Depends on baseline (db/schema.sql).
-- Idempotent: CREATE OR REPLACE for both function and view; safe to
-- re-apply.  View change is additive only — existing 11 columns and
-- their semantics are preserved verbatim; 2 new trailing columns are
-- appended (mwr_annualized_percent, mwr_period_percent).
-- =====================================================================

SET search_path TO public, extensions;


-- ---------------------------------------------------------------------
-- 1) public.xirr(amounts numeric[], dates date[]) -> numeric
--    Annualized money-weighted return IN PERCENT.
--    Bisection over r in [-0.9999, +100]; ACT/365 day-count.
--    Returns NULL on degenerate inputs (<2 cashflows, no sign change,
--    zero time span, no NPV sign change, non-convergence).
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.xirr(amounts numeric[], dates date[])
 RETURNS numeric
 LANGUAGE plpgsql
 IMMUTABLE
AS $function$
-- =====================================================================
-- xirr(amounts, dates)  -> annualized money-weighted return IN PERCENT
--
-- Solves for r in:  NPV(r) = SUM( amount_i / (1+r)^((date_i - d0)/365) ) = 0
-- where d0 is the earliest date.  Uses ACT/365 day-count (Excel XIRR style).
--
-- Method: BISECTION over the rate bracket r in [-0.9999, +100]
--   (guaranteed convergence once a sign change in NPV is bracketed;
--    no derivative needed, cannot diverge unlike Newton-Raphson).
--
-- Returns the annualized rate as a PERCENT (e.g. -2.8680 for -2.868%/yr).
--
-- Returns NULL when XIRR is not computable:
--   * fewer than 2 cashflows / array length mismatch / NULL elements
--   * no sign change in amounts (no root exists)
--   * zero time span (all dates equal)
--   * NPV does not change sign across the bracket (root outside bracket)
--   * solver fails to converge within the iteration limit
-- =====================================================================
DECLARE
    n         int;
    d0        date;
    has_pos   boolean := false;
    has_neg   boolean := false;
    min_d     date;
    max_d     date;
    lo        numeric := -0.9999;
    hi        numeric := 100.0;
    mid       numeric;
    f_lo      numeric;
    f_hi      numeric;
    f_mid     numeric;
    npv       numeric;
    i         int;
    iter      int := 0;
    max_iter  int := 1000;
    tol       numeric := 1e-10;
BEGIN
    -- Guard 1: >=2 cashflows, equal-length arrays
    n := array_length(amounts, 1);
    IF n IS NULL OR n < 2 THEN RETURN NULL; END IF;
    IF array_length(dates, 1) IS DISTINCT FROM n THEN RETURN NULL; END IF;

    -- Scan: detect sign change + date span; reject NULL elements
    min_d := dates[1];
    max_d := dates[1];
    FOR i IN 1..n LOOP
        IF amounts[i] IS NULL OR dates[i] IS NULL THEN RETURN NULL; END IF;
        IF amounts[i] > 0 THEN has_pos := true; END IF;
        IF amounts[i] < 0 THEN has_neg := true; END IF;
        IF dates[i] < min_d THEN min_d := dates[i]; END IF;
        IF dates[i] > max_d THEN max_d := dates[i]; END IF;
    END LOOP;

    -- Guard 2: need one sign change, else no root
    IF NOT (has_pos AND has_neg) THEN RETURN NULL; END IF;
    -- Guard 3: zero time span
    IF max_d = min_d THEN RETURN NULL; END IF;

    d0 := min_d;

    -- NPV at the two bracket ends
    npv := 0;
    FOR i IN 1..n LOOP
        npv := npv + amounts[i] / power(1 + lo, (dates[i] - d0) / 365.0);
    END LOOP;
    f_lo := npv;

    npv := 0;
    FOR i IN 1..n LOOP
        npv := npv + amounts[i] / power(1 + hi, (dates[i] - d0) / 365.0);
    END LOOP;
    f_hi := npv;

    -- Exact root at a bracket end?
    IF f_lo = 0 THEN RETURN lo * 100; END IF;
    IF f_hi = 0 THEN RETURN hi * 100; END IF;
    -- Guard 4: root must be bracketed
    IF sign(f_lo) = sign(f_hi) THEN RETURN NULL; END IF;

    -- Bisection loop
    LOOP
        iter := iter + 1;
        mid := (lo + hi) / 2.0;

        npv := 0;
        FOR i IN 1..n LOOP
            npv := npv + amounts[i] / power(1 + mid, (dates[i] - d0) / 365.0);
        END LOOP;
        f_mid := npv;

        IF (hi - lo) / 2.0 < tol THEN RETURN mid * 100; END IF;

        IF f_mid = 0 THEN
            RETURN mid * 100;
        ELSIF sign(f_mid) = sign(f_lo) THEN
            lo := mid;
            f_lo := f_mid;
        ELSE
            hi := mid;
            f_hi := f_mid;
        END IF;

        -- Guard 5: non-convergence
        IF iter >= max_iter THEN RETURN NULL; END IF;
    END LOOP;
END;
$function$;


-- ---------------------------------------------------------------------
-- 2) v_portfolio_performance_public — extended with two MWR columns.
--    Existing 11 columns kept byte-identical; mwr_annualized_percent
--    and mwr_period_percent appended at the end.
--    New CTEs (mwr_inputs, mwr_arrays, mwr_by_portfolio) are added;
--    existing CTEs are untouched.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW public.v_portfolio_performance_public AS
 WITH daily_nav AS (
         SELECT v_portfolio_daily_nav.portfolio_id,
            v_portfolio_daily_nav.portfolio_name,
            v_portfolio_daily_nav.base_currency,
            v_portfolio_daily_nav.snapshot_date,
            v_portfolio_daily_nav.nav_base,
            lag(v_portfolio_daily_nav.nav_base) OVER (PARTITION BY v_portfolio_daily_nav.portfolio_id ORDER BY v_portfolio_daily_nav.snapshot_date) AS prev_nav_base
           FROM v_portfolio_daily_nav
        ), daily_cashflows AS (
         SELECT portfolio_cashflows.portfolio_id,
            portfolio_cashflows.cashflow_date,
            sum(portfolio_cashflows.amount_base) AS cashflow_base
           FROM portfolio_cashflows
          GROUP BY portfolio_cashflows.portfolio_id, portfolio_cashflows.cashflow_date
        ), daily_returns AS (
         SELECT n_1.portfolio_id,
            n_1.portfolio_name,
            n_1.base_currency,
            n_1.snapshot_date,
            n_1.nav_base,
            n_1.prev_nav_base,
            COALESCE(c_1.cashflow_base, 0::numeric) AS cashflow_base,
                CASE
                    WHEN n_1.prev_nav_base IS NULL OR n_1.prev_nav_base = 0::numeric THEN NULL::numeric
                    ELSE (n_1.nav_base - COALESCE(c_1.cashflow_base, 0::numeric)) / n_1.prev_nav_base - 1::numeric
                END AS daily_twr_return
           FROM daily_nav n_1
             LEFT JOIN daily_cashflows c_1 ON c_1.portfolio_id = n_1.portfolio_id AND c_1.cashflow_date = n_1.snapshot_date
        ), twr_by_portfolio AS (
         SELECT daily_returns.portfolio_id,
            exp(sum(ln(1::numeric + daily_returns.daily_twr_return))) - 1::numeric AS twr_return
           FROM daily_returns
          WHERE daily_returns.daily_twr_return IS NOT NULL AND daily_returns.daily_twr_return > '-1'::integer::numeric
          GROUP BY daily_returns.portfolio_id
        ), latest_nav AS (
         SELECT DISTINCT ON (v_portfolio_daily_nav.portfolio_id) v_portfolio_daily_nav.portfolio_id,
            v_portfolio_daily_nav.portfolio_name,
            v_portfolio_daily_nav.base_currency,
            v_portfolio_daily_nav.snapshot_date AS latest_nav_date,
            v_portfolio_daily_nav.nav_base AS current_nav
           FROM v_portfolio_daily_nav
          ORDER BY v_portfolio_daily_nav.portfolio_id, v_portfolio_daily_nav.snapshot_date DESC
        ), cashflow_totals AS (
         SELECT portfolio_cashflows.portfolio_id,
            COALESCE(sum(portfolio_cashflows.amount_base), 0::numeric) AS net_contributions,
            COALESCE(sum(
                CASE
                    WHEN portfolio_cashflows.amount_base > 0::numeric THEN portfolio_cashflows.amount_base
                    ELSE 0::numeric
                END), 0::numeric) AS total_deposits,
            COALESCE(sum(
                CASE
                    WHEN portfolio_cashflows.amount_base < 0::numeric THEN abs(portfolio_cashflows.amount_base)
                    ELSE 0::numeric
                END), 0::numeric) AS total_withdrawals
           FROM portfolio_cashflows
          GROUP BY portfolio_cashflows.portfolio_id
        ), mwr_inputs AS (
         SELECT pc.portfolio_id,
            pc.cashflow_date AS flow_date,
            (- pc.amount_base) AS flow_amount
           FROM portfolio_cashflows pc
          WHERE pc.amount_base IS NOT NULL
        UNION ALL
         SELECT ln.portfolio_id,
            ln.latest_nav_date AS flow_date,
            ln.current_nav AS flow_amount
           FROM latest_nav ln
        ), mwr_arrays AS (
         SELECT mwr_inputs.portfolio_id,
            array_agg(mwr_inputs.flow_amount ORDER BY mwr_inputs.flow_date, mwr_inputs.flow_amount) AS amounts,
            array_agg(mwr_inputs.flow_date ORDER BY mwr_inputs.flow_date, mwr_inputs.flow_amount) AS dates,
            min(mwr_inputs.flow_date) AS first_flow_date,
            max(mwr_inputs.flow_date) AS last_flow_date
           FROM mwr_inputs
          GROUP BY mwr_inputs.portfolio_id
        ), mwr_by_portfolio AS (
         SELECT mwr_arrays.portfolio_id,
            public.xirr(mwr_arrays.amounts, mwr_arrays.dates) AS mwr_annualized_percent,
            mwr_arrays.first_flow_date,
            mwr_arrays.last_flow_date
           FROM mwr_arrays
        )
 SELECT n.portfolio_name,
    n.base_currency,
    n.latest_nav_date,
    n.current_nav,
    COALESCE(c.net_contributions, 0::numeric) AS net_contributions,
    n.current_nav - COALESCE(c.net_contributions, 0::numeric) AS net_profit,
        CASE
            WHEN COALESCE(c.total_deposits, 0::numeric) <= 0::numeric THEN NULL::numeric
            ELSE (n.current_nav - COALESCE(c.net_contributions, 0::numeric)) / c.total_deposits * 100::numeric
        END AS simple_return_percent,
    t.twr_return * 100::numeric AS twr_percent,
    COALESCE(c.total_deposits, 0::numeric) AS total_deposits,
    COALESCE(c.total_withdrawals, 0::numeric) AS total_withdrawals,
    n.current_nav - COALESCE(c.net_contributions, 0::numeric) AS net_profit_after_flows,
    m.mwr_annualized_percent,
        CASE
            WHEN m.mwr_annualized_percent IS NULL OR m.last_flow_date <= m.first_flow_date THEN NULL::numeric
            ELSE (power(1::numeric + m.mwr_annualized_percent / 100::numeric, ((m.last_flow_date - m.first_flow_date)::numeric / 365.0)) - 1::numeric) * 100::numeric
        END AS mwr_period_percent
   FROM latest_nav n
     LEFT JOIN cashflow_totals c ON c.portfolio_id = n.portfolio_id
     LEFT JOIN twr_by_portfolio t ON t.portfolio_id = n.portfolio_id
     LEFT JOIN mwr_by_portfolio m ON m.portfolio_id = n.portfolio_id;

-- End of migration 0001.
