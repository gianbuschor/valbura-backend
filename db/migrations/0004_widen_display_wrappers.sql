-- =====================================================================
-- Migration 0004 — WIDEN the display wrapper functions (Block 2, Option B)
--
-- Depends on baseline (db/schema.sql), 0001, 0002 (fx_to_display), 0003.
-- Apply order: db/schema.sql -> 0001 -> 0002 -> 0003 -> 0004.
--
-- WHY THIS MIGRATION EXISTS
--   0003 shipped the display wrappers as NARROW projections (key columns +
--   the *_display money columns). When wiring the ?currency= endpoints
--   (step 4) that narrowing silently dropped columns the old endpoints used
--   to return: TP/SL on positions, the native/base value columns, the *_native
--   trade/fee columns, nav_currency on the summary, etc.
--   DECISION (Option B): widen every wrapper to carry ALL base-view columns
--   PLUS the display columns — additive, nothing dropped. This keeps the
--   "display = CHF is byte-identical to the base view" contract complete:
--   with CHF you now get the FULL base row back, plus display columns that
--   equal their base counterparts.
--
-- WHY DROP + CREATE (not CREATE OR REPLACE)
--   CREATE OR REPLACE FUNCTION cannot change a function's OUT columns
--   (the RETURNS TABLE signature). Widening adds columns, so Postgres would
--   reject a plain REPLACE with "cannot change return type of existing
--   function". Hence DROP FUNCTION first, then CREATE.
--
-- IS THE DROP SAFE?
--   Yes. The 0003 wrappers are still DEAD in production: the live main.py
--   queries the *_base views directly and calls NONE of these functions yet.
--   No view, function, or policy depends on them. Dropping and recreating
--   them therefore cannot affect any live query. (See the apply-ordering
--   note handed over with this migration: apply 0004 BEFORE deploying the
--   new main.py, so the widened wrappers already exist the instant the new
--   endpoints go live — no data-poverty window.)
--
-- WHAT IS PRESERVED FROM 0003 (unchanged semantics)
--   * The CHF-pivot: value_display = value_base (CHF) * fx_to_display(...).
--   * The date policy: latest NAV / positions / overview / allocation /
--     summary -> CURRENT_DATE; trades -> trade_timestamp::date; closed ->
--     last_event_time::date; cashflows -> cashflow_date; NAV series ->
--     per snapshot_date (Variante-B-safe).
--   * allocation_percent scale-invariant -> passed through UNCHANGED.
--   * quantity / price / avg_cost / market_price stay NATIVE.
--   * Rounding mirrors 0003 exactly (overview/allocation display rounded to
--     2dp; nav/positions/trades/closed/summary/cashflows raw) so the
--     verified byte-identical-at-CHF property is retained.
--
-- SUB-DECISIONS BAKED IN
--   A1: get_nav_display / get_nav_series_display do NOT expose
--       deposits_withdrawals — v_portfolio_nav_snapshots_base has no such
--       column (it lives only on v_nav_latest_public). Deposits/withdrawals
--       are served cleanly by /public/cashflows (get_cashflows_display).
--   B1: get_allocation_display keeps the generic group_by / group_value pair
--       AND additionally surfaces all three ORIGINAL grouping columns
--       (asset_class, broker, trade_currency). Each is context-dependently
--       NULL: only the column matching the active p_group_by is populated
--       (see per-branch comments below). No rename; purely additive.
--
-- IDEMPOTENT: DROP ... IF EXISTS + CREATE; safe to re-run. Wrapped in a
-- single transaction so the whole widening is all-or-nothing and no function
-- is ever observable in a dropped-but-not-recreated state.
-- =====================================================================

BEGIN;

SET search_path TO public, extensions;


-- ---------------------------------------------------------------------
-- 1) get_nav_display — latest NAV per broker, WIDENED.
--    Full v_portfolio_nav_snapshots_base row + display columns.
--    LIVE snapshot -> CURRENT_DATE rate. (A1: no deposits_withdrawals.)
-- ---------------------------------------------------------------------
DROP FUNCTION IF EXISTS public.get_nav_display(text);
CREATE FUNCTION public.get_nav_display(p_display text DEFAULT NULL::text)
 RETURNS TABLE(
    portfolio_name text,
    base_currency text,
    broker text,
    snapshot_date date,
    native_currency text,
    fx_rate_to_base numeric,
    nav_native numeric,
    nav_base numeric,
    cash_native numeric,
    cash_base numeric,
    market_value_native numeric,
    market_value_base numeric,
    open_pnl_native numeric,
    open_pnl_base numeric,
    closed_pnl_native numeric,
    closed_pnl_base numeric,
    source text,
    created_at timestamptz,
    display_currency text,
    fx_to_display numeric,
    nav_display numeric,
    cash_display numeric,
    market_value_display numeric,
    open_pnl_display numeric,
    closed_pnl_display numeric
 )
 LANGUAGE sql
 STABLE
AS $function$
    WITH f AS ( SELECT public.fx_to_display(p_display, CURRENT_DATE) AS r )
    SELECT DISTINCT ON (b.portfolio_name, b.broker)
        b.portfolio_name,
        b.base_currency,
        b.broker,
        b.snapshot_date,
        b.native_currency,
        b.fx_rate_to_base,
        b.nav_native,
        b.nav_base,
        b.cash_native,
        b.cash_base,
        b.market_value_native,
        b.market_value_base,
        b.open_pnl_native,
        b.open_pnl_base,
        b.closed_pnl_native,
        b.closed_pnl_base,
        b.source,
        b.created_at,
        COALESCE(upper(p_display), 'CHF') AS display_currency,
        f.r AS fx_to_display,
        b.nav_base          * f.r AS nav_display,
        b.cash_base         * f.r AS cash_display,
        b.market_value_base * f.r AS market_value_display,
        b.open_pnl_base     * f.r AS open_pnl_display,
        b.closed_pnl_base   * f.r AS closed_pnl_display
    FROM public.v_portfolio_nav_snapshots_base b
    CROSS JOIN f
    ORDER BY b.portfolio_name, b.broker, b.snapshot_date DESC, b.created_at DESC;
$function$;


-- ---------------------------------------------------------------------
-- 1b) get_nav_series_display — FULL NAV history, WIDENED.
--     Same widened columns as get_nav_display, but each row converted at
--     ITS OWN snapshot_date (Variante-B-safe per-row rate).
-- ---------------------------------------------------------------------
DROP FUNCTION IF EXISTS public.get_nav_series_display(text);
CREATE FUNCTION public.get_nav_series_display(p_display text DEFAULT NULL::text)
 RETURNS TABLE(
    portfolio_name text,
    base_currency text,
    broker text,
    snapshot_date date,
    native_currency text,
    fx_rate_to_base numeric,
    nav_native numeric,
    nav_base numeric,
    cash_native numeric,
    cash_base numeric,
    market_value_native numeric,
    market_value_base numeric,
    open_pnl_native numeric,
    open_pnl_base numeric,
    closed_pnl_native numeric,
    closed_pnl_base numeric,
    source text,
    created_at timestamptz,
    display_currency text,
    fx_to_display numeric,
    nav_display numeric,
    cash_display numeric,
    market_value_display numeric,
    open_pnl_display numeric,
    closed_pnl_display numeric
 )
 LANGUAGE sql
 STABLE
AS $function$
    SELECT
        b.portfolio_name,
        b.base_currency,
        b.broker,
        b.snapshot_date,
        b.native_currency,
        b.fx_rate_to_base,
        b.nav_native,
        b.nav_base,
        b.cash_native,
        b.cash_base,
        b.market_value_native,
        b.market_value_base,
        b.open_pnl_native,
        b.open_pnl_base,
        b.closed_pnl_native,
        b.closed_pnl_base,
        b.source,
        b.created_at,
        COALESCE(upper(p_display), 'CHF') AS display_currency,
        public.fx_to_display(p_display, b.snapshot_date) AS fx_to_display,
        b.nav_base          * public.fx_to_display(p_display, b.snapshot_date) AS nav_display,
        b.cash_base         * public.fx_to_display(p_display, b.snapshot_date) AS cash_display,
        b.market_value_base * public.fx_to_display(p_display, b.snapshot_date) AS market_value_display,
        b.open_pnl_base     * public.fx_to_display(p_display, b.snapshot_date) AS open_pnl_display,
        b.closed_pnl_base   * public.fx_to_display(p_display, b.snapshot_date) AS closed_pnl_display
    FROM public.v_portfolio_nav_snapshots_base b
    ORDER BY b.portfolio_name, b.broker, b.snapshot_date, b.created_at;
$function$;


-- ---------------------------------------------------------------------
-- 2) get_overview_display — cumulative per-portfolio totals, WIDENED.
--    Adds the base gross_trade_volume_base / total_fees_base columns
--    alongside their display counterparts. Cumulative volume/fees have no
--    single native date -> CURRENT_DATE rate. Display cols rounded to 2dp.
-- ---------------------------------------------------------------------
DROP FUNCTION IF EXISTS public.get_overview_display(text);
CREATE FUNCTION public.get_overview_display(p_display text DEFAULT NULL::text)
 RETURNS TABLE(
    portfolio_name text,
    base_currency text,
    trade_count bigint,
    instrument_count bigint,
    broker_count bigint,
    gross_trade_volume_base numeric,
    total_fees_base numeric,
    first_trade_at timestamptz,
    last_trade_at timestamptz,
    display_currency text,
    fx_to_display numeric,
    gross_trade_volume_display numeric,
    total_fees_display numeric
 )
 LANGUAGE sql
 STABLE
AS $function$
    WITH f AS ( SELECT public.fx_to_display(p_display, CURRENT_DATE) AS r )
    SELECT
        o.portfolio_name,
        o.base_currency,
        o.trade_count,
        o.instrument_count,
        o.broker_count,
        o.gross_trade_volume_base,
        o.total_fees_base,
        o.first_trade_at,
        o.last_trade_at,
        COALESCE(upper(p_display), 'CHF') AS display_currency,
        f.r AS fx_to_display,
        round(o.gross_trade_volume_base * f.r, 2) AS gross_trade_volume_display,
        round(o.total_fees_base         * f.r, 2) AS total_fees_display
    FROM public.v_portfolio_overview_base o
    CROSS JOIN f;
$function$;


-- ---------------------------------------------------------------------
-- 3) get_allocation_display — allocation by asset_class | broker | currency.
--    WIDENED (B1): keeps generic group_by/group_value AND surfaces all
--    three ORIGINAL grouping columns (asset_class, broker, trade_currency).
--
--    IMPORTANT: the three grouping columns are CONTEXT-DEPENDENTLY NULL.
--    Only the one matching the active p_group_by is populated in each row;
--    the other two are NULL. (e.g. p_group_by='broker' => broker is set,
--    asset_class and trade_currency are NULL.) group_value always mirrors
--    the active grouping column for a uniform generic accessor.
--
--    allocation_percent is scale-invariant -> passed through UNCHANGED.
--    Cumulative volume -> CURRENT_DATE rate. Unknown p_group_by -> empty set.
-- ---------------------------------------------------------------------
DROP FUNCTION IF EXISTS public.get_allocation_display(text, text);
CREATE FUNCTION public.get_allocation_display(
    p_display text DEFAULT NULL::text,
    p_group_by text DEFAULT 'asset_class'::text
)
 RETURNS TABLE(
    portfolio_name text,
    base_currency text,
    asset_class text,      -- populated only when group_by='asset_class', else NULL
    broker text,           -- populated only when group_by='broker', else NULL
    trade_currency text,   -- populated only when group_by='currency', else NULL
    group_by text,
    group_value text,
    trade_count bigint,
    gross_trade_volume_base numeric,
    allocation_percent numeric,
    display_currency text,
    fx_to_display numeric,
    gross_trade_volume_display numeric
 )
 LANGUAGE sql
 STABLE
AS $function$
    WITH f AS ( SELECT public.fx_to_display(p_display, CURRENT_DATE) AS r )
    -- group_by = 'asset_class' : asset_class set; broker / trade_currency NULL
    SELECT
        a.portfolio_name,
        a.base_currency,
        a.asset_class,
        NULL::text AS broker,
        NULL::text AS trade_currency,
        'asset_class'::text AS group_by,
        a.asset_class AS group_value,
        a.trade_count,
        a.gross_trade_volume_base,
        a.allocation_percent,   -- scale-invariant: passed through unchanged
        COALESCE(upper(p_display), 'CHF') AS display_currency,
        f.r AS fx_to_display,
        round(a.gross_trade_volume_base * f.r, 2) AS gross_trade_volume_display
    FROM public.v_allocation_asset_class_base a
    CROSS JOIN f
    WHERE lower(p_group_by) = 'asset_class'

    UNION ALL

    -- group_by = 'broker' : broker set; asset_class / trade_currency NULL
    SELECT
        a.portfolio_name,
        a.base_currency,
        NULL::text AS asset_class,
        a.broker,
        NULL::text AS trade_currency,
        'broker'::text AS group_by,
        a.broker AS group_value,
        a.trade_count,
        a.gross_trade_volume_base,
        a.allocation_percent,
        COALESCE(upper(p_display), 'CHF'),
        f.r,
        round(a.gross_trade_volume_base * f.r, 2)
    FROM public.v_allocation_broker_base a
    CROSS JOIN f
    WHERE lower(p_group_by) = 'broker'

    UNION ALL

    -- group_by = 'currency' : trade_currency set; asset_class / broker NULL
    SELECT
        a.portfolio_name,
        a.base_currency,
        NULL::text AS asset_class,
        NULL::text AS broker,
        a.trade_currency,
        'currency'::text AS group_by,
        a.trade_currency AS group_value,
        a.trade_count,
        a.gross_trade_volume_base,
        a.allocation_percent,
        COALESCE(upper(p_display), 'CHF'),
        f.r,
        round(a.gross_trade_volume_base * f.r, 2)
    FROM public.v_allocation_currency_base a
    CROSS JOIN f
    WHERE lower(p_group_by) = 'currency';
$function$;


-- ---------------------------------------------------------------------
-- 4) get_positions_display — open positions, WIDENED.
--    Full v_positions_public_base row (incl. TP/SL, native+base values,
--    fx_rate_to_base, source_position_id, tp/sl order json) + display cols.
--    LIVE snapshot -> CURRENT_DATE rate. quantity / avg_cost / market_price
--    stay NATIVE; only market_value / open_pnl convert.
-- ---------------------------------------------------------------------
DROP FUNCTION IF EXISTS public.get_positions_display(text);
CREATE FUNCTION public.get_positions_display(p_display text DEFAULT NULL::text)
 RETURNS TABLE(
    portfolio_name text,
    base_currency text,
    broker text,
    symbol text,
    asset_class text,
    quantity numeric,
    avg_cost numeric,
    currency text,
    market_price numeric,
    market_value_native numeric,
    market_value_base numeric,
    open_pnl_native numeric,
    open_pnl_base numeric,
    fx_rate_to_base numeric,
    entry_date timestamptz,
    position_side text,
    take_profit numeric,
    stop_loss numeric,
    take_profit_order_id text,
    stop_loss_order_id text,
    source_position_id text,
    updated_at timestamptz,
    take_profit_orders jsonb,
    stop_loss_orders jsonb,
    display_currency text,
    fx_to_display numeric,
    market_value_display numeric,
    open_pnl_display numeric
 )
 LANGUAGE sql
 STABLE
AS $function$
    WITH f AS ( SELECT public.fx_to_display(p_display, CURRENT_DATE) AS r )
    SELECT
        pb.portfolio_name,
        pb.base_currency,
        pb.broker,
        pb.symbol,
        pb.asset_class,
        pb.quantity,        -- native
        pb.avg_cost,        -- native
        pb.currency,
        pb.market_price,    -- native
        pb.market_value_native,
        pb.market_value_base,
        pb.open_pnl_native,
        pb.open_pnl_base,
        pb.fx_rate_to_base,
        pb.entry_date,
        pb.position_side,
        pb.take_profit,
        pb.stop_loss,
        pb.take_profit_order_id,
        pb.stop_loss_order_id,
        pb.source_position_id,
        pb.updated_at,
        pb.take_profit_orders,
        pb.stop_loss_orders,
        COALESCE(upper(p_display), 'CHF') AS display_currency,
        f.r AS fx_to_display,
        pb.market_value_base * f.r AS market_value_display,
        pb.open_pnl_base     * f.r AS open_pnl_display
    FROM public.v_positions_public_base pb
    CROSS JOIN f;
$function$;


-- ---------------------------------------------------------------------
-- 5) get_trades_display — per-row trade values, WIDENED.
--    Full v_trades_enriched_base row (incl. native fee, fx_rate_to_base,
--    gross_value_base, fee_base) + display cols. Pivots gross_value_base /
--    fee_base at the PER-ROW trade date (trade_timestamp::date).
--    quantity / price / signed_quantity / gross_value_native / fee stay NATIVE.
-- ---------------------------------------------------------------------
DROP FUNCTION IF EXISTS public.get_trades_display(text);
CREATE FUNCTION public.get_trades_display(p_display text DEFAULT NULL::text)
 RETURNS TABLE(
    id uuid,
    portfolio_name text,
    base_currency text,
    broker text,
    symbol text,
    asset_class text,
    instrument_type text,
    side text,
    quantity numeric,
    price numeric,
    currency text,
    fee numeric,
    trade_timestamp timestamptz,
    signed_quantity numeric,
    gross_value_native numeric,
    fx_rate_to_base numeric,
    gross_value_base numeric,
    fee_base numeric,
    display_currency text,
    fx_to_display numeric,
    gross_value_display numeric,
    fee_display numeric
 )
 LANGUAGE sql
 STABLE
AS $function$
    SELECT
        t.id,
        t.portfolio_name,
        t.base_currency,
        t.broker,
        t.symbol,
        t.asset_class,
        t.instrument_type,
        t.side,
        t.quantity,             -- native
        t.price,                -- native
        t.currency,
        t.fee,                  -- native
        t.trade_timestamp,
        t.signed_quantity,      -- native
        t.gross_value_native,   -- native
        t.fx_rate_to_base,
        t.gross_value_base,
        t.fee_base,
        COALESCE(upper(p_display), 'CHF') AS display_currency,
        public.fx_to_display(p_display, t.trade_timestamp::date) AS fx_to_display,
        t.gross_value_base * public.fx_to_display(p_display, t.trade_timestamp::date) AS gross_value_display,
        t.fee_base         * public.fx_to_display(p_display, t.trade_timestamp::date) AS fee_display
    FROM public.v_trades_enriched_base t;
$function$;


-- ---------------------------------------------------------------------
-- 6) get_closed_positions_display — realized P&L per symbol, WIDENED.
--    Adds closed_pnl_base alongside closed_pnl_native + closed_pnl_display.
--    Per-row close date -> last_event_time::date (per-day rate).
-- ---------------------------------------------------------------------
DROP FUNCTION IF EXISTS public.get_closed_positions_display(text);
CREATE FUNCTION public.get_closed_positions_display(p_display text DEFAULT NULL::text)
 RETURNS TABLE(
    portfolio_name text,
    base_currency text,
    broker text,
    symbol text,
    asset_class text,
    event_count bigint,
    closed_pnl_native numeric,
    closed_pnl_base numeric,
    currency text,
    first_event_time timestamptz,
    last_event_time timestamptz,
    display_currency text,
    fx_to_display numeric,
    closed_pnl_display numeric
 )
 LANGUAGE sql
 STABLE
AS $function$
    SELECT
        c.portfolio_name,
        c.base_currency,
        c.broker,
        c.symbol,
        c.asset_class,
        c.event_count,
        c.closed_pnl_native,    -- native
        c.closed_pnl_base,
        c.currency,
        c.first_event_time,
        c.last_event_time,
        COALESCE(upper(p_display), 'CHF') AS display_currency,
        public.fx_to_display(p_display, c.last_event_time::date) AS fx_to_display,
        c.closed_pnl_base * public.fx_to_display(p_display, c.last_event_time::date) AS closed_pnl_display
    FROM public.v_closed_positions_public c;
$function$;


-- ---------------------------------------------------------------------
-- 7) get_portfolio_summary_display — live aggregated summary, WIDENED.
--    Adds the base nav_currency + native aggregate money columns (nav, cash,
--    market_value, open_pnl, closed_pnl, total_pnl) alongside *_display.
--    LIVE aggregate NAV -> CURRENT_DATE rate. Display cols raw (mirror 0003).
-- ---------------------------------------------------------------------
DROP FUNCTION IF EXISTS public.get_portfolio_summary_display(text);
CREATE FUNCTION public.get_portfolio_summary_display(p_display text DEFAULT NULL::text)
 RETURNS TABLE(
    portfolio_name text,
    base_currency text,
    nav_currency text,
    nav numeric,
    cash numeric,
    market_value numeric,
    open_pnl numeric,
    closed_pnl numeric,
    total_pnl numeric,
    has_nav boolean,
    status_label text,
    trade_count bigint,
    last_trade_at timestamptz,
    last_sync_at timestamptz,
    nav_updated_at timestamptz,
    display_currency text,
    fx_to_display numeric,
    nav_display numeric,
    cash_display numeric,
    market_value_display numeric,
    open_pnl_display numeric,
    closed_pnl_display numeric,
    total_pnl_display numeric
 )
 LANGUAGE sql
 STABLE
AS $function$
    WITH f AS ( SELECT public.fx_to_display(p_display, CURRENT_DATE) AS r )
    SELECT
        s.portfolio_name,
        s.base_currency,
        s.nav_currency,
        s.nav,
        s.cash,
        s.market_value,
        s.open_pnl,
        s.closed_pnl,
        s.total_pnl,
        s.has_nav,
        s.status_label,
        s.trade_count,
        s.last_trade_at,
        s.last_sync_at,
        s.nav_updated_at,
        COALESCE(upper(p_display), 'CHF') AS display_currency,
        f.r AS fx_to_display,
        s.nav          * f.r AS nav_display,
        s.cash         * f.r AS cash_display,
        s.market_value * f.r AS market_value_display,
        s.open_pnl     * f.r AS open_pnl_display,
        s.closed_pnl   * f.r AS closed_pnl_display,
        s.total_pnl    * f.r AS total_pnl_display
    FROM public.v_public_portfolio_summary s
    CROSS JOIN f;
$function$;


-- ---------------------------------------------------------------------
-- 8) get_cashflows_display — deposits/withdrawals, WIDENED.
--    Adds amount_base alongside amount_native + amount_display.
--    Per-row cashflow_date (per-day rate).
-- ---------------------------------------------------------------------
DROP FUNCTION IF EXISTS public.get_cashflows_display(text);
CREATE FUNCTION public.get_cashflows_display(p_display text DEFAULT NULL::text)
 RETURNS TABLE(
    portfolio_name text,
    base_currency text,
    broker text,
    cashflow_date date,
    currency text,
    amount_native numeric,
    amount_base numeric,
    cashflow_type text,
    source text,
    external_id text,
    created_at timestamptz,
    display_currency text,
    fx_to_display numeric,
    amount_display numeric
 )
 LANGUAGE sql
 STABLE
AS $function$
    SELECT
        c.portfolio_name,
        c.base_currency,
        c.broker,
        c.cashflow_date::date,
        c.currency,
        c.amount_native,       -- native
        c.amount_base,
        c.cashflow_type,
        c.source,
        c.external_id,
        c.created_at,
        COALESCE(upper(p_display), 'CHF') AS display_currency,
        public.fx_to_display(p_display, c.cashflow_date::date) AS fx_to_display,
        c.amount_base * public.fx_to_display(p_display, c.cashflow_date::date) AS amount_display
    FROM public.v_portfolio_cashflows_public c;
$function$;


COMMIT;
