-- =====================================================================
-- Migration 0003 — display wrapper functions (Block 2, step 3)
--
-- Depends on baseline (db/schema.sql), 0001, and 0002 (fx_to_display).
-- Apply order: db/schema.sql -> 0001 -> 0002 -> 0003.
--
-- PURELY ADDITIVE & SAFE TO APPLY ANYTIME:
--   * Creates NINE new functions. Touches no table, view, or existing
--     function. Nothing calls them yet — they stay inert until step 4
--     wires the ?currency= endpoints. Dead functions = zero runtime
--     effect on the live system.
--   * Idempotent: every object uses CREATE OR REPLACE; safe to re-run.
--
-- WHAT THESE WRAPPERS DO (the whole Block-2 contract in one place):
--   Each function is a THIN wrapper over an existing *_base view. It
--   pivots the canonical CHF base value through the single multiplier
--   from migration 0002:
--       value_display = value_base (CHF) * fx_to_display(p_display, date)
--   so the carry-forward / fallback policy is identical everywhere by
--   construction. There is NO second native->display path, so nothing
--   can diverge from the base layer.
--
-- THE SIX CRITICAL INVARIANTS (each one is visible in the code below):
--   1) display = 'CHF' (or NULL) => fx_to_display = 1 => every _display
--      column equals its _base column BYTE-FOR-BYTE. Null-regression
--      guarantee: today's behaviour is reproduced exactly.
--   2) PERFORMANCE stays canonical CHF. None of these wrappers touch
--      v_portfolio_performance_* / TWR / MWR. Performance is never
--      parametrised by currency — it is "Performance in CHF", full stop.
--   3) allocation_percent is SCALE-INVARIANT and is passed through
--      UNCHANGED. (num*f)/(den*f) cancels f, so converting would be a
--      no-op at best and a rounding-bug at worst. We do not multiply it.
--   4) quantity / price / avg_cost / market_price stay NATIVE. They are
--      instrument quantities and per-unit prices, not portfolio values;
--      converting them would be meaningless. Only money columns convert.
--   5) get_trades_display is NEW. It pivots through gross_value_base /
--      fee_base at the PER-ROW trade date, leaving the old, deprecated
--      get_trades_enriched() (native->display, fx_rate_to_display)
--      completely untouched.
--   6) DATE POLICY:
--        - Genuinely per-row dated rows -> the ROW's own native date
--          (per-day rate):  trades -> trade_timestamp::date,
--          closed positions -> last_event_time::date,
--          cashflows -> cashflow_date,
--          NAV time-series -> snapshot_date (each historical NAV point
--          at ITS OWN day's rate — this is the Variante-B-safe path; a
--          NAV curve in EUR/USD must NOT be scaled by a single factor).
--        - Live snapshots and cumulative aggregates that have no single
--          native date -> CURRENT_DATE (today's rate):
--          latest NAV, positions, overview (cumulative volume/fees),
--          allocation (cumulative volume), summary (live aggregate NAV).
--
-- VALIDATION: allowed-currency validation (CHF/EUR/USD/GBP/JPY/CNY) lives
-- in the API layer (step 4). If an unknown target is passed here,
-- fx_to_display returns NULL and the _display columns degrade to NULL
-- (the safe "not convertible" degenerate). These functions never raise.
-- =====================================================================

SET search_path TO public, extensions;


-- ---------------------------------------------------------------------
-- 1) get_nav_display — latest NAV per broker, in display currency.
--
--    v_nav_latest_public is NATIVE (s.nav in s.currency, no _base cols),
--    so we pivot through v_portfolio_nav_snapshots_base instead, taking
--    the newest snapshot per (portfolio, broker). LIVE snapshot -> today.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_nav_display(p_display text DEFAULT NULL::text)
 RETURNS TABLE(
    portfolio_name text,
    base_currency text,
    broker text,
    snapshot_date date,
    native_currency text,
    display_currency text,
    fx_to_display numeric,
    nav_display numeric,
    cash_display numeric,
    market_value_display numeric,
    open_pnl_display numeric,
    closed_pnl_display numeric,
    source text,
    created_at timestamptz
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
        COALESCE(upper(p_display), 'CHF') AS display_currency,
        f.r AS fx_to_display,
        b.nav_base          * f.r AS nav_display,
        b.cash_base         * f.r AS cash_display,
        b.market_value_base * f.r AS market_value_display,
        b.open_pnl_base     * f.r AS open_pnl_display,
        b.closed_pnl_base   * f.r AS closed_pnl_display,
        b.source,
        b.created_at
    FROM public.v_portfolio_nav_snapshots_base b
    CROSS JOIN f
    ORDER BY b.portfolio_name, b.broker, b.snapshot_date DESC, b.created_at DESC;
$function$;


-- ---------------------------------------------------------------------
-- 1b) get_nav_series_display — FULL NAV history, in display currency.
--
--     Variante-B-safe: each historical snapshot is converted at ITS OWN
--     snapshot_date (per-row rate), NOT today's. A NAV curve in EUR/USD
--     therefore reflects real FX movement over time instead of being
--     uniformly rescaled by one factor. Use this for time-series charts;
--     use get_nav_display for the single current value.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_nav_series_display(p_display text DEFAULT NULL::text)
 RETURNS TABLE(
    portfolio_name text,
    base_currency text,
    broker text,
    snapshot_date date,
    native_currency text,
    display_currency text,
    fx_to_display numeric,
    nav_display numeric,
    cash_display numeric,
    market_value_display numeric,
    open_pnl_display numeric,
    closed_pnl_display numeric,
    source text,
    created_at timestamptz
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
        COALESCE(upper(p_display), 'CHF') AS display_currency,
        public.fx_to_display(p_display, b.snapshot_date) AS fx_to_display,
        b.nav_base          * public.fx_to_display(p_display, b.snapshot_date) AS nav_display,
        b.cash_base         * public.fx_to_display(p_display, b.snapshot_date) AS cash_display,
        b.market_value_base * public.fx_to_display(p_display, b.snapshot_date) AS market_value_display,
        b.open_pnl_base     * public.fx_to_display(p_display, b.snapshot_date) AS open_pnl_display,
        b.closed_pnl_base   * public.fx_to_display(p_display, b.snapshot_date) AS closed_pnl_display,
        b.source,
        b.created_at
    FROM public.v_portfolio_nav_snapshots_base b
    ORDER BY b.portfolio_name, b.broker, b.snapshot_date, b.created_at;
$function$;


-- ---------------------------------------------------------------------
-- 2) get_overview_display — cumulative per-portfolio totals.
--
--    Cumulative gross volume + fees have NO single native date -> today.
--    Counts (trade/instrument/broker) and dates pass through unchanged.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_overview_display(p_display text DEFAULT NULL::text)
 RETURNS TABLE(
    portfolio_name text,
    base_currency text,
    display_currency text,
    fx_to_display numeric,
    trade_count bigint,
    instrument_count bigint,
    broker_count bigint,
    gross_trade_volume_display numeric,
    total_fees_display numeric,
    first_trade_at timestamptz,
    last_trade_at timestamptz
 )
 LANGUAGE sql
 STABLE
AS $function$
    WITH f AS ( SELECT public.fx_to_display(p_display, CURRENT_DATE) AS r )
    SELECT
        o.portfolio_name,
        o.base_currency,
        COALESCE(upper(p_display), 'CHF') AS display_currency,
        f.r AS fx_to_display,
        o.trade_count,
        o.instrument_count,
        o.broker_count,
        round(o.gross_trade_volume_base * f.r, 2) AS gross_trade_volume_display,
        round(o.total_fees_base         * f.r, 2) AS total_fees_display,
        o.first_trade_at,
        o.last_trade_at
    FROM public.v_portfolio_overview_base o
    CROSS JOIN f;
$function$;


-- ---------------------------------------------------------------------
-- 3) get_allocation_display — allocation by asset_class | broker | currency.
--
--    p_group_by selects which base view to wrap. allocation_percent is
--    scale-invariant and passes through UNCHANGED. Cumulative volume ->
--    today's rate. Unknown p_group_by -> empty set.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_allocation_display(
    p_display text DEFAULT NULL::text,
    p_group_by text DEFAULT 'asset_class'::text
)
 RETURNS TABLE(
    portfolio_name text,
    base_currency text,
    display_currency text,
    fx_to_display numeric,
    group_by text,
    group_value text,
    trade_count bigint,
    gross_trade_volume_display numeric,
    allocation_percent numeric
 )
 LANGUAGE sql
 STABLE
AS $function$
    WITH f AS ( SELECT public.fx_to_display(p_display, CURRENT_DATE) AS r )
    SELECT
        a.portfolio_name,
        a.base_currency,
        COALESCE(upper(p_display), 'CHF') AS display_currency,
        f.r AS fx_to_display,
        'asset_class'::text AS group_by,
        a.asset_class AS group_value,
        a.trade_count,
        round(a.gross_trade_volume_base * f.r, 2) AS gross_trade_volume_display,
        a.allocation_percent   -- scale-invariant: passed through unchanged
    FROM public.v_allocation_asset_class_base a
    CROSS JOIN f
    WHERE lower(p_group_by) = 'asset_class'

    UNION ALL

    SELECT
        a.portfolio_name,
        a.base_currency,
        COALESCE(upper(p_display), 'CHF'),
        f.r,
        'broker'::text,
        a.broker AS group_value,
        a.trade_count,
        round(a.gross_trade_volume_base * f.r, 2),
        a.allocation_percent
    FROM public.v_allocation_broker_base a
    CROSS JOIN f
    WHERE lower(p_group_by) = 'broker'

    UNION ALL

    SELECT
        a.portfolio_name,
        a.base_currency,
        COALESCE(upper(p_display), 'CHF'),
        f.r,
        'currency'::text,
        a.trade_currency AS group_value,
        a.trade_count,
        round(a.gross_trade_volume_base * f.r, 2),
        a.allocation_percent
    FROM public.v_allocation_currency_base a
    CROSS JOIN f
    WHERE lower(p_group_by) = 'currency';
$function$;


-- ---------------------------------------------------------------------
-- 4) get_positions_display — open positions, in display currency.
--
--    LIVE snapshot -> today's rate. quantity / avg_cost / market_price
--    stay NATIVE; only market_value and open_pnl convert.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_positions_display(p_display text DEFAULT NULL::text)
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
    display_currency text,
    fx_to_display numeric,
    market_value_display numeric,
    open_pnl_display numeric,
    entry_date timestamptz,
    position_side text,
    updated_at timestamptz
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
        COALESCE(upper(p_display), 'CHF') AS display_currency,
        f.r AS fx_to_display,
        pb.market_value_base * f.r AS market_value_display,
        pb.open_pnl_base     * f.r AS open_pnl_display,
        pb.entry_date,
        pb.position_side,
        pb.updated_at
    FROM public.v_positions_public_base pb
    CROSS JOIN f;
$function$;


-- ---------------------------------------------------------------------
-- 5) get_trades_display — NEW. Per-row trade values in display currency.
--
--    Pivots through gross_value_base / fee_base at the PER-ROW trade date
--    (trade_timestamp::date) — NOT today. quantity / price / signed_qty /
--    gross_value_native stay NATIVE. Leaves the old deprecated
--    get_trades_enriched() (native->display) completely untouched.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_trades_display(p_display text DEFAULT NULL::text)
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
    trade_timestamp timestamptz,
    signed_quantity numeric,
    gross_value_native numeric,
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
        t.trade_timestamp,
        t.signed_quantity,      -- native
        t.gross_value_native,   -- native
        COALESCE(upper(p_display), 'CHF') AS display_currency,
        public.fx_to_display(p_display, t.trade_timestamp::date) AS fx_to_display,
        t.gross_value_base * public.fx_to_display(p_display, t.trade_timestamp::date) AS gross_value_display,
        t.fee_base         * public.fx_to_display(p_display, t.trade_timestamp::date) AS fee_display
    FROM public.v_trades_enriched_base t;
$function$;


-- ---------------------------------------------------------------------
-- 6) get_closed_positions_display — realized P&L per symbol, in display ccy.
--
--    Per-row close date -> last_event_time::date (per-day rate).
--    closed_pnl_native stays native; only closed_pnl_base converts.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_closed_positions_display(p_display text DEFAULT NULL::text)
 RETURNS TABLE(
    portfolio_name text,
    base_currency text,
    broker text,
    symbol text,
    asset_class text,
    event_count bigint,
    closed_pnl_native numeric,
    currency text,
    display_currency text,
    fx_to_display numeric,
    closed_pnl_display numeric,
    first_event_time timestamptz,
    last_event_time timestamptz
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
        c.currency,
        COALESCE(upper(p_display), 'CHF') AS display_currency,
        public.fx_to_display(p_display, c.last_event_time::date) AS fx_to_display,
        c.closed_pnl_base * public.fx_to_display(p_display, c.last_event_time::date) AS closed_pnl_display,
        c.first_event_time,
        c.last_event_time
    FROM public.v_closed_positions_public c;
$function$;


-- ---------------------------------------------------------------------
-- 7) get_portfolio_summary_display — live aggregated portfolio summary.
--
--    LIVE aggregate NAV -> today's rate. All money columns (nav, cash,
--    market_value, open_pnl, closed_pnl, total_pnl) convert; counts,
--    flags, labels and timestamps pass through unchanged.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_portfolio_summary_display(p_display text DEFAULT NULL::text)
 RETURNS TABLE(
    portfolio_name text,
    base_currency text,
    display_currency text,
    fx_to_display numeric,
    nav_display numeric,
    cash_display numeric,
    market_value_display numeric,
    open_pnl_display numeric,
    closed_pnl_display numeric,
    total_pnl_display numeric,
    has_nav boolean,
    status_label text,
    trade_count bigint,
    last_trade_at timestamptz,
    last_sync_at timestamptz,
    nav_updated_at timestamptz
 )
 LANGUAGE sql
 STABLE
AS $function$
    WITH f AS ( SELECT public.fx_to_display(p_display, CURRENT_DATE) AS r )
    SELECT
        s.portfolio_name,
        s.base_currency,
        COALESCE(upper(p_display), 'CHF') AS display_currency,
        f.r AS fx_to_display,
        s.nav          * f.r AS nav_display,
        s.cash         * f.r AS cash_display,
        s.market_value * f.r AS market_value_display,
        s.open_pnl     * f.r AS open_pnl_display,
        s.closed_pnl   * f.r AS closed_pnl_display,
        s.total_pnl    * f.r AS total_pnl_display,
        s.has_nav,
        s.status_label,
        s.trade_count,
        s.last_trade_at,
        s.last_sync_at,
        s.nav_updated_at
    FROM public.v_public_portfolio_summary s
    CROSS JOIN f;
$function$;


-- ---------------------------------------------------------------------
-- 8) get_cashflows_display — deposits/withdrawals in display currency.
--
--    Per-row cashflow_date (per-day rate). amount_native stays native;
--    only amount_base converts. cashflow_type / source / external_id
--    pass through unchanged.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_cashflows_display(p_display text DEFAULT NULL::text)
 RETURNS TABLE(
    portfolio_name text,
    base_currency text,
    broker text,
    cashflow_date date,
    currency text,
    amount_native numeric,
    display_currency text,
    fx_to_display numeric,
    amount_display numeric,
    cashflow_type text,
    source text,
    external_id text,
    created_at timestamptz
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
        COALESCE(upper(p_display), 'CHF') AS display_currency,
        public.fx_to_display(p_display, c.cashflow_date::date) AS fx_to_display,
        c.amount_base * public.fx_to_display(p_display, c.cashflow_date::date) AS amount_display,
        c.cashflow_type,
        c.source,
        c.external_id,
        c.created_at
    FROM public.v_portfolio_cashflows_public c;
$function$;
