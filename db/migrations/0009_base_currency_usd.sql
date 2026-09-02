-- ---------------------------------------------------------------------------
-- 0009_base_currency_usd.sql  —  CUTOVER: base_currency CHF -> USD (all portfolios)
-- ---------------------------------------------------------------------------
-- WHY
--   Design decision: ALL portfolios report in USD (Global + Alternatives flip
--   from CHF; Day Trading was already USD). Done pre-go-live, no real data yet.
--
-- ⚠ NOT PURELY ADDITIVE (unlike 0001-0008). This is the FIRST migration that
--   MUTATES existing behaviour on purpose:
--     - UPDATEs portfolios.base_currency (data)
--     - CREATE OR REPLACEs fx_to_display + the 9 display wrappers (pivot flip)
--     - CREATE OR REPLACEs v_portfolio_daily_nav (silent-fallback consistency fix)
--   It also backfills fx_rates additively (new from/to pairs only) and fixes one
--   test trade. Baseline schema.sql stays untouched; every change lives here.
--
-- COUPLING (the core reason a base flip is not just an UPDATE):
--   The display toggle computes  value_display = value_base * fx_to_display(disp)
--   and fx_to_display returns the (base -> disp) multiplier, historically hard-
--   anchored to CHF. Once value_base is USD, the anchor MUST become USD or every
--   toggle value is dimensionally wrong. This migration flips base AND anchor in
--   lockstep. (This also fixes the pre-existing Day-Trading display bug, where
--   base was already USD but the anchor was still CHF.)
--
-- DATA READINESS (verified before writing this):
--   native->base is ALWAYS an exact from/to lookup on fx_rates (no cross at read
--   time). 5 of 8 booked native currencies had no ->USD row. This migration
--   backfills them: EUR/GBP/HKD via the CHF bridge (X->USD = X->CHF / USD->CHF,
--   at every X->CHF date; read-time carry-forward then mirrors today's CHF-base
--   behaviour exactly), SOL as a single historic row, CNH via a single row
--   (offshore ~= onshore CNY). USD->X for the display anchor already exists.
-- ---------------------------------------------------------------------------

BEGIN;

-- =========================================================================
-- STEP 1 — base_currency: CHF -> USD for Global + Alternatives
--   Day Trading is already USD (left untouched). Every accounting/perf/risk/
--   benchmark view reads p.base_currency dynamically, so this UPDATE re-points
--   all of them at once.
-- =========================================================================
UPDATE public.portfolios
   SET base_currency = 'USD'
 WHERE name IN ('Global', 'Alternatives')
   AND base_currency = 'CHF';   -- idempotent: no-op on re-run

-- =========================================================================
-- STEP 4 — CHF-bridge backfill for EUR, GBP, HKD  (ADDITIVE: new ->USD pairs)
--   X->USD@date = (X->CHF@date) / (USD->CHF at-or-before date).
--   The USD->CHF leg uses the SAME carry-forward the read path uses, so the
--   derived series is bit-consistent with how base=CHF resolves today.
--   ON CONFLICT DO NOTHING => never touches an existing row => fingerprint-safe.
-- =========================================================================
INSERT INTO public.fx_rates (rate_date, from_currency, to_currency, rate, source)
SELECT x.rate_date, x.from_currency, 'USD', x.rate / u.rate, 'chf_bridge_backfill_0009'
  FROM public.fx_rates x
  CROSS JOIN LATERAL (
        SELECT fr.rate
          FROM public.fx_rates fr
         WHERE fr.from_currency = 'USD' AND fr.to_currency = 'CHF'
           AND fr.rate_date <= x.rate_date
         ORDER BY fr.rate_date DESC
         LIMIT 1
  ) u
 WHERE x.from_currency IN ('EUR', 'GBP', 'HKD')
   AND x.to_currency = 'CHF'
ON CONFLICT (rate_date, from_currency, to_currency) DO NOTHING;

-- =========================================================================
-- STEP 6 — CNH fix (ADDITIVE, no raw-trade mutation)
--   The single test trade (Alternatives / IBKR / CHF.CNH / 2026-08-17) is in
--   CNH (offshore yuan), which no feed provides. Offshore ~= onshore, so we add
--   ONE CNH->USD row = 1 / (USD->CNY at-or-before that date). No trade is
--   rewritten (auditable). See notes below for the relabel alternative.
-- =========================================================================
INSERT INTO public.fx_rates (rate_date, from_currency, to_currency, rate, source)
SELECT DATE '2026-08-17', 'CNH', 'USD', 1.0 / u.rate, 'cnh_as_cny_0009'
  FROM (SELECT fr.rate FROM public.fx_rates fr
         WHERE fr.from_currency = 'USD' AND fr.to_currency = 'CNY'
           AND fr.rate_date <= DATE '2026-08-17'
         ORDER BY fr.rate_date DESC LIMIT 1) u
ON CONFLICT (rate_date, from_currency, to_currency) DO NOTHING;

-- =========================================================================
-- STEP 5 — SOL single row  (ADDITIVE)
--   All 8 SOL cashflows fall on 2025-08-18 (~375d back => outside CoinGecko's
--   365d Demo window), so one row suffices. Value = Twelve Data SOL/USD close
--   for 2025-08-18 = 182.94 (confirmed manually). Read-time carry-forward then
--   values every SOL cashflow on that day. If amount_base is NULL today (no
--   SOL->CHF ever existed), this also repairs a pre-existing hole in MWR.
-- =========================================================================
INSERT INTO public.fx_rates (rate_date, from_currency, to_currency, rate, source)
VALUES (DATE '2025-08-18', 'SOL', 'USD', 182.94, 'sol_spot_td_20250818_0009')
ON CONFLICT (rate_date, from_currency, to_currency) DO NOTHING;

-- =========================================================================
-- STEP 2 — Pivot flip: fx_to_display anchor CHF -> USD
--   Identity when display is NULL or USD (=> 1, base==display byte-identical).
--   Else the (USD -> display) rate, carry-forward then latest. USD->X rows are
--   already written daily by the FX sync, so no data gap.
-- =========================================================================
CREATE OR REPLACE FUNCTION public.fx_to_display(p_display text, p_date date)
 RETURNS numeric
 LANGUAGE sql
 STABLE
AS $function$
    SELECT CASE
        WHEN p_display IS NULL OR upper(p_display) = 'USD' THEN 1::numeric
        ELSE COALESCE(
            ( SELECT fr.rate
                FROM public.fx_rates fr
               WHERE fr.from_currency = 'USD'
                 AND fr.to_currency = upper(p_display)
                 AND fr.rate_date <= p_date
               ORDER BY fr.rate_date DESC
               LIMIT 1 ),
            ( SELECT fr.rate
                FROM public.fx_rates fr
               WHERE fr.from_currency = 'USD'
                 AND fr.to_currency = upper(p_display)
               ORDER BY fr.rate_date DESC
               LIMIT 1 )
        )
    END;
$function$;

-- =========================================================================
-- STEP 8 — Consistency fix: v_portfolio_daily_nav silent fallback -> NULL
--   Old: missing FX rate => ELSE s.nav (native passed through UNCONVERTED =
--   silently wrong, poisons TWR/risk/benchmark). New: ELSE NULL => the day
--   drops out (final WHERE nav_base IS NOT NULL), same philosophy as the >-1
--   skip. Under current data the ELSE branch is never hit (nav currencies CHF
--   [=base] and USDT [rate exists]) => fingerprint-neutral; this is a guard-rail.
--   Body is byte-identical to schema.sql EXCEPT the one ELSE line.
-- =========================================================================
CREATE OR REPLACE VIEW public.v_portfolio_daily_nav AS
 WITH latest_fx AS (
         SELECT DISTINCT ON (fx_rates.from_currency, fx_rates.to_currency) fx_rates.from_currency,
            fx_rates.to_currency,
            fx_rates.rate,
            fx_rates.rate_date
           FROM fx_rates
          ORDER BY fx_rates.from_currency, fx_rates.to_currency, fx_rates.rate_date DESC
        ), snapshots_base AS (
         SELECT p.id AS portfolio_id,
            p.name AS portfolio_name,
            p.base_currency,
            s.broker,
            s.snapshot_date,
            s.currency,
                CASE
                    WHEN s.currency = p.base_currency THEN s.nav
                    WHEN fx.rate IS NOT NULL THEN s.nav * fx.rate
                    ELSE NULL::numeric          -- was: ELSE s.nav (silent pass-through)
                END AS nav_base
           FROM portfolio_nav_snapshots s
             JOIN portfolios p ON p.id = s.portfolio_id
             LEFT JOIN latest_fx fx ON fx.from_currency = s.currency AND fx.to_currency = p.base_currency
          WHERE s.nav IS NOT NULL
        ), calendar AS (
         SELECT p.id AS portfolio_id,
            p.name AS portfolio_name,
            p.base_currency,
            d.d::date AS snapshot_date
           FROM portfolios p
             CROSS JOIN generate_series((( SELECT min(portfolio_nav_snapshots.snapshot_date) AS min
                   FROM portfolio_nav_snapshots
                  WHERE portfolio_nav_snapshots.nav IS NOT NULL))::timestamp with time zone, CURRENT_DATE::timestamp with time zone, '1 day'::interval) d(d)
        ), portfolio_brokers AS (
         SELECT DISTINCT snapshots_base.portfolio_id,
            snapshots_base.portfolio_name,
            snapshots_base.base_currency,
            snapshots_base.broker
           FROM snapshots_base
        ), calendar_brokers AS (
         SELECT c.portfolio_id,
            c.portfolio_name,
            c.base_currency,
            c.snapshot_date,
            b.broker
           FROM calendar c
             JOIN portfolio_brokers b ON b.portfolio_id = c.portfolio_id
        ), broker_nav_carry_forward AS (
         SELECT cb.portfolio_id,
            cb.portfolio_name,
            cb.base_currency,
            cb.snapshot_date,
            cb.broker,
            ( SELECT sb.nav_base
                   FROM snapshots_base sb
                  WHERE sb.portfolio_id = cb.portfolio_id AND sb.broker = cb.broker AND sb.snapshot_date <= cb.snapshot_date
                  ORDER BY sb.snapshot_date DESC
                 LIMIT 1) AS nav_base
           FROM calendar_brokers cb
        )
 SELECT portfolio_id,
    portfolio_name,
    base_currency,
    snapshot_date,
    sum(nav_base) AS nav_base
   FROM broker_nav_carry_forward
  WHERE nav_base IS NOT NULL
  GROUP BY portfolio_id, portfolio_name, base_currency, snapshot_date;

-- =========================================================================
-- STEP 2 (cont.) — Display wrappers: label default 'CHF' -> 'USD'
--   9 functions re-declared. Bodies are VERBATIM from 0004, the ONLY change is
--   COALESCE(upper(p_display),'CHF') -> 'USD' (11 spots). fx_to_display already
--   gets raw p_display, so this only corrects the default LABEL; the value math
--   is driven by the flipped anchor above. (Orthogonal to correctness via the
--   API, which passes an explicit display; included so the wrappers' own NULL
--   default is self-consistent with the new base.)
-- =========================================================================
CREATE OR REPLACE FUNCTION public.get_nav_display(p_display text DEFAULT NULL::text)
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
        COALESCE(upper(p_display), 'USD') AS display_currency,
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

CREATE OR REPLACE FUNCTION public.get_nav_series_display(p_display text DEFAULT NULL::text)
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
        COALESCE(upper(p_display), 'USD') AS display_currency,
        public.fx_to_display(p_display, b.snapshot_date) AS fx_to_display,
        b.nav_base          * public.fx_to_display(p_display, b.snapshot_date) AS nav_display,
        b.cash_base         * public.fx_to_display(p_display, b.snapshot_date) AS cash_display,
        b.market_value_base * public.fx_to_display(p_display, b.snapshot_date) AS market_value_display,
        b.open_pnl_base     * public.fx_to_display(p_display, b.snapshot_date) AS open_pnl_display,
        b.closed_pnl_base   * public.fx_to_display(p_display, b.snapshot_date) AS closed_pnl_display
    FROM public.v_portfolio_nav_snapshots_base b
    ORDER BY b.portfolio_name, b.broker, b.snapshot_date, b.created_at;
$function$;

CREATE OR REPLACE FUNCTION public.get_overview_display(p_display text DEFAULT NULL::text)
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
        COALESCE(upper(p_display), 'USD') AS display_currency,
        f.r AS fx_to_display,
        round(o.gross_trade_volume_base * f.r, 2) AS gross_trade_volume_display,
        round(o.total_fees_base         * f.r, 2) AS total_fees_display
    FROM public.v_portfolio_overview_base o
    CROSS JOIN f;
$function$;

CREATE OR REPLACE FUNCTION public.get_allocation_display(
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
        COALESCE(upper(p_display), 'USD') AS display_currency,
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
        COALESCE(upper(p_display), 'USD'),
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
        COALESCE(upper(p_display), 'USD'),
        f.r,
        round(a.gross_trade_volume_base * f.r, 2)
    FROM public.v_allocation_currency_base a
    CROSS JOIN f
    WHERE lower(p_group_by) = 'currency';
$function$;

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
        COALESCE(upper(p_display), 'USD') AS display_currency,
        f.r AS fx_to_display,
        pb.market_value_base * f.r AS market_value_display,
        pb.open_pnl_base     * f.r AS open_pnl_display
    FROM public.v_positions_public_base pb
    CROSS JOIN f;
$function$;

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
        COALESCE(upper(p_display), 'USD') AS display_currency,
        public.fx_to_display(p_display, t.trade_timestamp::date) AS fx_to_display,
        t.gross_value_base * public.fx_to_display(p_display, t.trade_timestamp::date) AS gross_value_display,
        t.fee_base         * public.fx_to_display(p_display, t.trade_timestamp::date) AS fee_display
    FROM public.v_trades_enriched_base t;
$function$;

CREATE OR REPLACE FUNCTION public.get_closed_positions_display(p_display text DEFAULT NULL::text)
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
        COALESCE(upper(p_display), 'USD') AS display_currency,
        public.fx_to_display(p_display, c.last_event_time::date) AS fx_to_display,
        c.closed_pnl_base * public.fx_to_display(p_display, c.last_event_time::date) AS closed_pnl_display
    FROM public.v_closed_positions_public c;
$function$;

CREATE OR REPLACE FUNCTION public.get_portfolio_summary_display(p_display text DEFAULT NULL::text)
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
        COALESCE(upper(p_display), 'USD') AS display_currency,
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

CREATE OR REPLACE FUNCTION public.get_cashflows_display(p_display text DEFAULT NULL::text)
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
        COALESCE(upper(p_display), 'USD') AS display_currency,
        public.fx_to_display(p_display, c.cashflow_date::date) AS fx_to_display,
        c.amount_base * public.fx_to_display(p_display, c.cashflow_date::date) AS amount_display
    FROM public.v_portfolio_cashflows_public c;
$function$;

COMMIT;

-- ---------------------------------------------------------------------------
-- POST-APPLY VERIFICATION (run manually; not part of the transaction)
-- ---------------------------------------------------------------------------
-- (0) DAY TRADING byte-identical where nothing should change. Its base was
--     already USD, so NAV/perf/risk must be unchanged. Fingerprint before+after:
--       SELECT md5(string_agg(snapshot_date::text||':'||nav_base, ',' ORDER BY snapshot_date))
--         FROM public.v_portfolio_daily_nav WHERE portfolio_name='Day Trading';
--
-- (1) "byte-identical-at-USD" (mirror of the old CHF contract): with the base as
--     display, fx_to_display=1 and every _display == _base EXACTLY.
--       SELECT count(*) FILTER (WHERE nav_display <> nav_base) AS mismatches
--         FROM public.get_nav_display('USD');      -- expect 0
--       SELECT fx_to_display FROM public.get_nav_display('USD') LIMIT 1;  -- expect 1
--
-- (2) BACKFILL sanity: every booked EUR/GBP/HKD native now resolves to USD.
--       SELECT v.c AS native_currency,
--              EXISTS (SELECT 1 FROM public.fx_rates f
--                      WHERE f.from_currency=v.c AND f.to_currency='USD') AS has_to_usd
--         FROM (VALUES ('EUR'),('GBP'),('HKD'),('CNH')) v(c) ORDER BY 1;  -- all true
--
-- (3) NO silent pass-through remains: no snapshot day is valued without a rate.
--       -- (guard-rail; expect the count unchanged vs before, i.e. 0 new NULLs)
--       SELECT portfolio_name, count(*) FROM public.v_portfolio_daily_nav
--        GROUP BY 1 ORDER BY 1;
--
-- (4) PLAUSIBILITY for the flipped portfolios: new USD NAV ~= old CHF NAV * (CHF->USD).
--     (record old CHF nav_base BEFORE apply, compare after.)
--       SELECT portfolio_name, snapshot_date, nav_base AS nav_usd
--         FROM public.v_portfolio_daily_nav
--        WHERE portfolio_name IN ('Global','Alternatives')
--        ORDER BY portfolio_name, snapshot_date DESC LIMIT 5;
--
-- (5) Performance/Risk/Benchmark now echo USD for all three portfolios.
--       SELECT name, base_currency FROM public.portfolios ORDER BY name;  -- all USD
-- ---------------------------------------------------------------------------
