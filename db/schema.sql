-- =====================================================================
-- Valbura — public schema baseline
-- Reconstructed via MCP introspection (Weg ii) on 2026-05-29.
-- Represents the PRE-A3 state of the production Supabase DB
--   (project: vccmozgivgcvvapmpfot)
-- A3 (xirr function + MWR view columns) follows as migration 0001.
--
-- Apply order: this file FIRST, then db/migrations/0001_*.sql onward.
--
-- See db/schema.sql.gaps.md for the explicit list of items that are
-- NOT reproduced here (Supabase-platform-managed roles / ACLs /
-- platform extensions / event triggers).
-- =====================================================================

SET statement_timeout = 0;
SET client_min_messages = warning;
SET search_path TO public, extensions;

-- ---------------------------------------------------------------------
-- Extensions (user-relevant; platform-provided ones excluded — see gaps)
-- ---------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS "pgcrypto"  WITH SCHEMA extensions;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA extensions;


-- =====================================================================
-- TABLES (16) — columns + defaults + NOT NULL only.
-- Constraints, indexes, RLS follow after all tables exist.
-- =====================================================================

CREATE TABLE IF NOT EXISTS broker_accounts (
    id                  uuid                        NOT NULL DEFAULT gen_random_uuid(),
    broker              text                        NOT NULL,
    portfolio_id        uuid,
    account_name        text,
    account_identifier  text,
    base_currency       text,
    is_active           boolean                              DEFAULT true,
    created_at          timestamp with time zone             DEFAULT now(),
    source_key          text,
    sync_enabled        boolean                              DEFAULT true,
    notes               text,
    updated_at          timestamp with time zone             DEFAULT now()
);

CREATE TABLE IF NOT EXISTS closed_trades (
    id              uuid                        NOT NULL DEFAULT uuid_generate_v4(),
    portfolio_id    uuid,
    symbol          text                        NOT NULL,
    asset_class     text,
    side            text,
    quantity        numeric,
    entry_price     numeric,
    exit_price      numeric,
    currency        text,
    fee             numeric,
    pnl             numeric,
    open_date       timestamp with time zone,
    close_date      timestamp with time zone,
    broker          text,
    created_at      timestamp with time zone             DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fx_rates (
    id              uuid                        NOT NULL DEFAULT uuid_generate_v4(),
    rate_date       date                        NOT NULL,
    from_currency   text                        NOT NULL,
    to_currency     text                        NOT NULL DEFAULT 'CHF'::text,
    rate            numeric                     NOT NULL,
    source          text,
    created_at      timestamp with time zone             DEFAULT now()
);

CREATE TABLE IF NOT EXISTS import_jobs (
    id              uuid                        NOT NULL DEFAULT uuid_generate_v4(),
    broker          text                        NOT NULL,
    portfolio_id    uuid,
    source_account  text,
    status          text                        NOT NULL,
    started_at      timestamp with time zone             DEFAULT now(),
    finished_at     timestamp with time zone,
    rows_imported   integer                              DEFAULT 0,
    error_message   text,
    portfolio_name  text,
    rows_seen       integer                              DEFAULT 0,
    rows_inserted   integer                              DEFAULT 0,
    rows_updated    integer                              DEFAULT 0,
    metadata        jsonb                                DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS instruments (
    id                   uuid                        NOT NULL DEFAULT uuid_generate_v4(),
    symbol               text                        NOT NULL,
    name                 text,
    asset_class          text,
    instrument_type      text,
    base_currency        text,
    contract_multiplier  numeric                              DEFAULT 1,
    sector               text,
    region               text,
    created_at           timestamp with time zone             DEFAULT now(),
    updated_at           timestamp with time zone             DEFAULT now()
);

CREATE TABLE IF NOT EXISTS market_prices (
    id          uuid                        NOT NULL DEFAULT uuid_generate_v4(),
    price_date  date                        NOT NULL,
    symbol      text                        NOT NULL,
    price       numeric                     NOT NULL,
    currency    text                        NOT NULL,
    source      text,
    created_at  timestamp with time zone             DEFAULT now()
);

CREATE TABLE IF NOT EXISTS portfolio_cash (
    id                 uuid                        NOT NULL DEFAULT gen_random_uuid(),
    portfolio_id       uuid,
    broker             text                        NOT NULL,
    currency           text                        NOT NULL,
    cash_balance       numeric                              DEFAULT 0,
    cash_balance_base  numeric,
    updated_at         timestamp with time zone             DEFAULT now()
);

CREATE TABLE IF NOT EXISTS portfolio_cashflows (
    id              uuid                        NOT NULL DEFAULT gen_random_uuid(),
    portfolio_id    uuid                        NOT NULL,
    broker          text                        NOT NULL,
    cashflow_date   date                        NOT NULL,
    currency        text                        NOT NULL,
    amount_native   numeric                     NOT NULL,
    amount_base     numeric,
    cashflow_type   text                        NOT NULL,
    source          text                        NOT NULL,
    external_id     text                        NOT NULL,
    raw_payload     jsonb,
    created_at      timestamp with time zone    NOT NULL DEFAULT now(),
    updated_at      timestamp with time zone    NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS portfolio_daily_snapshots (
    id                uuid                        NOT NULL DEFAULT uuid_generate_v4(),
    portfolio_id      uuid,
    snapshot_date     date                        NOT NULL,
    nav_chf           numeric,
    cash_chf          numeric,
    market_value_chf  numeric,
    open_pnl_chf      numeric,
    closed_pnl_chf    numeric,
    total_pnl_chf     numeric,
    created_at        timestamp with time zone             DEFAULT now()
);

CREATE TABLE IF NOT EXISTS portfolio_nav_snapshots (
    id                    uuid                        NOT NULL DEFAULT gen_random_uuid(),
    portfolio_id          uuid,
    broker                text,
    snapshot_date         date                        NOT NULL,
    currency              text                        NOT NULL,
    nav                   numeric,
    cash                  numeric,
    market_value          numeric,
    open_pnl              numeric,
    closed_pnl            numeric,
    deposits_withdrawals  numeric,
    source                text,
    created_at            timestamp with time zone             DEFAULT now()
);

CREATE TABLE IF NOT EXISTS portfolios (
    id              uuid                        NOT NULL DEFAULT uuid_generate_v4(),
    name            text                        NOT NULL,
    description     text,
    created_at      timestamp with time zone             DEFAULT now(),
    updated_at      timestamp with time zone             DEFAULT now(),
    base_currency   text                                 DEFAULT 'CHF'::text
);

CREATE TABLE IF NOT EXISTS positions (
    id                    uuid                        NOT NULL DEFAULT uuid_generate_v4(),
    portfolio_id          uuid,
    symbol                text                        NOT NULL,
    asset_class           text,
    side                  text,
    quantity              numeric,
    avg_entry_price       numeric,
    current_price         numeric,
    currency              text,
    open_date             timestamp with time zone,
    broker                text,
    last_updated          timestamp with time zone             DEFAULT now(),
    avg_cost              numeric,
    market_price          numeric,
    market_value_native   numeric,
    market_value_base     numeric,
    open_pnl_native       numeric,
    open_pnl_base         numeric,
    updated_at            timestamp with time zone             DEFAULT now(),
    entry_date            timestamp with time zone,
    position_side         text,
    take_profit           numeric,
    stop_loss             numeric,
    take_profit_order_id  text,
    stop_loss_order_id    text,
    source_position_id    text,
    take_profit_orders    jsonb,
    stop_loss_orders      jsonb
);

CREATE TABLE IF NOT EXISTS realized_pnl_events (
    id                  uuid                        NOT NULL DEFAULT gen_random_uuid(),
    portfolio_id        uuid,
    broker              text                        NOT NULL,
    symbol              text,
    asset_class         text,
    realized_pnl        numeric,
    currency            text,
    realized_pnl_base   numeric,
    event_time          timestamp with time zone,
    external_id         text,
    raw_payload         jsonb,
    created_at          timestamp with time zone             DEFAULT now()
);

CREATE TABLE IF NOT EXISTS supported_display_currencies (
    currency    text                        NOT NULL,
    name        text,
    is_active   boolean                              DEFAULT true,
    created_at  timestamp with time zone             DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sync_errors (
    id              uuid                        NOT NULL DEFAULT uuid_generate_v4(),
    broker          text,
    portfolio_id    uuid,
    source_account  text,
    error_type      text,
    error_message   text,
    raw_payload     jsonb,
    created_at      timestamp with time zone             DEFAULT now(),
    portfolio_name  text
);

CREATE TABLE IF NOT EXISTS trades (
    id                  uuid                        NOT NULL DEFAULT uuid_generate_v4(),
    portfolio_id        uuid,
    symbol              text                        NOT NULL,
    asset_class         text,
    side                text,
    quantity            numeric,
    price               numeric,
    currency            text,
    fee                 numeric,
    broker              text,
    execution_time      timestamp with time zone,
    type                text,
    created_at          timestamp with time zone             DEFAULT now(),
    instrument_type     text,
    trade_date          timestamp with time zone,
    external_order_id   text,
    source_account      text,
    realized_pnl        numeric,
    pnl_currency        text,
    raw_import_id       text,
    imported_at         timestamp with time zone             DEFAULT now(),
    external_trade_id   text,
    raw_payload         jsonb
);


-- =====================================================================
-- CONSTRAINTS — Primary Keys, Unique constraints, Foreign Keys
-- (no CHECK constraints exist on any table)
-- =====================================================================

-- Primary Keys
ALTER TABLE broker_accounts                ADD CONSTRAINT broker_accounts_pkey                PRIMARY KEY (id);
ALTER TABLE closed_trades                  ADD CONSTRAINT closed_trades_pkey                  PRIMARY KEY (id);
ALTER TABLE fx_rates                       ADD CONSTRAINT fx_rates_pkey                       PRIMARY KEY (id);
ALTER TABLE import_jobs                    ADD CONSTRAINT import_jobs_pkey                    PRIMARY KEY (id);
ALTER TABLE instruments                    ADD CONSTRAINT instruments_pkey                    PRIMARY KEY (id);
ALTER TABLE market_prices                  ADD CONSTRAINT market_prices_pkey                  PRIMARY KEY (id);
ALTER TABLE portfolio_cash                 ADD CONSTRAINT portfolio_cash_pkey                 PRIMARY KEY (id);
ALTER TABLE portfolio_cashflows            ADD CONSTRAINT portfolio_cashflows_pkey            PRIMARY KEY (id);
ALTER TABLE portfolio_daily_snapshots      ADD CONSTRAINT portfolio_daily_snapshots_pkey      PRIMARY KEY (id);
ALTER TABLE portfolio_nav_snapshots        ADD CONSTRAINT portfolio_nav_snapshots_pkey        PRIMARY KEY (id);
ALTER TABLE portfolios                     ADD CONSTRAINT portfolios_pkey                     PRIMARY KEY (id);
ALTER TABLE positions                      ADD CONSTRAINT positions_pkey                      PRIMARY KEY (id);
ALTER TABLE realized_pnl_events            ADD CONSTRAINT realized_pnl_events_pkey            PRIMARY KEY (id);
ALTER TABLE supported_display_currencies   ADD CONSTRAINT supported_display_currencies_pkey   PRIMARY KEY (currency);
ALTER TABLE sync_errors                    ADD CONSTRAINT sync_errors_pkey                    PRIMARY KEY (id);
ALTER TABLE trades                         ADD CONSTRAINT trades_pkey                         PRIMARY KEY (id);

-- Unique constraints (only those declared as constraints in pg_constraint)
ALTER TABLE broker_accounts        ADD CONSTRAINT broker_accounts_broker_account_identifier_portfolio_id_key  UNIQUE (broker, account_identifier, portfolio_id);
ALTER TABLE fx_rates               ADD CONSTRAINT fx_rates_rate_date_from_currency_to_currency_key           UNIQUE (rate_date, from_currency, to_currency);
ALTER TABLE instruments            ADD CONSTRAINT instruments_symbol_key                                     UNIQUE (symbol);
ALTER TABLE market_prices          ADD CONSTRAINT market_prices_price_date_symbol_source_key                 UNIQUE (price_date, symbol, source);
ALTER TABLE portfolio_cash         ADD CONSTRAINT portfolio_cash_portfolio_id_broker_currency_key            UNIQUE (portfolio_id, broker, currency);
ALTER TABLE portfolio_daily_snapshots ADD CONSTRAINT portfolio_daily_snapshots_portfolio_id_snapshot_date_key UNIQUE (portfolio_id, snapshot_date);
ALTER TABLE portfolio_nav_snapshots  ADD CONSTRAINT portfolio_nav_snapshots_portfolio_id_broker_snapshot_date_c_key UNIQUE (portfolio_id, broker, snapshot_date, currency);
ALTER TABLE portfolios             ADD CONSTRAINT portfolios_name_unique                                     UNIQUE (name);
ALTER TABLE realized_pnl_events    ADD CONSTRAINT realized_pnl_events_broker_external_id_key                 UNIQUE (broker, external_id);

-- Foreign Keys (all point at portfolios.id; emitted after all tables exist)
ALTER TABLE broker_accounts            ADD CONSTRAINT broker_accounts_portfolio_id_fkey            FOREIGN KEY (portfolio_id) REFERENCES portfolios(id);
ALTER TABLE closed_trades              ADD CONSTRAINT closed_trades_portfolio_id_fkey              FOREIGN KEY (portfolio_id) REFERENCES portfolios(id) ON DELETE SET NULL;
ALTER TABLE import_jobs                ADD CONSTRAINT import_jobs_portfolio_id_fkey                FOREIGN KEY (portfolio_id) REFERENCES portfolios(id) ON DELETE SET NULL;
ALTER TABLE portfolio_cash             ADD CONSTRAINT portfolio_cash_portfolio_id_fkey             FOREIGN KEY (portfolio_id) REFERENCES portfolios(id);
ALTER TABLE portfolio_cashflows        ADD CONSTRAINT portfolio_cashflows_portfolio_id_fkey        FOREIGN KEY (portfolio_id) REFERENCES portfolios(id) ON DELETE CASCADE;
ALTER TABLE portfolio_daily_snapshots  ADD CONSTRAINT portfolio_daily_snapshots_portfolio_id_fkey  FOREIGN KEY (portfolio_id) REFERENCES portfolios(id) ON DELETE CASCADE;
ALTER TABLE portfolio_nav_snapshots    ADD CONSTRAINT portfolio_nav_snapshots_portfolio_id_fkey    FOREIGN KEY (portfolio_id) REFERENCES portfolios(id);
ALTER TABLE positions                  ADD CONSTRAINT positions_portfolio_id_fkey                  FOREIGN KEY (portfolio_id) REFERENCES portfolios(id) ON DELETE SET NULL;
ALTER TABLE realized_pnl_events        ADD CONSTRAINT realized_pnl_events_portfolio_id_fkey        FOREIGN KEY (portfolio_id) REFERENCES portfolios(id);
ALTER TABLE sync_errors                ADD CONSTRAINT sync_errors_portfolio_id_fkey                FOREIGN KEY (portfolio_id) REFERENCES portfolios(id) ON DELETE SET NULL;
ALTER TABLE trades                     ADD CONSTRAINT trades_portfolio_id_fkey                     FOREIGN KEY (portfolio_id) REFERENCES portfolios(id) ON DELETE SET NULL;


-- =====================================================================
-- INDEXES (standalone, not backing constraints)
-- =====================================================================
CREATE UNIQUE INDEX IF NOT EXISTS broker_accounts_unique_mapping_idx     ON broker_accounts     USING btree (broker, account_identifier, portfolio_id);
CREATE UNIQUE INDEX IF NOT EXISTS portfolio_cash_unique_idx              ON portfolio_cash      USING btree (portfolio_id, broker, currency);
CREATE        INDEX IF NOT EXISTS portfolio_cashflows_broker_idx         ON portfolio_cashflows USING btree (broker);
CREATE        INDEX IF NOT EXISTS portfolio_cashflows_portfolio_date_idx ON portfolio_cashflows USING btree (portfolio_id, cashflow_date);
CREATE UNIQUE INDEX IF NOT EXISTS portfolio_cashflows_unique_external    ON portfolio_cashflows USING btree (portfolio_id, broker, source, external_id);
CREATE UNIQUE INDEX IF NOT EXISTS portfolio_nav_snapshots_unique_idx     ON portfolio_nav_snapshots USING btree (portfolio_id, broker, snapshot_date, currency);
CREATE UNIQUE INDEX IF NOT EXISTS positions_unique_idx                   ON positions           USING btree (portfolio_id, broker, symbol);
CREATE UNIQUE INDEX IF NOT EXISTS realized_pnl_events_unique_idx         ON realized_pnl_events USING btree (broker, external_id) WHERE (external_id IS NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS trades_broker_external_trade_id_unique ON trades              USING btree (broker, external_trade_id);


-- =====================================================================
-- ROW LEVEL SECURITY — enabled on every table, no policies.
-- Backend uses service_role (BYPASSRLS); anon/authenticated have no access.
-- =====================================================================
ALTER TABLE broker_accounts              ENABLE ROW LEVEL SECURITY;
ALTER TABLE closed_trades                ENABLE ROW LEVEL SECURITY;
ALTER TABLE fx_rates                     ENABLE ROW LEVEL SECURITY;
ALTER TABLE import_jobs                  ENABLE ROW LEVEL SECURITY;
ALTER TABLE instruments                  ENABLE ROW LEVEL SECURITY;
ALTER TABLE market_prices                ENABLE ROW LEVEL SECURITY;
ALTER TABLE portfolio_cash               ENABLE ROW LEVEL SECURITY;
ALTER TABLE portfolio_cashflows          ENABLE ROW LEVEL SECURITY;
ALTER TABLE portfolio_daily_snapshots    ENABLE ROW LEVEL SECURITY;
ALTER TABLE portfolio_nav_snapshots      ENABLE ROW LEVEL SECURITY;
ALTER TABLE portfolios                   ENABLE ROW LEVEL SECURITY;
ALTER TABLE positions                    ENABLE ROW LEVEL SECURITY;
ALTER TABLE realized_pnl_events          ENABLE ROW LEVEL SECURITY;
ALTER TABLE supported_display_currencies ENABLE ROW LEVEL SECURITY;
ALTER TABLE sync_errors                  ENABLE ROW LEVEL SECURITY;
ALTER TABLE trades                       ENABLE ROW LEVEL SECURITY;


-- =====================================================================
-- FUNCTIONS (2)
-- =====================================================================

CREATE OR REPLACE FUNCTION public.rls_auto_enable()
 RETURNS event_trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE
  cmd record;
BEGIN
  FOR cmd IN
    SELECT *
    FROM pg_event_trigger_ddl_commands()
    WHERE command_tag IN ('CREATE TABLE', 'CREATE TABLE AS', 'SELECT INTO')
      AND object_type IN ('table','partitioned table')
  LOOP
     IF cmd.schema_name IS NOT NULL AND cmd.schema_name IN ('public') AND cmd.schema_name NOT IN ('pg_catalog','information_schema') AND cmd.schema_name NOT LIKE 'pg_toast%' AND cmd.schema_name NOT LIKE 'pg_temp%' THEN
      BEGIN
        EXECUTE format('alter table if exists %s enable row level security', cmd.object_identity);
        RAISE LOG 'rls_auto_enable: enabled RLS on %', cmd.object_identity;
      EXCEPTION
        WHEN OTHERS THEN
          RAISE LOG 'rls_auto_enable: failed to enable RLS on %', cmd.object_identity;
      END;
     ELSE
        RAISE LOG 'rls_auto_enable: skip % (either system schema or not in enforced list: %.)', cmd.object_identity, cmd.schema_name;
     END IF;
  END LOOP;
END;
$function$;

CREATE OR REPLACE FUNCTION public.get_trades_enriched(display_currency text DEFAULT NULL::text)
 RETURNS TABLE(trade_id uuid, portfolio_name text, portfolio_base_currency text, display_currency text, broker text, symbol text, asset_class text, instrument_type text, side text, quantity numeric, price numeric, trade_currency text, fee numeric, trade_timestamp timestamp with time zone, signed_quantity numeric, gross_value_native numeric, fx_rate_to_display numeric, gross_value_display numeric, fee_display numeric)
 LANGUAGE sql
 STABLE
AS $function$
    SELECT
        t.id AS trade_id,
        p.name AS portfolio_name,
        p.base_currency AS portfolio_base_currency,
        COALESCE(display_currency, p.base_currency) AS display_currency,
        t.broker,
        t.symbol,
        t.asset_class,
        t.instrument_type,
        t.side,
        t.quantity,
        t.price,
        t.currency AS trade_currency,
        t.fee,
        COALESCE(t.trade_date, t.execution_time) AS trade_timestamp,
        CASE
            WHEN UPPER(t.side) = 'BUY' THEN t.quantity
            WHEN UPPER(t.side) = 'SELL' THEN -t.quantity
            ELSE t.quantity
        END AS signed_quantity,
        ABS(t.quantity * t.price) AS gross_value_native,
        CASE
            WHEN t.currency = COALESCE(display_currency, p.base_currency) THEN 1
            ELSE fx.rate
        END AS fx_rate_to_display,
        CASE
            WHEN t.currency = COALESCE(display_currency, p.base_currency) THEN ABS(t.quantity * t.price)
            WHEN fx.rate IS NOT NULL THEN ABS(t.quantity * t.price) * fx.rate
            ELSE NULL
        END AS gross_value_display,
        CASE
            WHEN t.currency = COALESCE(display_currency, p.base_currency) THEN t.fee
            WHEN fx.rate IS NOT NULL THEN t.fee * fx.rate
            ELSE NULL
        END AS fee_display
    FROM public.trades t
    JOIN public.portfolios p ON p.id = t.portfolio_id
    LEFT JOIN LATERAL (
        SELECT fr.rate
        FROM public.fx_rates fr
        WHERE fr.from_currency = t.currency
          AND fr.to_currency = COALESCE(display_currency, p.base_currency)
          AND fr.rate_date <= COALESCE(t.trade_date::date, t.execution_time::date, CURRENT_DATE)
        ORDER BY fr.rate_date DESC
        LIMIT 1
    ) fx ON TRUE;
$function$;


-- =====================================================================
-- EVENT TRIGGER — auto-enables RLS on any new table in public
-- =====================================================================
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = 'ensure_rls') THEN
    CREATE EVENT TRIGGER ensure_rls
      ON ddl_command_end
      WHEN TAG IN ('CREATE TABLE', 'CREATE TABLE AS', 'SELECT INTO')
      EXECUTE FUNCTION public.rls_auto_enable();
  END IF;
END $$;


-- =====================================================================
-- VIEWS (24) — emitted in topological order
-- Level 0: depend only on tables
-- Level 1: depend on Level-0 views
-- =====================================================================

-- ----- Level 0 ------------------------------------------------------------

CREATE OR REPLACE VIEW v_broker_accounts_public AS
 SELECT ba.id,
    ba.broker,
    ba.account_name,
    ba.account_identifier,
    ba.source_key,
    ba.base_currency,
    ba.sync_enabled,
    ba.is_active,
    ba.notes,
    p.name AS portfolio_name,
    p.base_currency AS portfolio_base_currency,
    ba.created_at,
    ba.updated_at
   FROM broker_accounts ba
     JOIN portfolios p ON p.id = ba.portfolio_id
  ORDER BY ba.broker, p.name, ba.account_name;

CREATE OR REPLACE VIEW v_closed_positions_detail_public AS
 SELECT p.name AS portfolio_name,
    p.base_currency,
    r.broker,
    r.symbol,
    r.asset_class,
    COALESCE(r.raw_payload ->> 'position_id'::text, r.raw_payload ->> 'ticket'::text, r.external_id) AS closed_position_id,
    'deal_detail'::text AS detail_level,
    NULL::timestamp with time zone AS entry_date,
    NULL::numeric AS entry_price,
    r.event_time AS exit_date,
    (r.raw_payload ->> 'price'::text)::numeric AS exit_price,
    (r.raw_payload ->> 'volume'::text)::numeric AS quantity,
    r.realized_pnl AS closed_pnl_native,
    r.realized_pnl_base AS closed_pnl_base,
    r.currency,
    COALESCE((r.raw_payload ->> 'commission'::text)::numeric, 0::numeric) AS commission,
    COALESCE((r.raw_payload ->> 'swap'::text)::numeric, 0::numeric) AS swap,
        CASE
            WHEN lower(COALESCE(r.raw_payload ->> 'comment'::text, ''::text)) ~~ '%[sl%'::text THEN 'SL'::text
            WHEN lower(COALESCE(r.raw_payload ->> 'comment'::text, ''::text)) ~~ '%[tp%'::text THEN 'TP'::text
            ELSE NULL::text
        END AS close_reason,
    r.raw_payload ->> 'comment'::text AS close_comment,
    r.event_time AS first_event_time,
    r.event_time AS last_event_time,
    1 AS event_count,
    r.raw_payload
   FROM realized_pnl_events r
     JOIN portfolios p ON p.id = r.portfolio_id
  WHERE r.broker = 'MT5'::text
UNION ALL
 SELECT p.name AS portfolio_name,
    p.base_currency,
    r.broker,
    r.symbol,
    r.asset_class,
    COALESCE(r.raw_payload ->> 'conid'::text, r.symbol, r.external_id) AS closed_position_id,
    'symbol_summary'::text AS detail_level,
    NULL::timestamp with time zone AS entry_date,
    NULL::numeric AS entry_price,
    r.event_time AS exit_date,
    NULL::numeric AS exit_price,
    NULL::numeric AS quantity,
    r.realized_pnl AS closed_pnl_native,
    r.realized_pnl_base AS closed_pnl_base,
    r.currency,
    NULL::numeric AS commission,
    NULL::numeric AS swap,
    NULL::text AS close_reason,
    r.raw_payload ->> 'description'::text AS close_comment,
    r.event_time AS first_event_time,
    r.event_time AS last_event_time,
    1 AS event_count,
    r.raw_payload
   FROM realized_pnl_events r
     JOIN portfolios p ON p.id = r.portfolio_id
  WHERE r.broker = 'IBKR'::text AND (COALESCE(r.symbol, ''::text) <> ALL (ARRAY['UNKNOWN'::text, ''::text])) AND COALESCE(r.raw_payload ->> 'description'::text, ''::text) <> 'Total (All Assets)'::text;

CREATE OR REPLACE VIEW v_closed_positions_public AS
 SELECT p.name AS portfolio_name,
    p.base_currency,
    r.broker,
    r.symbol,
    COALESCE(max(r.asset_class), 'Unknown'::text) AS asset_class,
    count(*) AS event_count,
    sum(r.realized_pnl) AS closed_pnl_native,
    sum(r.realized_pnl_base) AS closed_pnl_base,
    max(r.currency) AS currency,
    min(r.event_time) AS first_event_time,
    max(r.event_time) AS last_event_time
   FROM realized_pnl_events r
     JOIN portfolios p ON p.id = r.portfolio_id
  GROUP BY p.name, p.base_currency, r.broker, r.symbol
  ORDER BY p.name, (sum(r.realized_pnl_base)) DESC NULLS LAST;

CREATE OR REPLACE VIEW v_nav_latest_public AS
 SELECT DISTINCT ON (p.name, s.broker) p.name AS portfolio_name,
    p.base_currency,
    s.broker,
    s.snapshot_date,
    s.currency,
    s.nav,
    s.cash,
    s.market_value,
    s.open_pnl,
    s.closed_pnl,
    s.deposits_withdrawals,
    s.source,
    s.created_at
   FROM portfolio_nav_snapshots s
     JOIN portfolios p ON p.id = s.portfolio_id
  ORDER BY p.name, s.broker, s.snapshot_date DESC, s.created_at DESC;

CREATE OR REPLACE VIEW v_portfolio_cashflows_public AS
 SELECT p.name AS portfolio_name,
    p.base_currency,
    c.broker,
    c.cashflow_date,
    c.currency,
    c.amount_native,
    c.amount_base,
    c.cashflow_type,
    c.source,
    c.external_id,
    c.created_at
   FROM portfolio_cashflows c
     JOIN portfolios p ON p.id = c.portfolio_id;

CREATE OR REPLACE VIEW v_portfolio_daily_nav AS
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
                    ELSE s.nav
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

CREATE OR REPLACE VIEW v_portfolio_nav_snapshots_base AS
 SELECT p.name AS portfolio_name,
    p.base_currency,
    s.broker,
    s.snapshot_date,
    s.currency AS native_currency,
    COALESCE(fx_before.rate, fx_latest.rate,
        CASE
            WHEN s.currency = p.base_currency THEN 1
            ELSE NULL::integer
        END::numeric) AS fx_rate_to_base,
    s.nav AS nav_native,
        CASE
            WHEN s.currency = p.base_currency THEN s.nav
            ELSE s.nav * COALESCE(fx_before.rate, fx_latest.rate)
        END AS nav_base,
    s.cash AS cash_native,
        CASE
            WHEN s.currency = p.base_currency THEN s.cash
            ELSE s.cash * COALESCE(fx_before.rate, fx_latest.rate)
        END AS cash_base,
    s.market_value AS market_value_native,
        CASE
            WHEN s.currency = p.base_currency THEN s.market_value
            ELSE s.market_value * COALESCE(fx_before.rate, fx_latest.rate)
        END AS market_value_base,
    s.open_pnl AS open_pnl_native,
        CASE
            WHEN s.currency = p.base_currency THEN s.open_pnl
            ELSE s.open_pnl * COALESCE(fx_before.rate, fx_latest.rate)
        END AS open_pnl_base,
    s.closed_pnl AS closed_pnl_native,
        CASE
            WHEN s.currency = p.base_currency THEN s.closed_pnl
            ELSE s.closed_pnl * COALESCE(fx_before.rate, fx_latest.rate)
        END AS closed_pnl_base,
    s.source,
    s.created_at
   FROM portfolio_nav_snapshots s
     JOIN portfolios p ON p.id = s.portfolio_id
     LEFT JOIN LATERAL ( SELECT fx.rate
           FROM fx_rates fx
          WHERE fx.from_currency = s.currency AND fx.to_currency = p.base_currency AND fx.rate_date <= s.snapshot_date
          ORDER BY fx.rate_date DESC
         LIMIT 1) fx_before ON true
     LEFT JOIN LATERAL ( SELECT fx.rate
           FROM fx_rates fx
          WHERE fx.from_currency = s.currency AND fx.to_currency = p.base_currency
          ORDER BY fx.rate_date DESC
         LIMIT 1) fx_latest ON true;

CREATE OR REPLACE VIEW v_positions_public AS
 SELECT p.name AS portfolio_name,
    p.base_currency,
    pos.broker,
    pos.symbol,
    pos.asset_class,
    pos.quantity,
    pos.avg_cost,
    pos.currency,
    pos.market_price,
    pos.market_value_native,
    pos.market_value_base,
    pos.open_pnl_native,
    pos.open_pnl_base,
    pos.updated_at
   FROM positions pos
     JOIN portfolios p ON p.id = pos.portfolio_id;

CREATE OR REPLACE VIEW v_positions_public_base AS
 WITH latest_fx AS (
         SELECT DISTINCT ON (fx_rates.from_currency, fx_rates.to_currency) fx_rates.from_currency,
            fx_rates.to_currency,
            fx_rates.rate,
            fx_rates.rate_date
           FROM fx_rates
          ORDER BY fx_rates.from_currency, fx_rates.to_currency, fx_rates.rate_date DESC
        )
 SELECT p.name AS portfolio_name,
    p.base_currency,
    pos.broker,
    pos.symbol,
    pos.asset_class,
    pos.quantity,
    pos.avg_cost,
    pos.currency,
    pos.market_price,
    pos.market_value_native,
        CASE
            WHEN pos.currency = p.base_currency THEN pos.market_value_native
            WHEN pos.broker = 'Bitget'::text AND fx.rate IS NOT NULL THEN pos.market_value_native * fx.rate
            ELSE pos.market_value_base
        END AS market_value_base,
    pos.open_pnl_native,
        CASE
            WHEN pos.currency = p.base_currency THEN pos.open_pnl_native
            WHEN pos.broker = 'Bitget'::text AND fx.rate IS NOT NULL THEN pos.open_pnl_native * fx.rate
            ELSE pos.open_pnl_base
        END AS open_pnl_base,
        CASE
            WHEN pos.currency = p.base_currency THEN 1::numeric
            WHEN pos.broker = 'Bitget'::text THEN fx.rate
            ELSE NULL::numeric
        END AS fx_rate_to_base,
    pos.entry_date,
    pos.position_side,
    pos.take_profit,
    pos.stop_loss,
    pos.take_profit_order_id,
    pos.stop_loss_order_id,
    pos.source_position_id,
    pos.updated_at,
    pos.take_profit_orders,
    pos.stop_loss_orders
   FROM positions pos
     JOIN portfolios p ON p.id = pos.portfolio_id
     LEFT JOIN latest_fx fx ON fx.from_currency = pos.currency AND fx.to_currency = p.base_currency;

CREATE OR REPLACE VIEW v_sync_status AS
 SELECT DISTINCT ON (broker, portfolio_name) broker,
    portfolio_name,
    status,
    started_at,
    finished_at,
    COALESCE(rows_seen, 0) AS rows_seen,
    COALESCE(rows_inserted, 0) AS rows_inserted,
    COALESCE(rows_updated, 0) AS rows_updated,
    error_message,
    COALESCE(metadata, '{}'::jsonb) AS metadata
   FROM import_jobs
  ORDER BY broker, portfolio_name, started_at DESC;

CREATE OR REPLACE VIEW v_trades_enriched_base AS
 SELECT t.id,
    p.name AS portfolio_name,
    p.base_currency,
    t.broker,
    t.symbol,
    t.asset_class,
    t.instrument_type,
    t.side,
    t.quantity,
    t.price,
    t.currency,
    t.fee,
    COALESCE(t.trade_date, t.execution_time) AS trade_timestamp,
        CASE
            WHEN upper(t.side) = 'BUY'::text THEN t.quantity
            WHEN upper(t.side) = 'SELL'::text THEN - t.quantity
            ELSE t.quantity
        END AS signed_quantity,
    abs(t.quantity * t.price) AS gross_value_native,
        CASE
            WHEN t.currency = p.base_currency THEN 1::numeric
            ELSE fx.rate
        END AS fx_rate_to_base,
        CASE
            WHEN t.currency = p.base_currency THEN abs(t.quantity * t.price)
            WHEN fx.rate IS NOT NULL THEN abs(t.quantity * t.price) * fx.rate
            ELSE NULL::numeric
        END AS gross_value_base,
        CASE
            WHEN t.currency = p.base_currency THEN t.fee
            WHEN fx.rate IS NOT NULL THEN t.fee * fx.rate
            ELSE NULL::numeric
        END AS fee_base
   FROM trades t
     JOIN portfolios p ON p.id = t.portfolio_id
     LEFT JOIN LATERAL ( SELECT fr.rate
           FROM fx_rates fr
          WHERE fr.from_currency = t.currency AND fr.to_currency = p.base_currency AND fr.rate_date <= COALESCE(t.trade_date::date, t.execution_time::date, CURRENT_DATE)
          ORDER BY fr.rate_date DESC
         LIMIT 1) fx ON true;

-- ----- Level 1 ------------------------------------------------------------

CREATE OR REPLACE VIEW v_allocation_asset_class_base AS
 SELECT portfolio_name,
    base_currency,
    asset_class,
    count(*) AS trade_count,
    round(sum(gross_value_base), 2) AS gross_trade_volume_base,
    round(sum(gross_value_base) / NULLIF(sum(sum(gross_value_base)) OVER (PARTITION BY portfolio_name), 0::numeric) * 100::numeric, 2) AS allocation_percent
   FROM v_trades_enriched_base
  GROUP BY portfolio_name, base_currency, asset_class;

CREATE OR REPLACE VIEW v_allocation_broker_base AS
 SELECT portfolio_name,
    base_currency,
    broker,
    count(*) AS trade_count,
    round(sum(gross_value_base), 2) AS gross_trade_volume_base,
    round(sum(gross_value_base) / NULLIF(sum(sum(gross_value_base)) OVER (PARTITION BY portfolio_name), 0::numeric) * 100::numeric, 2) AS allocation_percent
   FROM v_trades_enriched_base
  GROUP BY portfolio_name, base_currency, broker;

CREATE OR REPLACE VIEW v_allocation_currency_base AS
 SELECT portfolio_name,
    base_currency,
    currency AS trade_currency,
    count(*) AS trade_count,
    round(sum(gross_value_base), 2) AS gross_trade_volume_base,
    round(sum(gross_value_base) / NULLIF(sum(sum(gross_value_base)) OVER (PARTITION BY portfolio_name), 0::numeric) * 100::numeric, 2) AS allocation_percent
   FROM v_trades_enriched_base
  GROUP BY portfolio_name, base_currency, currency;

CREATE OR REPLACE VIEW v_currency_exposure_missing_fx AS
 SELECT portfolio_name,
    base_currency,
    currency AS trade_currency,
    count(*) AS affected_trades,
    min(trade_timestamp) AS first_trade_at,
    max(trade_timestamp) AS last_trade_at
   FROM v_trades_enriched_base
  WHERE fx_rate_to_base IS NULL
  GROUP BY portfolio_name, base_currency, currency
  ORDER BY portfolio_name, currency;

CREATE OR REPLACE VIEW v_portfolio_overview_base AS
 SELECT portfolio_name,
    base_currency,
    count(*) AS trade_count,
    count(DISTINCT symbol) AS instrument_count,
    count(DISTINCT broker) AS broker_count,
    round(sum(gross_value_base), 2) AS gross_trade_volume_base,
    round(sum(COALESCE(fee_base, 0::numeric)), 2) AS total_fees_base,
    min(trade_timestamp) AS first_trade_at,
    max(trade_timestamp) AS last_trade_at
   FROM v_trades_enriched_base
  GROUP BY portfolio_name, base_currency;

CREATE OR REPLACE VIEW v_portfolio_performance_public AS
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
    n.current_nav - COALESCE(c.net_contributions, 0::numeric) AS net_profit_after_flows
   FROM latest_nav n
     LEFT JOIN cashflow_totals c ON c.portfolio_id = n.portfolio_id
     LEFT JOIN twr_by_portfolio t ON t.portfolio_id = n.portfolio_id;

CREATE OR REPLACE VIEW v_portfolio_trade_summary_base AS
 SELECT portfolio_name,
    base_currency,
    broker,
    asset_class,
    count(*) AS trade_count,
    sum(gross_value_base) AS gross_trade_volume_base,
    sum(fee_base) AS total_fees_base,
    min(trade_timestamp) AS first_trade_at,
    max(trade_timestamp) AS last_trade_at
   FROM v_trades_enriched_base
  GROUP BY portfolio_name, base_currency, broker, asset_class;

CREATE OR REPLACE VIEW v_position_allocation_asset_class AS
 WITH totals AS (
         SELECT v_positions_public_base.portfolio_name,
            v_positions_public_base.base_currency,
            sum(abs(v_positions_public_base.market_value_base)) AS total_market_value_base
           FROM v_positions_public_base
          GROUP BY v_positions_public_base.portfolio_name, v_positions_public_base.base_currency
        )
 SELECT pos.portfolio_name,
    pos.base_currency,
    pos.asset_class,
    count(*) AS position_count,
    sum(abs(pos.market_value_base)) AS market_value_base,
        CASE
            WHEN t.total_market_value_base > 0::numeric THEN round(sum(abs(pos.market_value_base)) / t.total_market_value_base * 100::numeric, 2)
            ELSE 0::numeric
        END AS allocation_percent
   FROM v_positions_public_base pos
     JOIN totals t ON t.portfolio_name = pos.portfolio_name
  GROUP BY pos.portfolio_name, pos.base_currency, pos.asset_class, t.total_market_value_base
  ORDER BY pos.portfolio_name, (
        CASE
            WHEN t.total_market_value_base > 0::numeric THEN round(sum(abs(pos.market_value_base)) / t.total_market_value_base * 100::numeric, 2)
            ELSE 0::numeric
        END) DESC;

CREATE OR REPLACE VIEW v_position_allocation_broker AS
 WITH totals AS (
         SELECT v_positions_public_base.portfolio_name,
            v_positions_public_base.base_currency,
            sum(abs(v_positions_public_base.market_value_base)) AS total_market_value_base
           FROM v_positions_public_base
          GROUP BY v_positions_public_base.portfolio_name, v_positions_public_base.base_currency
        )
 SELECT pos.portfolio_name,
    pos.base_currency,
    pos.broker,
    count(*) AS position_count,
    sum(abs(pos.market_value_base)) AS market_value_base,
        CASE
            WHEN t.total_market_value_base > 0::numeric THEN round(sum(abs(pos.market_value_base)) / t.total_market_value_base * 100::numeric, 2)
            ELSE 0::numeric
        END AS allocation_percent
   FROM v_positions_public_base pos
     JOIN totals t ON t.portfolio_name = pos.portfolio_name
  GROUP BY pos.portfolio_name, pos.base_currency, pos.broker, t.total_market_value_base
  ORDER BY pos.portfolio_name, (
        CASE
            WHEN t.total_market_value_base > 0::numeric THEN round(sum(abs(pos.market_value_base)) / t.total_market_value_base * 100::numeric, 2)
            ELSE 0::numeric
        END) DESC;

CREATE OR REPLACE VIEW v_position_allocation_currency AS
 WITH totals AS (
         SELECT v_positions_public_base.portfolio_name,
            v_positions_public_base.base_currency,
            sum(abs(v_positions_public_base.market_value_base)) AS total_market_value_base
           FROM v_positions_public_base
          GROUP BY v_positions_public_base.portfolio_name, v_positions_public_base.base_currency
        )
 SELECT pos.portfolio_name,
    pos.base_currency,
    pos.currency,
    count(*) AS position_count,
    sum(abs(pos.market_value_base)) AS market_value_base,
        CASE
            WHEN t.total_market_value_base > 0::numeric THEN round(sum(abs(pos.market_value_base)) / t.total_market_value_base * 100::numeric, 2)
            ELSE 0::numeric
        END AS allocation_percent
   FROM v_positions_public_base pos
     JOIN totals t ON t.portfolio_name = pos.portfolio_name
  GROUP BY pos.portfolio_name, pos.base_currency, pos.currency, t.total_market_value_base
  ORDER BY pos.portfolio_name, (
        CASE
            WHEN t.total_market_value_base > 0::numeric THEN round(sum(abs(pos.market_value_base)) / t.total_market_value_base * 100::numeric, 2)
            ELSE 0::numeric
        END) DESC;

CREATE OR REPLACE VIEW v_position_allocation_symbol AS
 WITH totals AS (
         SELECT v_positions_public_base.portfolio_name,
            v_positions_public_base.base_currency,
            sum(abs(v_positions_public_base.market_value_base)) AS total_market_value_base
           FROM v_positions_public_base
          GROUP BY v_positions_public_base.portfolio_name, v_positions_public_base.base_currency
        )
 SELECT pos.portfolio_name,
    pos.base_currency,
    pos.broker,
    pos.symbol,
    pos.asset_class,
    pos.currency,
    pos.quantity,
    pos.market_value_base,
    pos.open_pnl_base,
    pos.entry_date,
    pos.position_side,
    pos.take_profit,
    pos.stop_loss,
        CASE
            WHEN t.total_market_value_base > 0::numeric THEN round(abs(pos.market_value_base) / t.total_market_value_base * 100::numeric, 2)
            ELSE 0::numeric
        END AS allocation_percent
   FROM v_positions_public_base pos
     JOIN totals t ON t.portfolio_name = pos.portfolio_name
  ORDER BY pos.portfolio_name, (
        CASE
            WHEN t.total_market_value_base > 0::numeric THEN round(abs(pos.market_value_base) / t.total_market_value_base * 100::numeric, 2)
            ELSE 0::numeric
        END) DESC;

CREATE OR REPLACE VIEW v_public_portfolio_summary AS
 WITH trade_counts AS (
         SELECT p_1.id AS portfolio_id,
            count(t.id) AS trade_count,
            max(t.execution_time) AS last_trade_at
           FROM portfolios p_1
             LEFT JOIN trades t ON t.portfolio_id = p_1.id
          GROUP BY p_1.id
        ), latest_nav_per_broker AS (
         SELECT DISTINCT ON (v_portfolio_nav_snapshots_base.portfolio_name, v_portfolio_nav_snapshots_base.broker) v_portfolio_nav_snapshots_base.portfolio_name,
            v_portfolio_nav_snapshots_base.base_currency,
            v_portfolio_nav_snapshots_base.broker,
            v_portfolio_nav_snapshots_base.snapshot_date,
            v_portfolio_nav_snapshots_base.native_currency,
            v_portfolio_nav_snapshots_base.fx_rate_to_base,
            v_portfolio_nav_snapshots_base.nav_base,
            v_portfolio_nav_snapshots_base.cash_base,
            v_portfolio_nav_snapshots_base.market_value_base,
            v_portfolio_nav_snapshots_base.open_pnl_base,
            v_portfolio_nav_snapshots_base.closed_pnl_base,
            v_portfolio_nav_snapshots_base.created_at
           FROM v_portfolio_nav_snapshots_base
          WHERE v_portfolio_nav_snapshots_base.fx_rate_to_base IS NOT NULL
          ORDER BY v_portfolio_nav_snapshots_base.portfolio_name, v_portfolio_nav_snapshots_base.broker, v_portfolio_nav_snapshots_base.snapshot_date DESC, v_portfolio_nav_snapshots_base.created_at DESC
        ), nav_agg AS (
         SELECT latest_nav_per_broker.portfolio_name,
            latest_nav_per_broker.base_currency,
            sum(latest_nav_per_broker.nav_base) AS nav,
            sum(latest_nav_per_broker.cash_base) AS cash,
            sum(latest_nav_per_broker.market_value_base) AS market_value,
            sum(latest_nav_per_broker.open_pnl_base) AS open_pnl,
            sum(latest_nav_per_broker.closed_pnl_base) AS closed_pnl_from_snapshots,
            max(latest_nav_per_broker.created_at) AS nav_updated_at
           FROM latest_nav_per_broker
          GROUP BY latest_nav_per_broker.portfolio_name, latest_nav_per_broker.base_currency
        ), latest_sync AS (
         SELECT import_jobs.portfolio_name,
            max(import_jobs.finished_at) AS last_sync_at
           FROM import_jobs
          WHERE import_jobs.status = 'success'::text
          GROUP BY import_jobs.portfolio_name
        ), realized AS (
         SELECT p_1.name AS portfolio_name,
            sum(r_1.realized_pnl_base) AS closed_pnl_base
           FROM realized_pnl_events r_1
             JOIN portfolios p_1 ON p_1.id = r_1.portfolio_id
          GROUP BY p_1.name
        )
 SELECT p.name AS portfolio_name,
    p.base_currency,
    p.base_currency AS nav_currency,
    na.nav,
    na.cash,
    na.market_value,
    na.open_pnl,
    COALESCE(na.closed_pnl_from_snapshots, r.closed_pnl_base) AS closed_pnl,
    COALESCE(na.open_pnl, 0::numeric) + COALESCE(na.closed_pnl_from_snapshots, r.closed_pnl_base, 0::numeric) AS total_pnl,
        CASE
            WHEN na.nav IS NOT NULL THEN true
            ELSE false
        END AS has_nav,
        CASE
            WHEN na.nav IS NOT NULL THEN 'Live'::text
            ELSE 'Waiting for NAV'::text
        END AS status_label,
    tc.trade_count,
    tc.last_trade_at,
    ls.last_sync_at,
    na.nav_updated_at
   FROM portfolios p
     LEFT JOIN trade_counts tc ON tc.portfolio_id = p.id
     LEFT JOIN nav_agg na ON na.portfolio_name = p.name
     LEFT JOIN latest_sync ls ON ls.portfolio_name = p.name
     LEFT JOIN realized r ON r.portfolio_name = p.name
  ORDER BY p.name;

CREATE OR REPLACE VIEW v_recent_trades_base AS
 SELECT portfolio_name,
    base_currency,
    broker,
    symbol,
    asset_class,
    instrument_type,
    side,
    quantity,
    price,
    currency,
    round(gross_value_native, 2) AS gross_value_native,
    round(gross_value_base, 2) AS gross_value_base,
    round(fee_base, 2) AS fee_base,
    trade_timestamp
   FROM v_trades_enriched_base
  ORDER BY trade_timestamp DESC;

-- =====================================================================
-- End of baseline.
-- =====================================================================
