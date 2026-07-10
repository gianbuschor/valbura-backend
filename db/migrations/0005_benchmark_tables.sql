-- =====================================================================
-- Migration 0005 — Benchmark data model (Block 3, DATA DDL only)
--
-- Depends on baseline (db/schema.sql), 0001, 0002, 0003, 0004.
-- Apply order: db/schema.sql -> 0001 -> 0002 -> 0003 -> 0004 -> 0005.
--
-- WHY THIS MIGRATION EXISTS
--   Block 3 compares each portfolio's TWR curve against one or more
--   external benchmarks (indices / ETF proxies / BTC). This migration
--   creates ONLY the data-holding schema + seed:
--     * benchmarks            — the catalog of known benchmarks (what &
--                               where to fetch, native currency, US-exchange
--                               pinning against ticker collisions).
--     * portfolio_benchmarks  — which benchmarks each portfolio is compared
--                               against, with display ordering.
--     * benchmark_prices      — the fetched EOD close history, stored NATIVE
--                               (USD for all sources, incl. BTC vs_currency=usd).
--   The READ MODEL (TWR-series view, get_benchmark_comparison, summary) is
--   deliberately kept OUT of this file and ships separately in 0006 — same
--   split as base views (schema.sql) vs display wrappers (0003/0004).
--
-- CURRENCY CONTRACT
--   Prices are stored in each benchmark's NATIVE currency (all USD here).
--   FX conversion native -> portfolio base happens at READ time in 0006,
--   per price_date, via the existing fx_rates table + the same LATERAL
--   "rate_date <= price_date, else latest" lookup used for NAV/trades.
--   Nothing in this migration converts or rebases — it only stores raw data.
--
-- START-DATE NORMALIZATION (context for 0006, no logic here)
--   Both the portfolio TWR index and each benchmark index are rebased to
--   100 at the SAME anchor day S = MAX(first portfolio NAV date, first
--   benchmark price date). If a benchmark has no close exactly on S
--   (weekend / holiday), 0006 uses carry-forward (last close <= S) as the
--   denominator, so BOTH curves anchor at 100 on exactly the same calendar
--   day — no artificial alpha offset from mismatched start points. This
--   table just needs to hold enough history for that lookback to succeed;
--   the backfill window starts a few days before the first NAV date.
--
-- IDEMPOTENCY
--   CREATE TABLE IF NOT EXISTS + seed INSERTs with ON CONFLICT DO NOTHING.
--   Safe to re-run. schema.sql stays the pure PRE-A3 baseline (untouched).
-- =====================================================================

BEGIN;

-- ---------------------------------------------------------------------
-- 1) benchmarks — catalog: what to fetch, from where, in which currency
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.benchmarks (
    benchmark_key    text        PRIMARY KEY,          -- stable code, e.g. 'SP500'
    display_name     text        NOT NULL,             -- human label for charts/legend
    source           text        NOT NULL,             -- 'fred' | 'coingecko' | 'twelvedata'
    source_symbol    text        NOT NULL,             -- id/symbol at that source
    native_currency  text        NOT NULL DEFAULT 'USD',
    mic_code         text,                             -- US-exchange pin (ARCX) vs ticker collision; NULL for non-exchange sources
    exchange         text,                             -- human-readable exchange hint (e.g. 'NYSE'); NULL where N/A
    instrument_type  text        NOT NULL,             -- 'index' | 'etf' | 'crypto'
    active           boolean     NOT NULL DEFAULT true,
    created_at       timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT benchmarks_source_chk
        CHECK (source IN ('fred', 'coingecko', 'twelvedata')),
    CONSTRAINT benchmarks_instrument_type_chk
        CHECK (instrument_type IN ('index', 'etf', 'crypto'))
);

COMMENT ON TABLE  public.benchmarks IS
    'Catalog of comparison benchmarks (source, symbol, native currency, US-exchange pin). Read model lives in 0006.';
COMMENT ON COLUMN public.benchmarks.mic_code IS
    'US-exchange MIC pin (e.g. ARCX) to disambiguate colliding tickers (DBC/IGE) at Twelve Data. NULL for FRED/CoinGecko.';

-- ---------------------------------------------------------------------
-- 2) portfolio_benchmarks — which benchmarks apply to which portfolio
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.portfolio_benchmarks (
    id            uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    portfolio_id  uuid        NOT NULL
                              REFERENCES public.portfolios(id) ON DELETE CASCADE,
    benchmark_key text        NOT NULL
                              REFERENCES public.benchmarks(benchmark_key) ON DELETE CASCADE,
    sort_order    integer     NOT NULL DEFAULT 0,       -- display order in charts/legend
    active        boolean     NOT NULL DEFAULT true,
    created_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT portfolio_benchmarks_unique UNIQUE (portfolio_id, benchmark_key)
);

CREATE INDEX IF NOT EXISTS portfolio_benchmarks_portfolio_idx
    ON public.portfolio_benchmarks (portfolio_id, sort_order);

COMMENT ON TABLE public.portfolio_benchmarks IS
    'Mapping portfolio -> benchmark(s) with display ordering. Multiple benchmarks per portfolio allowed.';

-- ---------------------------------------------------------------------
-- 3) benchmark_prices — fetched EOD close history, stored NATIVE
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.benchmark_prices (
    id            uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    benchmark_key text        NOT NULL
                              REFERENCES public.benchmarks(benchmark_key) ON DELETE CASCADE,
    price_date    date        NOT NULL,
    close         numeric     NOT NULL,                -- EOD close in native_currency
    currency      text        NOT NULL DEFAULT 'USD',  -- redundant native-currency stamp (audit)
    source        text        NOT NULL,                -- provenance: which source delivered this row
    created_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT benchmark_prices_unique UNIQUE (benchmark_key, price_date)
);

-- newest-first lookback per benchmark (carry-forward "last close <= S" in 0006)
CREATE INDEX IF NOT EXISTS benchmark_prices_key_date_desc_idx
    ON public.benchmark_prices (benchmark_key, price_date DESC);

COMMENT ON TABLE  public.benchmark_prices IS
    'EOD close history per benchmark, stored in NATIVE currency. FX->base happens at read time in 0006.';
COMMENT ON COLUMN public.benchmark_prices.close IS
    'EOD close in the benchmark''s native_currency (USD for all current benchmarks, incl. BTC vs_currency=usd).';

-- =====================================================================
-- SEED — default benchmark catalog + portfolio map
-- =====================================================================

-- 4) Catalog rows -----------------------------------------------------
--    All native_currency USD. FRED = keyless index series; CoinGecko =
--    keyless BTC (vs_currency=usd); Twelve Data = ETF proxies pinned to
--    the US exchange (mic_code ARCX) against ticker collisions.
INSERT INTO public.benchmarks
    (benchmark_key, display_name, source, source_symbol, native_currency, mic_code, exchange, instrument_type)
VALUES
    ('SP500',      'S&P 500',                    'fred',       'SP500',      'USD', NULL,   NULL,   'index'),
    ('DJIA',       'Dow Jones Industrial Avg.',  'fred',       'DJIA',       'USD', NULL,   NULL,   'index'),
    ('MSCI_WORLD', 'MSCI World (URTH)',          'twelvedata', 'URTH',       'USD', 'ARCX', 'NYSE', 'etf'),
    ('COMMODITY',  'Commodity Basket (DBC)',     'twelvedata', 'DBC',        'USD', 'ARCX', 'NYSE', 'etf'),
    ('NATRES',     'Natural Resources (GUNR)',   'twelvedata', 'GUNR',       'USD', 'ARCX', 'NYSE', 'etf'),
    ('BTC',        'Bitcoin',                    'coingecko',  'bitcoin',    'USD', NULL,   NULL,   'crypto')
ON CONFLICT (benchmark_key) DO NOTHING;

-- 5) Portfolio -> benchmark map --------------------------------------
--    Looked up by portfolio name (portfolios_name_unique guarantees a
--    single match). ON CONFLICT DO NOTHING keeps this idempotent.
--    Global (CHF):       S&P 500 (0), MSCI World (1)
--    Day Trading (USD):  Dow Jones (0), S&P 500 (1)
--    Alternatives (CHF): BTC (0), Commodity (1), Natural Resources (2)
INSERT INTO public.portfolio_benchmarks (portfolio_id, benchmark_key, sort_order)
SELECT p.id, m.benchmark_key, m.sort_order
FROM (
    VALUES
        ('Global',       'SP500',      0),
        ('Global',       'MSCI_WORLD', 1),
        ('Day Trading',  'DJIA',       0),
        ('Day Trading',  'SP500',      1),
        ('Alternatives', 'BTC',        0),
        ('Alternatives', 'COMMODITY',  1),
        ('Alternatives', 'NATRES',     2)
) AS m(portfolio_name, benchmark_key, sort_order)
JOIN public.portfolios p ON p.name = m.portfolio_name
ON CONFLICT (portfolio_id, benchmark_key) DO NOTHING;

-- 6) Guard: assert all 7 expected mappings exist ----------------------
--    The JOIN above is silent — a misspelled portfolio name (wrong case,
--    stray space) would just drop that mapping row without error. This
--    block re-counts the expected (portfolio_name, benchmark_key) pairs
--    against what actually landed and ABORTS the whole transaction if it
--    is not exactly 7. Re-run safe: counts existing rows, not new inserts.
DO $$
DECLARE
    v_expected constant integer := 7;
    v_actual   integer;
BEGIN
    SELECT count(*)
      INTO v_actual
      FROM (
          VALUES
              ('Global',       'SP500'),
              ('Global',       'MSCI_WORLD'),
              ('Day Trading',  'DJIA'),
              ('Day Trading',  'SP500'),
              ('Alternatives', 'BTC'),
              ('Alternatives', 'COMMODITY'),
              ('Alternatives', 'NATRES')
      ) AS m(portfolio_name, benchmark_key)
      JOIN public.portfolios p          ON p.name = m.portfolio_name
      JOIN public.portfolio_benchmarks pb
             ON pb.portfolio_id  = p.id
            AND pb.benchmark_key = m.benchmark_key;

    IF v_actual <> v_expected THEN
        RAISE EXCEPTION
            'Benchmark seed guard failed: expected % portfolio_benchmarks mappings, found %. '
            'A portfolio name in the seed does not match public.portfolios.name exactly '
            '(check case/spacing for Global, Day Trading, Alternatives). Transaction rolled back.',
            v_expected, v_actual;
    END IF;

    RAISE NOTICE 'Benchmark seed guard OK: % / % mappings present.', v_actual, v_expected;
END $$;

COMMIT;

-- =====================================================================
-- POST-APPLY VERIFICATION (run manually, read-only; not part of the txn)
-- =====================================================================
-- SELECT benchmark_key, source, source_symbol, mic_code, native_currency
--   FROM public.benchmarks ORDER BY benchmark_key;
--
-- SELECT p.name, pb.benchmark_key, pb.sort_order
--   FROM public.portfolio_benchmarks pb
--   JOIN public.portfolios p ON p.id = pb.portfolio_id
--  ORDER BY p.name, pb.sort_order;
-- Expect 7 mapping rows across the three portfolios.
