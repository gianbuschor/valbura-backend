-- ---------------------------------------------------------------------------
-- 0010_cashflow_base_recompute_usd.sql  —  follow-up to 0009: rebase stored
--                                          cashflow.amount_base to USD
-- ---------------------------------------------------------------------------
-- WHY
--   0009 flipped base_currency CHF->USD for Global + Alternatives. Every VIEW
--   recomputes native->base on the fly, so NAV/positions/trades followed the
--   flip automatically. But portfolio_cashflows.amount_base is a STORED column
--   (written at ingest time, under the old base=CHF), and the base flip does
--   NOT touch stored data. Result after 0009:
--     - CHF cashflows still hold the CHF amount (amount_base == amount_native).
--     - USDT/SOL cashflows still hold amount_base = NULL.
--   This under-states net_contributions / total_deposits / total_withdrawals,
--   skews MWR (xirr over -amount_base), and skews TWR on cashflow days.
--
-- WHAT
--   Recompute amount_base = amount_native * (native -> USD @ cashflow_date) for
--   the cashflows of the two FLIPPED portfolios only. Day Trading (base was
--   always USD, USD cashflows, amount_base already correct) is OUT of scope.
--
-- native -> USD RATE (robust; mirrors the 0009 EUR/GBP/HKD CHF-bridge):
--   1) USD / USDT / USDC  -> 1   (stablecoin identity; date-independent. This is
--      why a 2025-08-18 USDT flow must NOT do a dated fx_rates lookup: the
--      USDT->USD=1 rows are sync-dated (recent), so a carry-forward <= 2025-08-18
--      finds nothing. Treat as 1 directly, like get_fx_rate does.)
--   2) CHF -> USD = 1 / (USD->CHF @<=date). The direct CHF->USD cross only exists
--      since 2026-06-17 (73 rows), but USD->CHF is a full 193-row series since
--      Dec 2025, so the bridge covers every CHF cashflow date. (Direct CHF->USD
--      == 1/(USD->CHF) by construction, so this is consistent where both exist.)
--   3) anything else: COALESCE(direct native->USD, bridge (native->CHF)/(USD->CHF)).
--      Direct wins where present (e.g. SOL->USD from 0009); the CHF bridge is the
--      fallback for any currency whose direct ->USD row is sparse/missing.
--
-- ⚠ DATA MUTATION (not additive), like 0009's UPDATE. Scoped, and IDEMPOTENT:
--   the new value is derived from amount_native (immutable) * a rate, so
--   re-running yields the same result. Safe to re-apply (this file supersedes an
--   earlier version of 0010 that used a bare direct ->USD lookup and left CHF
--   (pre-2026-06-17) and USDT (2025-08-18) rows NULL). Baseline + 0009 untouched.
--
-- RATE COVERAGE (verified): the only cashflow currencies for Global + Alternatives
--   are CHF, USDT, SOL. All resolve at every cashflow date under the rules above.
-- ---------------------------------------------------------------------------

BEGIN;

UPDATE public.portfolio_cashflows pc
   SET amount_base = pc.amount_native * (
         CASE
           -- (1) stablecoins & USD: identity, date-independent
           WHEN pc.currency IN ('USD', 'USDT', 'USDC') THEN 1::numeric
           -- (2) CHF via the full USD->CHF series (bridge; covers pre-2026-06-17)
           WHEN pc.currency = 'CHF' THEN (
                 SELECT 1.0 / r.rate
                   FROM public.fx_rates r
                  WHERE r.from_currency = 'USD' AND r.to_currency = 'CHF'
                    AND r.rate_date <= pc.cashflow_date
                  ORDER BY r.rate_date DESC
                  LIMIT 1 )
           -- (3) everything else: direct ->USD, else CHF-bridge fallback
           ELSE COALESCE(
                 ( SELECT r.rate
                     FROM public.fx_rates r
                    WHERE r.from_currency = pc.currency AND r.to_currency = 'USD'
                      AND r.rate_date <= pc.cashflow_date
                    ORDER BY r.rate_date DESC
                    LIMIT 1 ),
                 ( SELECT nc.rate / uc.rate
                     FROM ( SELECT r.rate FROM public.fx_rates r
                             WHERE r.from_currency = pc.currency AND r.to_currency = 'CHF'
                               AND r.rate_date <= pc.cashflow_date
                             ORDER BY r.rate_date DESC LIMIT 1 ) nc,
                          ( SELECT r.rate FROM public.fx_rates r
                             WHERE r.from_currency = 'USD' AND r.to_currency = 'CHF'
                               AND r.rate_date <= pc.cashflow_date
                             ORDER BY r.rate_date DESC LIMIT 1 ) uc )
                 )
         END)
 WHERE pc.portfolio_id IN (
         SELECT id FROM public.portfolios WHERE name IN ('Global', 'Alternatives')
       );

COMMIT;

-- ---------------------------------------------------------------------------
-- POST-APPLY VERIFICATION (run manually; not part of the transaction)
-- ---------------------------------------------------------------------------
-- (C) NO NULL amount_base remains for the two portfolios -> null_base = 0 for ALL
--     currencies (CHF/USDT/SOL). This is the check that failed on the first pass.
--       SELECT pc.currency, count(*) AS n,
--              count(*) FILTER (WHERE pc.amount_base IS NULL) AS null_base
--         FROM public.portfolio_cashflows pc
--         JOIN public.portfolios p ON p.id = pc.portfolio_id
--        WHERE p.name IN ('Global','Alternatives')
--        GROUP BY pc.currency ORDER BY pc.currency;      -- every null_base = 0
--
-- (D2) CHF rows: implied_rate (amount_base/amount_native) must equal the BRIDGE
--      rate 1/(USD->CHF) at that date, and be present for EVERY CHF date incl.
--      the pre-2026-06-17 ones.
--       SELECT p.name, pc.cashflow_date, pc.amount_native, pc.amount_base,
--              round(pc.amount_base / NULLIF(pc.amount_native,0), 6) AS implied_rate,
--              ( SELECT round(1.0 / r.rate, 6) FROM public.fx_rates r
--                 WHERE r.from_currency='USD' AND r.to_currency='CHF'
--                   AND r.rate_date <= pc.cashflow_date
--                 ORDER BY r.rate_date DESC LIMIT 1 ) AS bridge_chf_usd
--         FROM public.portfolio_cashflows pc
--         JOIN public.portfolios p ON p.id = pc.portfolio_id
--        WHERE p.name IN ('Global','Alternatives') AND pc.currency='CHF'
--        ORDER BY p.name, pc.cashflow_date;   -- implied_rate == bridge_chf_usd
--
-- (D3) USDT/SOL now populated: USDT implied_rate = 1.0, SOL = 182.94.
--       SELECT p.name, pc.cashflow_date, pc.currency, pc.amount_native, pc.amount_base
--         FROM public.portfolio_cashflows pc
--         JOIN public.portfolios p ON p.id = pc.portfolio_id
--        WHERE p.name IN ('Global','Alternatives') AND pc.currency IN ('USDT','SOL')
--        ORDER BY p.name, pc.currency, pc.cashflow_date;
--
-- (B) MWR / contributions of the two flipped portfolios (now in USD, complete):
--       SELECT portfolio_name, net_contributions, total_deposits,
--              total_withdrawals, mwr_period_percent
--         FROM public.v_portfolio_performance_public
--        WHERE portfolio_name IN ('Global','Alternatives')
--        ORDER BY portfolio_name;
--
-- (E) Day Trading UNTOUCHED (out of scope) — perf row byte-identical:
--       SELECT portfolio_name, net_contributions, total_deposits,
--              total_withdrawals, mwr_period_percent
--         FROM public.v_portfolio_performance_public
--        WHERE portfolio_name = 'Day Trading';
-- ---------------------------------------------------------------------------
