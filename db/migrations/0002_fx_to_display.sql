-- =====================================================================
-- Migration 0002 — fx_to_display(): central display-currency multiplier
--
-- Depends on baseline (db/schema.sql) and migration 0001.
-- Apply order: db/schema.sql -> 0001 -> 0002.
--
-- PURELY ADDITIVE & SAFE TO APPLY ANYTIME:
--   * Creates ONE new function. Touches no table, view, or existing
--     function. Nothing calls it yet — it stays inert until the Block-2
--     display wrappers (step 3) invoke it. A dead function has zero
--     runtime effect on the live system.
--   * Idempotent: CREATE OR REPLACE; safe to re-run.
--
-- Role in the currency toggle (Block 2):
--   The toggle pivots every value through CHF (the portfolio base):
--       value_display = value_base (CHF) * fx_to_display(display, date)
--   This function is the SINGLE SOURCE OF TRUTH for that multiplier, so
--   the carry-forward policy is, by construction, identical everywhere.
--
-- Carry-forward policy (the one unified rule):
--   1) display = 'CHF' (or NULL)  -> 1        (base == display, no lookup)
--   2) newest fx_rates row CHF->display with rate_date <= p_date
--                                  -> that rate (carry-forward to date)
--   3) fallback: newest CHF->display row of any date (fx_latest)
--   4) otherwise                  -> NULL     (no rate known)
--
-- NOTE: allowed-currency validation (CHF/EUR/USD only) lives in the API
-- layer (step 4). This function just returns NULL for an unknown target,
-- which is the safe degenerate (callers treat NULL as "not convertible").
-- =====================================================================

SET search_path TO public, extensions;


CREATE OR REPLACE FUNCTION public.fx_to_display(p_display text, p_date date)
 RETURNS numeric
 LANGUAGE sql
 STABLE
AS $function$
    SELECT CASE
        WHEN p_display IS NULL OR upper(p_display) = 'CHF' THEN 1::numeric
        ELSE COALESCE(
            -- (2) carry-forward: newest rate on/before the target date
            ( SELECT fr.rate
                FROM public.fx_rates fr
               WHERE fr.from_currency = 'CHF'
                 AND fr.to_currency = upper(p_display)
                 AND fr.rate_date <= p_date
               ORDER BY fr.rate_date DESC
               LIMIT 1 ),
            -- (3) fallback: newest rate of any date (only matters for the
            --     pre-seed window, which does not exist post go-live)
            ( SELECT fr.rate
                FROM public.fx_rates fr
               WHERE fr.from_currency = 'CHF'
                 AND fr.to_currency = upper(p_display)
               ORDER BY fr.rate_date DESC
               LIMIT 1 )
            -- (4) else COALESCE yields NULL -> not convertible
        )
    END;
$function$;
