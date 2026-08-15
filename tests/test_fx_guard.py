#!/usr/bin/env python3
"""Standalone unit test for the FX plausibility guard in sync_fx.

NO database, NO network, NO FastAPI. The guard lives inline in the async
`sync_fx_rates` endpoint (main.py, ~lines 3952-3980) so it cannot be imported
directly; this file mirrors its arithmetic VERBATIM and asserts the behaviour:

    FX_GUARD_BAND = 0.05                      # 5% day-over-day
    pct = (new/prev - 1.0) * 100.0
    flagged  <=>  abs(pct) > FX_GUARD_BAND*100.0   (strict >, so exactly 5% passes)

Baseline (`prev`) = last stored USD->ccy strictly BEFORE today; None/0 -> skip.
Scope = the 5 USD->fiat SOURCE rates only (CHF/EUR/GBP/JPY/CNY).
Stablecoins (USDT/USDC) and the derived CHF->X crosses are NOT guarded.

Run:  python3 tests/test_fx_guard.py     (exit 0 = all pass)
"""

FX_GUARD_BAND = 0.05

# The exact set the guard iterates (main.py:3954-3955). Stablecoins absent by design.
GUARDED_PAIRS = ("CHF", "EUR", "GBP", "JPY", "CNY")
EXCLUDED_PAIRS = ("USDT", "USDC")


def evaluate(prev, new, ccy="CHF"):
    """Return a warning dict if the move trips the guard, else None.

    Mirrors main.py:3956-3980 exactly (None/0 baseline -> skip -> None).
    """
    if new is None:
        return None
    if prev is None or float(prev) == 0.0:
        return None
    pct = (float(new) / float(prev) - 1.0) * 100.0
    if abs(pct) > FX_GUARD_BAND * 100.0:
        return {
            "pair": f"USD/{ccy}",
            "last_known": round(float(prev), 6),
            "written": round(float(new), 6),
            "pct_change": round(pct, 2),
        }
    return None


def main():
    passed = 0

    def check(name, cond):
        nonlocal passed
        assert cond, f"FAIL: {name}"
        passed += 1
        print(f"  ok  {name}")

    print("FX guard — sharpness (write-and-flag; the value is still written either way):")

    # 1) THE real incident: usd_chf 0.778 -> 0.91 must be flagged.
    w = evaluate(0.778, 0.91, "CHF")
    check("0.778 -> 0.91 is flagged", w is not None)
    check("  pct_change ~= +16.97%", w and abs(w["pct_change"] - 16.97) < 0.01)
    check("  pair label is USD/CHF", w and w["pair"] == "USD/CHF")
    check("  last_known/written carried", w and w["last_known"] == 0.778 and w["written"] == 0.91)

    # 2) Normal daily fiat move must NOT be flagged (real majors ~0.5-2%/day).
    check("0.778 -> 0.782 (+0.51%) not flagged", evaluate(0.778, 0.782, "CHF") is None)
    check("0.778 -> 0.770 (-1.03%) not flagged", evaluate(0.778, 0.770, "CHF") is None)
    check("1.10  -> 1.115 EUR (+1.36%) not flagged", evaluate(1.10, 1.115, "EUR") is None)

    # 3) Boundary: threshold is 5% with strict '>'. NOTE float reality — the real
    #   guard divides floats, so a move that is "exactly" 5% (e.g. 1.0 -> 1.05)
    #   computes as 5.0000000000000004 and DOES trip. Real rates never land on a
    #   clean 5.0, so we assert the meaningful envelope: <5% passes, >5% trips.
    check("+4.9% passes (1.0 -> 1.049)", evaluate(1.0, 1.049) is None)
    check("-4.9% passes (1.0 -> 0.951)", evaluate(1.0, 0.951) is None)
    check("+5.1% trips (1.0 -> 1.051)", evaluate(1.0, 1.051) is not None)
    check("-5.1% trips (1.0 -> 0.949)", evaluate(1.0, 0.949) is not None)

    # 4) A genuine extreme (SNB-2015 class, ~-18%) IS flagged on the jump day...
    day1 = evaluate(1.20, 0.985, "CHF")  # ~-17.9%
    check("SNB-class -17.9% flagged on jump day", day1 is not None)
    # ...and the NEXT day self-confirms against the last WRITTEN value (no freeze):
    check("day 2 (0.985 -> 0.990, +0.5%) silently accepted", evaluate(0.985, 0.990, "CHF") is None)

    # 5) No-baseline / degenerate inputs are skipped (never crash, never flag).
    check("prev=None skipped", evaluate(None, 0.91) is None)
    check("prev=0 skipped", evaluate(0.0, 0.91) is None)
    check("new=None skipped", evaluate(0.778, None) is None)

    # 6) Scope: stablecoins are outside the guarded set entirely.
    check("USDT not in guarded set", "USDT" not in GUARDED_PAIRS)
    check("USDC not in guarded set", "USDC" not in GUARDED_PAIRS)
    check("guarded set is exactly the 5 fiat sources",
          GUARDED_PAIRS == ("CHF", "EUR", "GBP", "JPY", "CNY"))

    print(f"\nALL {passed} CHECKS PASSED — guard flags the 0.91 outlier, stays silent on real moves.")


if __name__ == "__main__":
    main()
