from fastapi import FastAPI, Request, Header
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import os
import json
import time
import hmac
import base64
import hashlib
import xml.etree.ElementTree as ET
import csv
import io
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta, date
from decimal import Decimal
from uuid import UUID
from typing import Optional, Any

import asyncpg
import httpx


# ---------------------------------------------------------------------
# Lifespan — on startup, sweep any import_jobs left in status='started'
# by a previous container that died mid-sync (Railway proxy kills the
# HTTP connection after ~60s on long syncs; the row never gets marked
# finished). Implementation in sweep_stale_import_jobs() below.
# ---------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: "FastAPI"):
    try:
        conn = await get_conn()
        try:
            swept = await sweep_stale_import_jobs(conn)
            if swept > 0:
                print(f"[lifespan] swept {swept} stale import_jobs at startup")
        finally:
            await conn.close()
    except Exception as e:
        # Never let a sweeper failure block app startup.
        print(f"[lifespan] startup sweeper failed (ignored): {e}")
    yield
    # Shutdown: nothing to do for now.


app = FastAPI(title="Valbura Portfolio API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -------------------------
# Helpers
# -------------------------

def require_admin_token(x_admin_token: Optional[str]):
    expected_token = os.getenv("SYNC_ADMIN_TOKEN")

    # If no token is configured, allow syncs.
    # This keeps local/testing deployments from breaking.
    if not expected_token:
        return

    if x_admin_token != expected_token:
        raise PermissionError("Unauthorized")

async def get_conn():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")

    return await asyncpg.connect(
        db_url,
        statement_cache_size=0,
    )


def json_safe(value: Any):
    if isinstance(value, list):
        return [json_safe(v) for v in value]
    if isinstance(value, dict):
        return {k: json_safe(v) for k, v in value.items()}
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, UUID):
        return str(value)
    return value


async def get_portfolio_id(conn, portfolio_name: str):
    row = await conn.fetchrow(
        "SELECT id FROM public.portfolios WHERE name = $1",
        portfolio_name,
    )
    if not row:
        raise RuntimeError(f"Portfolio not found: {portfolio_name}")
    return row["id"]


async def start_import_job(conn, broker: str, portfolio_name: Optional[str], metadata: Optional[dict] = None):
    row = await conn.fetchrow(
        """
        INSERT INTO public.import_jobs (broker, portfolio_name, status, metadata)
        VALUES ($1, $2, 'started', $3::jsonb)
        RETURNING id
        """,
        broker,
        portfolio_name,
        json.dumps(metadata or {}),
    )
    return row["id"]


async def finish_import_job(
    conn,
    job_id,
    status: str,
    rows_seen: int = 0,
    rows_inserted: int = 0,
    rows_updated: int = 0,
    error_message: Optional[str] = None,
    metadata: Optional[dict] = None,
):
    await conn.execute(
        """
        UPDATE public.import_jobs
        SET status = $2,
            finished_at = now(),
            rows_seen = $3,
            rows_inserted = $4,
            rows_updated = $5,
            error_message = $6,
            metadata = COALESCE($7::jsonb, metadata)
        WHERE id = $1
        """,
        job_id,
        status,
        rows_seen,
        rows_inserted,
        rows_updated,
        error_message,
        json.dumps(metadata) if metadata is not None else None,
    )


# How long an import_jobs row may stay in status='started' before it is treated
# as crashed. This ONE constant is shared by the startup sweeper AND the
# /sync/{bitget,ibkr} row-based concurrency checks — they MUST use the same value, or a
# healthy-but-still-running sync could be (a) wrongly swept to 'failed' and
# (b) treated as "not running" by the lock check, allowing a duplicate run.
#
# Both the sweeper and the lock-check measure a job's age from
# COALESCE((metadata->>'work_started_at')::timestamptz, started_at), NOT from
# started_at alone. Reason: /sync/bitget creates BOTH portfolio jobs up front
# sharing one started_at, but the worker processes them SEQUENTIALLY (Global,
# then Alternatives). Measured purely from started_at the second job is ~2x its
# real age at completion (~54 min vs ~27 min of actual work) and would trip this
# threshold mid-run. The worker stamps metadata.work_started_at = now() the
# moment it actually picks up each portfolio, so each job's age reflects its OWN
# work. started_at is left intact as the acceptance time (no created_at column
# exists, so we must not overwrite it).
#
# Why 40 (not 15): a single healthy Bitget funding-fee FULL-history scan takes
# ~27 min today (measured) and grows with history. With work_started_at the
# sweeper/lock-check see ~27 min, so 40 leaves real headroom without killing a
# healthy run. This is a BRIDGE: lower it back toward a few minutes once the
# funding-fee import is incremental (only new bills since the last successful
# sync) instead of a full rescan, which collapses runtime to seconds.
STALE_JOB_MINUTES = 40


async def sweep_stale_import_jobs(conn) -> int:
    """Mark any import_jobs row stuck in status='started' too long as 'failed'.

    Safety net for sync runs whose owning HTTP connection / container died
    before finish_import_job could be called (Railway proxy 60s timeout,
    deploy mid-sync, OOM kill, etc.). The threshold (STALE_JOB_MINUTES) is set
    safely above the longest legitimate run so a healthy sync is never killed.

    Returns the number of rows transitioned. Idempotent: re-running it
    immediately is a no-op.
    """
    result = await conn.execute(
        f"""
        UPDATE public.import_jobs
        SET status = 'failed',
            finished_at = now(),
            error_message = 'auto-stale: status started > {STALE_JOB_MINUTES} min, presumed crashed'
        WHERE status = 'started'
          AND COALESCE((metadata->>'work_started_at')::timestamptz, started_at)
                < now() - interval '{STALE_JOB_MINUTES} minutes'
        """
    )
    # asyncpg returns a tag like "UPDATE 3"; parse the count defensively.
    try:
        return int(result.split()[-1])
    except (ValueError, IndexError, AttributeError):
        return 0


async def log_sync_error(conn, broker: str, portfolio_name: Optional[str], error_message: str, raw_payload: Optional[dict] = None):
    await conn.execute(
        """
        INSERT INTO public.sync_errors (broker, portfolio_name, error_message, raw_payload)
        VALUES ($1, $2, $3, $4::jsonb)
        """,
        broker,
        portfolio_name,
        error_message,
        json.dumps(raw_payload or {}),
    )


def parse_decimal(value, default=0):
    if value is None or value == "":
        return default
    try:
        return float(str(value).replace(",", ""))
    except Exception:
        return default


def normalize_side(value: str):
    if not value:
        return "UNKNOWN"
    value = value.upper()
    if value in ["BOT", "BUY", "B"]:
        return "BUY"
    if value in ["SLD", "SELL", "S"]:
        return "SELL"
    return value


def parse_dt(value):
    if not value:
        return datetime.now(timezone.utc)
    value = str(value)
    for fmt in [
        "%Y%m%d;%H%M%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y%m%d",
        "%Y-%m-%d",
    ]:
        try:
            dt = datetime.strptime(value, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return datetime.now(timezone.utc)


async def get_fx_rate(
    conn,
    from_currency: str,
    to_currency: str,
    rate_date,
) -> Optional[float]:
    """
    Return the FX rate (from_currency → to_currency) for a given date.

    Rules:
    - USDC is treated as USDT (both are USD stablecoins; USDT rates cover both).
    - Returns 1.0 when from_currency == to_currency (after normalisation).
    - Tries an exact match on rate_date first.
    - Falls back to the most-recent rate on or before rate_date.
    - Returns None when no rate is found at all; the caller must handle this.
    """
    # Normalise stablecoins
    if from_currency == "USDC":
        from_currency = "USDT"
    if to_currency == "USDC":
        to_currency = "USDT"

    if from_currency == to_currency:
        return 1.0

    # 1. Exact date match
    row = await conn.fetchrow(
        """
        SELECT rate
        FROM public.fx_rates
        WHERE from_currency = $1
          AND to_currency   = $2
          AND rate_date     = $3
        """,
        from_currency,
        to_currency,
        rate_date,
    )
    if row:
        return float(row["rate"])

    # 2. Last known rate on or before the requested date
    row = await conn.fetchrow(
        """
        SELECT rate
        FROM public.fx_rates
        WHERE from_currency = $1
          AND to_currency   = $2
          AND rate_date    <= $3
        ORDER BY rate_date DESC
        LIMIT 1
        """,
        from_currency,
        to_currency,
        rate_date,
    )
    if row:
        return float(row["rate"])

    return None


# -------------------------
# Public endpoints
# -------------------------

@app.get("/public/overview")
async def get_overview():
    conn = await get_conn()
    try:
        rows = await conn.fetch(
            "SELECT * FROM public.v_portfolio_overview_base ORDER BY portfolio_name"
        )
        return JSONResponse(content=json_safe([dict(row) for row in rows]))
    finally:
        await conn.close()


@app.get("/public/allocation")
async def get_allocation(portfolio: str, group_by: str = "asset_class"):
    view_map = {
        "asset_class": "v_allocation_asset_class_base",
        "broker": "v_allocation_broker_base",
        "currency": "v_allocation_currency_base",
    }
    view = view_map.get(group_by.lower())
    if not view:
        return JSONResponse(content={"error": "Invalid group_by"}, status_code=400)

    conn = await get_conn()
    try:
        rows = await conn.fetch(
            f"""
            SELECT *
            FROM public.{view}
            WHERE portfolio_name = $1
            ORDER BY allocation_percent DESC
            """,
            portfolio,
        )
        return JSONResponse(content=json_safe([dict(row) for row in rows]))
    finally:
        await conn.close()


@app.get("/public/trades")
async def get_trades(portfolio: Optional[str] = None, limit: int = 100):
    conn = await get_conn()
    try:
        if portfolio:
            rows = await conn.fetch(
                """
                SELECT portfolio_name, base_currency, broker, symbol, asset_class,
                       instrument_type, side, quantity, price, currency,
                       gross_value_native, gross_value_base, fee_base, trade_timestamp
                FROM public.v_recent_trades_base
                WHERE portfolio_name = $1
                ORDER BY trade_timestamp DESC
                LIMIT $2
                """,
                portfolio,
                limit,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT portfolio_name, base_currency, broker, symbol, asset_class,
                       instrument_type, side, quantity, price, currency,
                       gross_value_native, gross_value_base, fee_base, trade_timestamp
                FROM public.v_recent_trades_base
                ORDER BY trade_timestamp DESC
                LIMIT $1
                """,
                limit,
            )
        return JSONResponse(content=json_safe([dict(row) for row in rows]))
    finally:
        await conn.close()


@app.get("/public/missing-fx")
async def get_missing_fx():
    conn = await get_conn()
    try:
        rows = await conn.fetch("SELECT * FROM public.v_currency_exposure_missing_fx")
        return JSONResponse(content=json_safe([dict(row) for row in rows]))
    finally:
        await conn.close()


@app.get("/public/sync-status")
async def get_sync_status():
    conn = await get_conn()
    try:
        rows = await conn.fetch(
            "SELECT * FROM public.v_sync_status ORDER BY started_at DESC"
        )
        return JSONResponse(content=json_safe([dict(row) for row in rows]))
    finally:
        await conn.close()

@app.get("/public/broker-accounts")
async def get_broker_accounts():
    conn = await get_conn()
    try:
        rows = await conn.fetch(
            """
            SELECT broker, portfolio_name, account_name, account_identifier,
                   source_key, base_currency, sync_enabled, is_active, updated_at
            FROM public.v_broker_accounts_public
            ORDER BY broker, portfolio_name, account_name
            """
        )
        return JSONResponse(content=json_safe([dict(row) for row in rows]))
    finally:
        await conn.close()

@app.get("/public/positions")
async def get_positions(portfolio: Optional[str] = None):
    conn = await get_conn()
    try:
        if portfolio:
            rows = await conn.fetch(
                """
                SELECT *
                FROM public.v_positions_public_base
                WHERE portfolio_name = $1
                ORDER BY market_value_base DESC NULLS LAST
                """,
                portfolio,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT *
                FROM public.v_positions_public_base
                ORDER BY portfolio_name, market_value_base DESC NULLS LAST
                """
            )
        return JSONResponse(content=json_safe([dict(row) for row in rows]))
    finally:
        await conn.close()

@app.get("/public/closed-positions")
async def get_public_closed_positions(
    portfolio: Optional[str] = None,
    limit: int = 200,
):
    conn = await get_conn()
    try:
        if portfolio:
            rows = await conn.fetch(
                """
                SELECT *
                FROM public.v_closed_positions_public
                WHERE portfolio_name = $1
                ORDER BY closed_pnl_base DESC NULLS LAST
                LIMIT $2
                """,
                portfolio,
                limit,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT *
                FROM public.v_closed_positions_public
                ORDER BY portfolio_name, closed_pnl_base DESC NULLS LAST
                LIMIT $1
                """,
                limit,
            )

        return JSONResponse(content=json_safe([dict(row) for row in rows]))

    finally:
        await conn.close()

@app.get("/public/closed-position-details")
async def get_public_closed_position_details(
    portfolio: Optional[str] = None,
    limit: int = 300,
):
    conn = await get_conn()
    try:
        if portfolio:
            rows = await conn.fetch(
                """
                SELECT *
                FROM public.v_closed_positions_detail_public
                WHERE portfolio_name = $1
                ORDER BY exit_date DESC NULLS LAST, closed_pnl_base DESC NULLS LAST
                LIMIT $2
                """,
                portfolio,
                limit,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT *
                FROM public.v_closed_positions_detail_public
                ORDER BY portfolio_name, exit_date DESC NULLS LAST, closed_pnl_base DESC NULLS LAST
                LIMIT $1
                """,
                limit,
            )

        return JSONResponse(content=json_safe([dict(row) for row in rows]))

    finally:
        await conn.close()


@app.get("/public/nav")
async def get_nav(portfolio: Optional[str] = None):
    conn = await get_conn()
    try:
        if portfolio:
            rows = await conn.fetch(
                """
                SELECT *
                FROM public.v_nav_latest_public
                WHERE portfolio_name = $1
                ORDER BY broker
                """,
                portfolio,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT *
                FROM public.v_nav_latest_public
                ORDER BY portfolio_name, broker
                """
            )
        return JSONResponse(content=json_safe([dict(row) for row in rows]))
    finally:
        await conn.close()

@app.get("/public/portfolio-summary")
async def get_portfolio_summary(portfolio: Optional[str] = None):
    conn = await get_conn()
    try:
        if portfolio:
            rows = await conn.fetch(
                """
                SELECT *
                FROM public.v_public_portfolio_summary
                WHERE portfolio_name = $1
                """,
                portfolio,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT *
                FROM public.v_public_portfolio_summary
                ORDER BY portfolio_name
                """
            )

        return JSONResponse(content=json_safe([dict(row) for row in rows]))
    finally:
        await conn.close()


@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/public/position-allocation")
async def get_position_allocation(portfolio: str, group_by: str = "asset_class"):
    view_map = {
        "asset_class": "v_position_allocation_asset_class",
        "broker": "v_position_allocation_broker",
        "currency": "v_position_allocation_currency",
        "symbol": "v_position_allocation_symbol",
    }

    view = view_map.get(group_by.lower())
    if not view:
        return JSONResponse(content={"error": "Invalid group_by"}, status_code=400)

    conn = await get_conn()
    try:
        rows = await conn.fetch(
            f"""
            SELECT *
            FROM public.{view}
            WHERE portfolio_name = $1
            ORDER BY allocation_percent DESC
            """,
            portfolio,
        )
        return JSONResponse(content=json_safe([dict(row) for row in rows]))
    finally:
        await conn.close()

@app.get("/public/dashboard")
async def get_public_dashboard(
    portfolio: str,
    trade_limit: int = 25,
    closed_limit: int = 100,
    closed_detail_limit: int = 200,
):
    conn = await get_conn()
    try:
        summary_rows = await conn.fetch(
            """
            SELECT *
            FROM public.v_public_portfolio_summary
            WHERE portfolio_name = $1
            """,
            portfolio,
        )

        nav_rows = await conn.fetch(
            """
            SELECT DISTINCT ON (broker) *
            FROM public.v_portfolio_nav_snapshots_base
            WHERE portfolio_name = $1
            ORDER BY broker, snapshot_date DESC, created_at DESC
            """,
            portfolio,
        )

        allocation_asset_class_rows = await conn.fetch(
            """
            SELECT *
            FROM public.v_position_allocation_asset_class
            WHERE portfolio_name = $1
            ORDER BY allocation_percent DESC
            """,
            portfolio,
        )

        allocation_broker_rows = await conn.fetch(
            """
            SELECT *
            FROM public.v_position_allocation_broker
            WHERE portfolio_name = $1
            ORDER BY allocation_percent DESC
            """,
            portfolio,
        )

        allocation_symbol_rows = await conn.fetch(
            """
            SELECT *
            FROM public.v_position_allocation_symbol
            WHERE portfolio_name = $1
            ORDER BY allocation_percent DESC
            """,
            portfolio,
        )

        position_rows = await conn.fetch(
            """
            SELECT *
            FROM public.v_positions_public_base
            WHERE portfolio_name = $1
            ORDER BY ABS(market_value_base) DESC NULLS LAST
            """,
            portfolio,
        )

        trade_rows = await conn.fetch(
            """
            SELECT *
            FROM public.v_recent_trades_base
            WHERE portfolio_name = $1
            ORDER BY trade_timestamp DESC
            LIMIT $2
            """,
            portfolio,
            trade_limit,
        )

        closed_position_rows = await conn.fetch(
            """
            SELECT *
            FROM public.v_closed_positions_public
            WHERE portfolio_name = $1
            ORDER BY closed_pnl_base DESC NULLS LAST
            LIMIT $2
            """,
            portfolio,
            closed_limit,
        )

        closed_position_detail_rows = await conn.fetch(
            """
            SELECT *
            FROM public.v_closed_positions_detail_public
            WHERE portfolio_name = $1
            ORDER BY exit_date DESC NULLS LAST, closed_pnl_base DESC NULLS LAST
            LIMIT $2
            """,
            portfolio,
            closed_detail_limit,
        )

        performance_row = await conn.fetchrow(
            """
            SELECT *
            FROM public.v_portfolio_performance_public
            WHERE portfolio_name = $1
            """,
            portfolio,
        )
        
        sync_rows = await conn.fetch(
            """
            SELECT DISTINCT ON (broker)
                broker,
                portfolio_name,
                status,
                started_at,
                finished_at,
                rows_seen,
                rows_inserted,
                rows_updated,
                error_message,
                metadata
            FROM public.import_jobs
            WHERE portfolio_name = $1
            AND status = 'success'
            ORDER BY broker, finished_at DESC NULLS LAST, started_at DESC
            """,
            portfolio,
        )

        sync_error_rows = await conn.fetch(
            """
            WITH latest_success AS (
                SELECT
                    broker,
                    MAX(finished_at) AS last_success_at
                FROM public.import_jobs
                WHERE portfolio_name = $1
                AND status = 'success'
                GROUP BY broker
            )
            SELECT DISTINCT ON (j.broker)
                j.broker,
                j.portfolio_name,
                j.status,
                j.started_at,
                j.finished_at,
                j.rows_seen,
                j.rows_inserted,
                j.rows_updated,
                j.error_message,
                j.metadata
            FROM public.import_jobs j
            LEFT JOIN latest_success s ON s.broker = j.broker
            WHERE j.portfolio_name = $1
            AND j.status = 'failed'
            AND (
                s.last_success_at IS NULL
                OR j.finished_at > s.last_success_at
            )
            ORDER BY j.broker, j.finished_at DESC NULLS LAST, j.started_at DESC
            """,
            portfolio,
        )

        return JSONResponse(
            content=json_safe(
                {
                    "portfolio": portfolio,
                    "summary": dict(summary_rows[0]) if summary_rows else None,
                    "nav_by_broker": [dict(row) for row in nav_rows],
                    "performance": dict(performance_row) if performance_row else None,
                    "allocation": {
                        "asset_class": [dict(row) for row in allocation_asset_class_rows],
                        "broker": [dict(row) for row in allocation_broker_rows],
                        "symbol": [dict(row) for row in allocation_symbol_rows],
                    },
                    "positions": [dict(row) for row in position_rows],
                    "recent_trades": [dict(row) for row in trade_rows],
                    "closed_positions": [dict(row) for row in closed_position_rows],
                    "closed_position_details": [dict(row) for row in closed_position_detail_rows],
                    "sync_status": [dict(row) for row in sync_rows],
                    "sync_errors": [dict(row) for row in sync_error_rows],
                }
            )
        )

    finally:
        await conn.close()

# -------------------------
# IBKR Flex importer
# -------------------------

async def fetch_ibkr_flex_report(query_id: str):
    token = os.getenv("IBKR_FLEX_TOKEN")
    if not token:
        raise RuntimeError("IBKR_FLEX_TOKEN missing")

    base_url = "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService"
    headers = {
        "User-Agent": "ValburaPortfolioImporter/1.0"
    }

    async with httpx.AsyncClient(timeout=90, follow_redirects=True, headers=headers) as client:
        send_url = f"{base_url}/SendRequest"
        send_params = {
            "t": token,
            "q": query_id,
            "v": "3",
        }

        send_resp = await client.get(send_url, params=send_params)
        send_text = send_resp.text.strip()

        try:
            root = ET.fromstring(send_text)
        except Exception as e:
            raise RuntimeError(
                f"IBKR SendRequest response is not XML: {str(e)} | "
                f"http_status={send_resp.status_code} | response_start={send_text[:500]}"
            )

        status = None
        ref_code = None
        error_code = None
        error_message = None

        for elem in root.iter():
            tag = elem.tag.lower()
            if tag.endswith("status"):
                status = elem.text
            elif tag.endswith("referencecode"):
                ref_code = elem.text
            elif tag.endswith("errorcode"):
                error_code = elem.text
            elif tag.endswith("errormessage"):
                error_message = elem.text

        if status and status.lower() != "success":
            raise RuntimeError(
                f"IBKR SendRequest failed: status={status}, "
                f"error_code={error_code}, error_message={error_message}, "
                f"response_start={send_text[:1000]}"
            )

        if not ref_code:
            raise RuntimeError(f"IBKR ReferenceCode missing: {send_text[:1000]}")

        fetch_url = f"{base_url}/GetStatement"
        fetch_params = {
            "t": token,
            "q": ref_code,
            "v": "3",
        }

        # Poll GetStatement on the SAME reference code until the statement is
        # ready. While IBKR is still building the report it returns ErrorCode
        # 1019 ("Statement generation in progress") — that is transient and MUST
        # be retried on the same reference code; issuing a fresh SendRequest each
        # time would restart generation and never finish for slow/large queries.
        #
        # Failure policy (correctness over silent gaps):
        #   - 1019                -> transient, keep polling until the deadline.
        #   - any other ErrorCode -> hard fail immediately (do NOT poll 180s on a
        #                            real error like an expired token or invalid
        #                            query — those already fail at SendRequest,
        #                            but we guard here too).
        #   - deadline reached     -> hard fail (raise), so the caller marks the
        #                            sync job "failed" instead of importing a
        #                            silently empty (0-row) statement as success.
        retryable_error_codes = {"1019"}
        max_wait_seconds = 180
        poll_interval = 8
        deadline = time.monotonic() + max_wait_seconds

        # Initial grace period before the first poll (statements are never ready
        # instantly after SendRequest).
        await asyncio.sleep(poll_interval)

        while True:
            fetch_resp = await client.get(fetch_url, params=fetch_params)
            fetch_text = fetch_resp.text.strip()

            if not fetch_text:
                raise RuntimeError("IBKR GetStatement returned empty response")

            error_code = None
            error_message = None
            try:
                fetch_root = ET.fromstring(fetch_text)
            except Exception:
                fetch_root = None

            if fetch_root is not None:
                for elem in fetch_root.iter():
                    tag = elem.tag.lower()
                    if tag.endswith("errorcode"):
                        error_code = (elem.text or "").strip()
                    elif tag.endswith("errormessage"):
                        error_message = (elem.text or "").strip()

            # No error envelope -> this is the real statement.
            if not error_code:
                return fetch_text

            # Transient "still generating" -> retry until the deadline.
            if error_code in retryable_error_codes:
                if time.monotonic() >= deadline:
                    raise RuntimeError(
                        f"IBKR statement not ready after {max_wait_seconds}s "
                        f"(query_id={query_id}): error_code={error_code}, "
                        f"error_message={error_message}"
                    )
                await asyncio.sleep(poll_interval)
                continue

            # Any other error code -> hard fail immediately.
            raise RuntimeError(
                f"IBKR GetStatement failed (query_id={query_id}): "
                f"error_code={error_code}, error_message={error_message}"
            )

@app.post("/debug/ibkr/tags")
async def debug_ibkr_tags(x_admin_token: Optional[str] = Header(None)):
    try:
        require_admin_token(x_admin_token)
    except PermissionError:
        return JSONResponse(content={"error": "Unauthorized"}, status_code=401)

    query_id = os.getenv("IBKR_ACTIVITY_QUERY_ID_GLOBAL")
    if not query_id:
        return JSONResponse(content={"error": "IBKR_ACTIVITY_QUERY_ID_GLOBAL missing"}, status_code=500)

    try:
        xml_text = await fetch_ibkr_flex_report(query_id)
        root = ET.fromstring(xml_text)

        tag_counts = {}
        sample_attrs = {}

        for elem in root.iter():
            tag = elem.tag.split("}")[-1]
            tag_counts[tag] = tag_counts.get(tag, 0) + 1

            if tag not in sample_attrs and elem.attrib:
                safe_attrs = {}
                for key, value in elem.attrib.items():
                    if key.lower() in ["accountid", "accountnumber", "acctid"]:
                        safe_attrs[key] = "***"
                    else:
                        safe_attrs[key] = value
                sample_attrs[tag] = safe_attrs

        return JSONResponse(
            content={
                "status": "success",
                "tag_counts": tag_counts,
                "sample_attrs": sample_attrs,
            }
        )

    except Exception as e:
        return JSONResponse(content={"status": "failed", "error": str(e)}, status_code=500)


def detect_ibkr_asset_class(asset_category: str):
    if not asset_category:
        return "Unknown"
    value = asset_category.lower()
    if "stock" in value:
        return "Stocks"
    if "future" in value:
        return "Futures"
    if "option" in value:
        return "Options"
    if "forex" in value or "cash" in value:
        return "Forex"
    return asset_category


async def upsert_ibkr_trades(conn, portfolio_name: str, report_text: str):
    portfolio_id = await get_portfolio_id(conn, portfolio_name)

    rows_seen = 0
    rows_inserted = 0
    rows_updated = 0

    report_text_stripped = report_text.strip()

    # -------------------------------------------------
    # CASE 1: XML Flex Report
    # -------------------------------------------------
    if report_text_stripped.startswith("<"):
        try:
            root = ET.fromstring(report_text)
        except Exception:
            root = None

        if root is not None:
            for elem in root.iter():
                if elem.tag.lower().endswith("trade"):
                    data = elem.attrib
                    rows_seen += 1

                    trade_id = (
                        data.get("tradeID")
                        or data.get("tradeId")
                        or data.get("ibExecID")
                        or data.get("execID")
                        or data.get("transactionID")
                    )

                    if not trade_id:
                        trade_id = f"ibkr-{portfolio_name}-{rows_seen}-{data.get('symbol','')}-{data.get('dateTime','')}"

                    trade_id = f"{portfolio_name}:{trade_id}"

                    symbol = data.get("symbol") or data.get("description") or "UNKNOWN"
                    side = normalize_side(data.get("buySell") or data.get("side"))
                    quantity = abs(parse_decimal(data.get("quantity")))
                    price = parse_decimal(data.get("tradePrice") or data.get("price"))
                    currency = data.get("currency") or "USD"
                    asset_class = detect_ibkr_asset_class(data.get("assetCategory"))
                    commission = parse_decimal(data.get("ibCommission"), 0)
                    trade_time = parse_dt(data.get("dateTime") or data.get("tradeDate"))

                    existing_trade = await conn.fetchrow(
                        """
                        SELECT id
                        FROM public.trades
                        WHERE broker = 'IBKR'
                        AND external_trade_id = $1
                        """,
                        trade_id,
                    )
                    
                    result = await conn.execute(
                        """
                        INSERT INTO public.trades (
                            portfolio_id, broker, symbol, asset_class, instrument_type,
                            side, quantity, price, currency, fee, trade_date,
                            execution_time, external_trade_id, raw_payload
                        )
                        VALUES (
                            $1, 'IBKR', $2, $3, $4,
                            $5, $6, $7, $8, $9, $10,
                            $10, $11, $12::jsonb
                        )
                        ON CONFLICT (broker, external_trade_id)
                        DO UPDATE SET
                            portfolio_id = EXCLUDED.portfolio_id,
                            symbol = EXCLUDED.symbol,
                            asset_class = EXCLUDED.asset_class,
                            instrument_type = EXCLUDED.instrument_type,
                            side = EXCLUDED.side,
                            quantity = EXCLUDED.quantity,
                            price = EXCLUDED.price,
                            currency = EXCLUDED.currency,
                            fee = EXCLUDED.fee,
                            trade_date = EXCLUDED.trade_date,
                            execution_time = EXCLUDED.execution_time,
                            raw_payload = EXCLUDED.raw_payload,
                            imported_at = now()
                        """,
                        portfolio_id,
                        symbol,
                        asset_class,
                        data.get("assetCategory") or asset_class,
                        side,
                        quantity,
                        price,
                        currency,
                        commission,
                        trade_time,
                        trade_id,
                        json.dumps(data),
                    )

                    if existing_trade:
                        rows_updated += 1
                    else:
                        rows_inserted += 1

            return {
                "rows_seen": rows_seen,
                "rows_inserted": rows_inserted,
                "rows_updated": rows_updated,
                "portfolio": portfolio_name,
                "format": "xml",
            }

async def upsert_ibkr_snapshot_and_positions(conn, portfolio_name: str, xml_text: str):
    portfolio_id = await get_portfolio_id(conn, portfolio_name)

    report_text_stripped = xml_text.strip()
    if not report_text_stripped.startswith("<"):
        return {
            "portfolio": portfolio_name,
            "snapshot_imported": False,
            "reason": "IBKR report is not XML",
        }

    root = ET.fromstring(xml_text)

    latest_equity = None
    latest_report_date = None

    for elem in root.iter():
        if elem.tag.lower().endswith("equitysummarybyreportdateinbase"):
            data = elem.attrib
            report_date = data.get("reportDate")

            if report_date and (latest_report_date is None or report_date > latest_report_date):
                latest_report_date = report_date
                latest_equity = data

    if not latest_equity:
        return {
            "portfolio": portfolio_name,
            "snapshot_imported": False,
            "reason": "No EquitySummaryByReportDateInBase found",
        }

    snapshot_date = parse_dt(latest_report_date).date()
    currency = latest_equity.get("currency") or "CHF"

    nav = parse_decimal(latest_equity.get("total"), 0)
    cash = parse_decimal(latest_equity.get("cash"), 0)

    # IBKR reports some unrealized components separately.
    # We use these as open PnL approximation in base currency.
    cfd_unrealized = parse_decimal(latest_equity.get("cfdUnrealizedPl"), 0)
    forex_cfd_unrealized = parse_decimal(latest_equity.get("forexCfdUnrealizedPl"), 0)
    open_pnl = cfd_unrealized + forex_cfd_unrealized

    market_value = nav - cash

    # --- IBKR cashflow from CashReportCurrency BASE_SUMMARY ---
    # In many Flex reports this is YTD/cumulative. Therefore:
    # - first import stores the current cumulative value as initial cashflow at fromDate
    # - later imports store only the delta versus the last imported cumulative value
    ibkr_cashflow = None

    for elem in root.iter():
        if elem.tag.lower().endswith("cashreportcurrency"):
            data = elem.attrib
            cash_currency = data.get("currency")

            if cash_currency != "BASE_SUMMARY":
                continue

            from_date_raw = data.get("fromDate")
            to_date_raw = data.get("toDate")
            deposit_withdrawals = parse_decimal(data.get("depositWithdrawals"), 0)

            if not to_date_raw:
                continue

            try:
                from_date = parse_dt(from_date_raw).date() if from_date_raw else snapshot_date
            except Exception:
                from_date = snapshot_date

            try:
                to_date = parse_dt(to_date_raw).date()
            except Exception:
                to_date = snapshot_date

            ibkr_cashflow = {
                "from_date": from_date,
                "to_date": to_date,
                "currency": currency,
                "ytd_deposit_withdrawals": deposit_withdrawals,
                "raw_payload": data,
            }
            break

    # Build all position rows first. This avoids deleting existing positions if
    # parsing/insertion would fail halfway through.
    position_rows = []
    positions_seen = 0
    total_position_value_base = 0
    total_open_pnl_base = 0

    for elem in root.iter():
        if elem.tag.lower().endswith("openposition"):
            data = elem.attrib
            positions_seen += 1

            symbol = data.get("symbol") or data.get("description") or "UNKNOWN"
            asset_class = detect_ibkr_asset_class(data.get("assetCategory"))
            position = parse_decimal(data.get("position"), 0)
            avg_cost = parse_decimal(data.get("costBasisPrice") or data.get("openPrice"), 0)
            position_currency = data.get("currency") or currency
            mark_price = parse_decimal(data.get("markPrice"), 0)

            fx_rate_to_base = parse_decimal(data.get("fxRateToBase"), 1)
            position_value_native = parse_decimal(data.get("positionValue"), 0)
            position_value_base = position_value_native * fx_rate_to_base

            open_pnl_native = parse_decimal(data.get("fifoPnlUnrealized"), 0)
            open_pnl_base = open_pnl_native * fx_rate_to_base

            total_position_value_base += position_value_base
            total_open_pnl_base += open_pnl_base

            position_side = "SHORT" if position < 0 else "LONG" if position > 0 else None

            entry_row = await conn.fetchrow(
                """
                SELECT MIN(execution_time) AS entry_date
                FROM public.trades
                WHERE portfolio_id = $1
                AND broker = 'IBKR'
                AND symbol = $2
                """,
                portfolio_id,
                symbol,
            )
            entry_date = entry_row["entry_date"] if entry_row else None
            source_position_id = data.get("conid") or data.get("symbol") or symbol

            position_rows.append({
                "symbol": symbol,
                "asset_class": asset_class,
                "position": position,
                "avg_cost": avg_cost,
                "position_currency": position_currency,
                "mark_price": mark_price,
                "position_value_native": position_value_native,
                "position_value_base": position_value_base,
                "open_pnl_native": open_pnl_native,
                "open_pnl_base": open_pnl_base,
                "entry_date": entry_date,
                "position_side": position_side,
                "source_position_id": source_position_id,
            })

    positions_inserted = 0
    cashflow_imported = False
    cashflow_amount_base = None

    async with conn.transaction():
        await conn.execute(
            """
            INSERT INTO public.portfolio_nav_snapshots (
                portfolio_id, broker, snapshot_date, currency,
                nav, cash, market_value, open_pnl, closed_pnl,
                deposits_withdrawals, source
            )
            VALUES (
                $1, 'IBKR', $2, $3,
                $4, $5, $6, $7, NULL,
                $8, 'ibkr_flex_equity_summary'
            )
            ON CONFLICT (portfolio_id, broker, snapshot_date, currency)
            DO UPDATE SET
                nav = EXCLUDED.nav,
                cash = EXCLUDED.cash,
                market_value = EXCLUDED.market_value,
                open_pnl = EXCLUDED.open_pnl,
                deposits_withdrawals = EXCLUDED.deposits_withdrawals,
                source = EXCLUDED.source,
                created_at = now()
            """,
            portfolio_id,
            snapshot_date,
            currency,
            nav,
            cash,
            market_value,
            open_pnl,
            ibkr_cashflow["ytd_deposit_withdrawals"] if ibkr_cashflow else None,
        )

        await conn.execute(
            """
            INSERT INTO public.portfolio_cash (
                portfolio_id, broker, currency, cash_balance,
                cash_balance_base, updated_at
            )
            VALUES ($1, 'IBKR', $2, $3, $3, now())
            ON CONFLICT (portfolio_id, broker, currency)
            DO UPDATE SET
                cash_balance = EXCLUDED.cash_balance,
                cash_balance_base = EXCLUDED.cash_balance_base,
                updated_at = now()
            """,
            portfolio_id,
            currency,
            cash,
        )

        # Import IBKR cashflow delta from YTD CashReportCurrency BASE_SUMMARY.
        if ibkr_cashflow is not None:
            previous_cashflow = await conn.fetchrow(
                """
                SELECT
                    cashflow_date,
                    raw_payload
                FROM public.portfolio_cashflows
                WHERE portfolio_id = $1
                  AND broker = 'IBKR'
                  AND source = 'ibkr_cash_report_ytd_delta'
                ORDER BY cashflow_date DESC, created_at DESC
                LIMIT 1
                """,
                portfolio_id,
            )

            current_ytd = ibkr_cashflow["ytd_deposit_withdrawals"]

            if previous_cashflow and previous_cashflow["raw_payload"]:
                previous_raw_payload = previous_cashflow["raw_payload"]

                if isinstance(previous_raw_payload, str):
                    try:
                        previous_raw_payload = json.loads(previous_raw_payload)
                    except Exception:
                        previous_raw_payload = {}

                if previous_raw_payload is None:
                    previous_raw_payload = {}

                previous_ytd = parse_decimal(
                    previous_raw_payload.get("current_ytd_deposit_withdrawals"),
                    0,
                )
                cashflow_delta = current_ytd - previous_ytd
                cashflow_date = ibkr_cashflow["to_date"]
            else:
                previous_ytd = 0
                cashflow_delta = current_ytd
                cashflow_date = ibkr_cashflow["from_date"]

            if cashflow_delta != 0:
                external_id = f"IBKR:{portfolio_name}:cash_report_ytd_delta:{cashflow_date.isoformat()}"

                raw_payload = {
                    "from_date": ibkr_cashflow["from_date"].isoformat(),
                    "to_date": ibkr_cashflow["to_date"].isoformat(),
                    "current_ytd_deposit_withdrawals": str(current_ytd),
                    "previous_ytd_deposit_withdrawals": str(previous_ytd),
                    "cashflow_delta": str(cashflow_delta),
                    "cash_report": ibkr_cashflow["raw_payload"],
                }

                await conn.execute(
                    """
                    INSERT INTO public.portfolio_cashflows (
                        portfolio_id, broker, cashflow_date, currency,
                        amount_native, amount_base,
                        cashflow_type, source, external_id, raw_payload,
                        updated_at
                    )
                    VALUES (
                        $1, 'IBKR', $2, $3,
                        $4, $4,
                        'NET_DEPOSIT_WITHDRAWAL',
                        'ibkr_cash_report_ytd_delta',
                        $5, $6::jsonb,
                        now()
                    )
                    ON CONFLICT (portfolio_id, broker, source, external_id)
                    DO UPDATE SET
                        cashflow_date = EXCLUDED.cashflow_date,
                        currency = EXCLUDED.currency,
                        amount_native = EXCLUDED.amount_native,
                        amount_base = EXCLUDED.amount_base,
                        cashflow_type = EXCLUDED.cashflow_type,
                        raw_payload = EXCLUDED.raw_payload,
                        updated_at = now()
                    """,
                    portfolio_id,
                    cashflow_date,
                    currency,
                    cashflow_delta,
                    external_id,
                    json.dumps(raw_payload),
                )

                cashflow_imported = True
                cashflow_amount_base = cashflow_delta

        # Replace only this portfolio's IBKR positions after parsing succeeded.
        await conn.execute(
            """
            DELETE FROM public.positions
            WHERE portfolio_id = $1
            AND broker = 'IBKR'
            """,
            portfolio_id,
        )

        for row in position_rows:
            await conn.execute(
                """
                INSERT INTO public.positions (
                    portfolio_id, broker, symbol, asset_class,
                    quantity, avg_cost, currency, market_price,
                    market_value_native, market_value_base,
                    open_pnl_native, open_pnl_base,
                    entry_date, position_side,
                    take_profit, stop_loss,
                    take_profit_order_id, stop_loss_order_id,
                    source_position_id,
                    updated_at,
                    take_profit_orders,
                    stop_loss_orders
                )
                VALUES (
                    $1, 'IBKR', $2, $3,
                    $4, $5, $6, $7,
                    $8, $9,
                    $10, $11,
                    $12, $13,
                    NULL, NULL,
                    NULL, NULL,
                    $14,
                    now(),
                    '[]'::jsonb,
                    '[]'::jsonb
                )
                ON CONFLICT (portfolio_id, broker, symbol)
                DO UPDATE SET
                    asset_class = EXCLUDED.asset_class,
                    quantity = EXCLUDED.quantity,
                    avg_cost = EXCLUDED.avg_cost,
                    currency = EXCLUDED.currency,
                    market_price = EXCLUDED.market_price,
                    market_value_native = EXCLUDED.market_value_native,
                    market_value_base = EXCLUDED.market_value_base,
                    open_pnl_native = EXCLUDED.open_pnl_native,
                    open_pnl_base = EXCLUDED.open_pnl_base,
                    entry_date = EXCLUDED.entry_date,
                    position_side = EXCLUDED.position_side,
                    take_profit = EXCLUDED.take_profit,
                    stop_loss = EXCLUDED.stop_loss,
                    take_profit_order_id = EXCLUDED.take_profit_order_id,
                    stop_loss_order_id = EXCLUDED.stop_loss_order_id,
                    source_position_id = EXCLUDED.source_position_id,
                    take_profit_orders = EXCLUDED.take_profit_orders,
                    stop_loss_orders = EXCLUDED.stop_loss_orders,
                    updated_at = now()
                """,
                portfolio_id,
                row["symbol"],
                row["asset_class"],
                row["position"],
                row["avg_cost"],
                row["position_currency"],
                row["mark_price"],
                row["position_value_native"],
                row["position_value_base"],
                row["open_pnl_native"],
                row["open_pnl_base"],
                row["entry_date"],
                row["position_side"],
                row["source_position_id"],
            )
            positions_inserted += 1

    return {
        "portfolio": portfolio_name,
        "snapshot_imported": True,
        "snapshot_date": snapshot_date.isoformat(),
        "currency": currency,
        "nav": nav,
        "cash": cash,
        "market_value": market_value,
        "open_pnl": open_pnl,
        "positions_seen": positions_seen,
        "positions_inserted": positions_inserted,
        "total_position_value_base": total_position_value_base,
        "total_open_pnl_base": total_open_pnl_base,
        "cashflow_imported": cashflow_imported,
        "cashflow_amount_base": cashflow_amount_base,
    }

async def upsert_ibkr_realized_pnl(conn, portfolio_name: str, xml_text: str):
    portfolio_id = await get_portfolio_id(conn, portfolio_name)

    report_text_stripped = xml_text.strip()
    if not report_text_stripped.startswith("<"):
        return {
            "portfolio": portfolio_name,
            "realized_imported": False,
            "reason": "IBKR report is not XML",
        }

    root = ET.fromstring(xml_text)

    rows_seen = 0
    rows_inserted = 0
    rows_updated = 0
    total_realized_pnl = 0

    for elem in root.iter():
        if elem.tag.lower().endswith("fifoperformancesummaryunderlying"):
            data = elem.attrib
            rows_seen += 1

            symbol = data.get("underlyingSymbol") or data.get("symbol") or "UNKNOWN"
            asset_class = detect_ibkr_asset_class(data.get("assetCategory"))
            currency = "CHF"

            realized_pnl = parse_decimal(data.get("totalRealizedPnl"), 0)
            unrealized_pnl = parse_decimal(data.get("totalUnrealizedPnl"), 0)
            total_fifo_pnl = parse_decimal(data.get("totalFifoPnl"), 0)

            # Skip pure zero rows to avoid noisy events
            if realized_pnl == 0 and unrealized_pnl == 0 and total_fifo_pnl == 0:
                continue

            event_date_raw = data.get("reportDate") or ""
            event_time = parse_dt(event_date_raw) if event_date_raw else datetime.now(timezone.utc)

            external_id = f"{portfolio_name}:ibkr-fifo-{symbol}-{event_date_raw or 'latest'}"

            existing_event = await conn.fetchrow(
                """
                SELECT id
                FROM public.realized_pnl_events
                WHERE broker = 'IBKR'
                AND external_id = $1
                """,
                external_id,
            )

            await conn.execute(
                """
                INSERT INTO public.realized_pnl_events (
                    portfolio_id, broker, symbol, asset_class,
                    realized_pnl, currency, realized_pnl_base,
                    event_time, external_id, raw_payload
                )
                VALUES (
                    $1, 'IBKR', $2, $3,
                    $4, $5, $4,
                    $6, $7, $8::jsonb
                )
                ON CONFLICT (broker, external_id)
                DO UPDATE SET
                    portfolio_id = EXCLUDED.portfolio_id,
                    symbol = EXCLUDED.symbol,
                    asset_class = EXCLUDED.asset_class,
                    realized_pnl = EXCLUDED.realized_pnl,
                    currency = EXCLUDED.currency,
                    realized_pnl_base = EXCLUDED.realized_pnl_base,
                    event_time = EXCLUDED.event_time,
                    raw_payload = EXCLUDED.raw_payload
                """,
                portfolio_id,
                symbol,
                asset_class,
                realized_pnl,
                currency,
                event_time,
                external_id,
                json.dumps(data),
            )

            if existing_event:
                rows_updated += 1
            else:
                rows_inserted += 1

            total_realized_pnl += realized_pnl

    return {
        "portfolio": portfolio_name,
        "realized_imported": True,
        "rows_seen": rows_seen,
        "rows_inserted": rows_inserted,
        "rows_updated": rows_updated,
        "total_realized_pnl": total_realized_pnl,
    }
    
    # -------------------------------------------------
    # CASE 2: CSV Flex Report / Activity Statement fallback
    # -------------------------------------------------
    reader = csv.reader(io.StringIO(report_text))

    for row in reader:
        if not row:
            continue

        # IBKR Activity Statement CSV trade rows usually look like:
        # Trades,Data,Order,Stocks,USD,AAPL,"2026-...",Quantity,Price,...
        if len(row) > 11 and row[0] == "Trades" and row[1] == "Data":
            rows_seen += 1

            try:
                asset_class = row[3]
                currency = row[4]
                symbol = row[5]
                trade_time_raw = row[6]
                quantity_raw = row[7]
                price_raw = row[8]
                commission_raw = row[11]
            except Exception:
                continue

            quantity_signed = parse_decimal(quantity_raw)
            quantity = abs(quantity_signed)
            side = "BUY" if quantity_signed > 0 else "SELL"
            price = parse_decimal(price_raw)
            commission = parse_decimal(commission_raw, 0)
            trade_time = parse_dt(trade_time_raw.replace(",", ""))

            raw_trade_id = f"{symbol}-{trade_time_raw}-{quantity_raw}-{price_raw}-{rows_seen}"
            trade_id = f"{portfolio_name}:{raw_trade_id}"

            data = {
                "source": "ibkr_csv",
                "asset_class": asset_class,
                "currency": currency,
                "symbol": symbol,
                "trade_time": trade_time_raw,
                "quantity": quantity_raw,
                "price": price_raw,
                "commission": commission_raw,
                "row": row,
            }

            existing_trade = await conn.fetchrow(
                """
                SELECT id
                FROM public.trades
                WHERE broker = 'IBKR'
                AND external_trade_id = $1
                """,
                trade_id,
            )
            
            result = await conn.execute(
                """
                INSERT INTO public.trades (
                    portfolio_id, broker, symbol, asset_class, instrument_type,
                    side, quantity, price, currency, fee, trade_date,
                    execution_time, external_trade_id, raw_payload
                )
                VALUES (
                    $1, 'IBKR', $2, $3, $4,
                    $5, $6, $7, $8, $9, $10,
                    $10, $11, $12::jsonb
                )
                ON CONFLICT (broker, external_trade_id)
                DO UPDATE SET
                    portfolio_id = EXCLUDED.portfolio_id,
                    symbol = EXCLUDED.symbol,
                    asset_class = EXCLUDED.asset_class,
                    instrument_type = EXCLUDED.instrument_type,
                    side = EXCLUDED.side,
                    quantity = EXCLUDED.quantity,
                    price = EXCLUDED.price,
                    currency = EXCLUDED.currency,
                    fee = EXCLUDED.fee,
                    trade_date = EXCLUDED.trade_date,
                    execution_time = EXCLUDED.execution_time,
                    raw_payload = EXCLUDED.raw_payload,
                    imported_at = now()
                """,
                portfolio_id,
                symbol,
                asset_class,
                asset_class,
                side,
                quantity,
                price,
                currency,
                commission,
                trade_time,
                trade_id,
                json.dumps(data),
            )

            if existing_trade:
                rows_updated += 1
            else:
                rows_inserted += 1

    return {
        "rows_seen": rows_seen,
        "rows_inserted": rows_inserted,
        "rows_updated": rows_updated,
        "portfolio": portfolio_name,
        "format": "csv",
    }


IBKR_LOCK_KEY = "sync:ibkr"  # advisory-lock key, distinct from Bitget so the two
                            # brokers never serialize against each other.
_ibkr_tasks: set = set()    # strong refs to in-flight IBKR workers (asyncio only
                            # holds weak refs to tasks -> keep our own or it may GC).


async def run_ibkr_sync_job(jobs: list):
    """Background worker for /sync/ibkr. Mirrors run_bitget_sync_job.

    Opens and OWNS its own connection. The import_jobs rows were already created
    (status='started') and COMMITTED by the request handler inside its xact lock;
    this worker only receives their ids + portfolio names + Flex query ids and
    drives the actual import, finishing each job independently.

    No advisory lock is held here. Concurrency is gated entirely in the handler
    (transaction-scoped lock + a row-based "is one already running?" check) — the
    only approach that survives the Supavisor transaction pooler.

    Contract:
      * Each portfolio runs in its OWN try/except — one failing does NOT abort
        the other.
      * The `finally` block ALWAYS closes the connection. On a hard crash the
        jobs stay 'started' and are reclaimed by the sweeper once they exceed
        STALE_JOB_MINUTES.
    """
    conn = await get_conn()
    try:
        for job in jobs:
            portfolio_name = job["portfolio"]
            job_id = job["job_id"]
            query_id = job["query_id"]
            try:
                # Stamp the moment THIS worker actually starts this portfolio, so
                # the sweeper/lock-check measure each job's age from its OWN work
                # (see the STALE_JOB_MINUTES comment). started_at stays intact as
                # the acceptance time.
                await conn.execute(
                    """
                    UPDATE public.import_jobs
                    SET metadata = COALESCE(metadata, '{}'::jsonb)
                                   || jsonb_build_object('work_started_at', now())
                    WHERE id = $1
                    """,
                    job_id,
                )

                xml_text = await fetch_ibkr_flex_report(query_id)
                result = await upsert_ibkr_trades(conn, portfolio_name, xml_text)
                snapshot_result = await upsert_ibkr_snapshot_and_positions(conn, portfolio_name, xml_text)
                realized_result = await upsert_ibkr_realized_pnl(conn, portfolio_name, xml_text)

                result["snapshot"] = snapshot_result
                result["realized_pnl"] = realized_result

                await finish_import_job(
                    conn,
                    job_id,
                    "success",
                    rows_seen=result["rows_seen"],
                    rows_inserted=result["rows_inserted"],
                    rows_updated=result["rows_updated"],
                    metadata=result,
                )
            except Exception as e:
                # Isolate the failure to THIS portfolio; the loop continues.
                print(f"[ibkr] portfolio {portfolio_name} failed: {e}")
                try:
                    await finish_import_job(conn, job_id, "failed", error_message=str(e))
                    await log_sync_error(conn, "IBKR", portfolio_name, str(e))
                except Exception as inner:
                    print(f"[ibkr] could not record failure for {portfolio_name}: {inner}")
    finally:
        # ALWAYS close the worker's own connection.
        await conn.close()


@app.get("/sync/status/{job_id}")
async def get_sync_job_status(job_id: str, x_admin_token: Optional[str] = Header(None)):
    """Return the full import_jobs row for a given job_id.

    Admin-token protected, like the POST /sync/* endpoints — the response
    can carry error messages and broker metadata that don't belong in a
    public surface. The public dashboard continues to use
    /public/sync-status (the v_sync_status view).
    """
    try:
        require_admin_token(x_admin_token)
    except PermissionError:
        return JSONResponse(content={"error": "Unauthorized"}, status_code=401)

    try:
        job_uuid = UUID(job_id)
    except ValueError:
        return JSONResponse(
            content={"error": "Invalid job_id (must be a UUID)"},
            status_code=400,
        )

    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            """
            SELECT id, broker, portfolio_name, source_account, status,
                   started_at, finished_at,
                   rows_seen, rows_inserted, rows_updated,
                   error_message, metadata
            FROM public.import_jobs
            WHERE id = $1
            """,
            job_uuid,
        )
        if row is None:
            return JSONResponse(content={"error": "job not found"}, status_code=404)
        return JSONResponse(content=json_safe(dict(row)))
    finally:
        await conn.close()


@app.post("/sync/ibkr")
async def sync_ibkr(x_admin_token: Optional[str] = Header(None)):
    return await _start_ibkr_sync(x_admin_token)


@app.post("/sync/ibkr/trigger")
async def trigger_ibkr(x_admin_token: Optional[str] = Header(None)):
    # Alias of /sync/ibkr, kept through go-live so a cron pointing at EITHER
    # route keeps working. Remove after the cron target is confirmed/migrated.
    return await _start_ibkr_sync(x_admin_token)


async def _start_ibkr_sync(x_admin_token: Optional[str]):
    """Async /sync/ibkr handler, mirroring sync_bitget.

    Returns 202 + job ids after creating both portfolio jobs inside a pooler-safe
    transaction-scoped advisory lock; 409 if an IBKR sync is already running; 401
    on a bad token; 500 on a setup/DB error.
    """
    try:
        require_admin_token(x_admin_token)
    except PermissionError:
        return JSONResponse(content={"error": "Unauthorized"}, status_code=401)

    global_query = os.getenv("IBKR_ACTIVITY_QUERY_ID_GLOBAL")
    alternatives_query = os.getenv("IBKR_ACTIVITY_QUERY_ID_ALTERNATIVES") or global_query
    if not global_query:
        return JSONResponse(
            content={"status": "failed", "broker": "IBKR",
                     "error": "IBKR_ACTIVITY_QUERY_ID_GLOBAL missing"},
            status_code=500,
        )
    portfolios = [("Global", global_query), ("Alternatives", alternatives_query)]

    jobs = []
    already_running = False

    conn = await get_conn()
    try:
        # (4) Sweep stale zombies first — never let a sweep failure block the sync.
        try:
            swept = await sweep_stale_import_jobs(conn)
            if swept > 0:
                print(f"[ibkr] swept {swept} stale import_jobs before sync")
        except Exception as e:
            print(f"[ibkr] pre-sync sweep failed (ignored): {e}")

        # (2) Pooler-safe mutex (see sync_bitget for the full rationale). A
        #     TRANSACTION-scoped advisory lock serializes the check+insert (one
        #     transaction = one pinned backend), and the mutex STATE is row-based.
        #     Distinct lock key from Bitget so the two brokers don't block each
        #     other; the row check is scoped to broker='IBKR'. Check AND insert run
        #     in the SAME transaction under the SAME lock -> no TOCTOU window. The
        #     xact lock auto-releases at COMMIT.
        async with conn.transaction():
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext($1))", IBKR_LOCK_KEY
            )
            running = await conn.fetchval(
                f"""
                SELECT count(*) FROM public.import_jobs
                WHERE broker = 'IBKR'
                  AND status = 'started'
                  AND COALESCE((metadata->>'work_started_at')::timestamptz, started_at)
                        > now() - interval '{STALE_JOB_MINUTES} minutes'
                """
            )
            if running and running > 0:
                already_running = True
            else:
                # Create both jobs INSIDE the locked transaction so their ids ride
                # along in the 202 and a concurrent caller sees them.
                for portfolio_name, query_id in portfolios:
                    job_id = await start_import_job(
                        conn,
                        "IBKR",
                        portfolio_name,
                        {"query_id": query_id},
                    )
                    jobs.append({
                        "job_id": job_id,                   # UUID, used by the worker
                        "broker": "IBKR",
                        "portfolio": portfolio_name,
                        "query_id": query_id,               # passed to the worker
                        "poll_url": f"/sync/status/{job_id}",
                    })
        # transaction committed: jobs are persisted and the xact lock is released.
    except Exception as e:
        await conn.close()
        return JSONResponse(content={"status": "failed", "error": str(e)}, status_code=500)
    finally:
        if not conn.is_closed():
            await conn.close()

    if already_running:
        return JSONResponse(
            content={
                "status": "in_progress",
                "broker": "IBKR",
                "message": "an IBKR sync is already running",
            },
            status_code=409,
        )

    # Spawn the worker, which opens and owns its OWN fresh connection. Keep a
    # strong reference so the task can't be GC'd mid-run.
    task = asyncio.create_task(run_ibkr_sync_job(jobs))
    _ibkr_tasks.add(task)
    task.add_done_callback(_ibkr_tasks.discard)

    return JSONResponse(
        content=json_safe({"status": "accepted", "broker": "IBKR", "jobs": jobs}),
        status_code=202,
    )


# -------------------------
# Bitget importer
# -------------------------

def bitget_sign(timestamp: str, method: str, request_path: str, query_string: str, body: str, secret: str):
    prehash = timestamp + method.upper() + request_path
    if query_string:
        prehash += "?" + query_string
    prehash += body
    signature = hmac.new(
        secret.encode("utf-8"),
        prehash.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    return base64.b64encode(signature).decode()


def get_bitget_creds(portfolio_name: str) -> dict:
    """Resolve the Bitget API credential set for a given portfolio.

        Global       -> BITGET_GLOBAL_API_KEY / _SECRET / _PASSPHRASE
        Alternatives -> BITGET_ALTERNATIVES_API_KEY / _SECRET / _PASSPHRASE

    NO silent fallback: if the requested set is missing or incomplete this raises
    a hard, explicit error. We deliberately do NOT fall back to the Global keys
    for Alternatives — a missing Alternatives set must fail loudly, never silently
    mirror the Global account into the Alternatives portfolio.

    The returned dict carries a human-readable 'source' name (GLOBAL/ALTERNATIVES)
    that is SAFE to log into job metadata. It is never a secret. The actual
    key/secret/passphrase values are never logged or surfaced anywhere.
    """
    prefixes = {
        "Global": "BITGET_GLOBAL_",
        "Alternatives": "BITGET_ALTERNATIVES_",
    }
    prefix = prefixes.get(portfolio_name)
    if prefix is None:
        raise RuntimeError(
            f"No Bitget credential mapping for portfolio: {portfolio_name!r}"
        )

    api_key = os.getenv(prefix + "API_KEY")
    secret = os.getenv(prefix + "SECRET")
    passphrase = os.getenv(prefix + "PASSPHRASE")

    if not api_key or not secret or not passphrase:
        # Name only the expected ENV var NAMES — never echo the (partial) values.
        raise RuntimeError(
            f"Bitget credentials missing for portfolio {portfolio_name!r} "
            f"(expected {prefix}API_KEY / {prefix}SECRET / {prefix}PASSPHRASE)"
        )

    source = "ALTERNATIVES" if portfolio_name == "Alternatives" else "GLOBAL"
    return {
        "api_key": api_key,
        "secret": secret,
        "passphrase": passphrase,
        "source": source,
    }


async def bitget_get(path: str, params: dict, creds: dict):
    # creds is a required parameter (no default): every call site must pass an
    # explicitly resolved credential set from get_bitget_creds(). This makes any
    # missed call site fail fast instead of silently signing with the wrong keys.
    api_key = creds["api_key"]
    secret = creds["secret"]
    passphrase = creds["passphrase"]

    query_string = "&".join([f"{k}={v}" for k, v in params.items() if v is not None])
    timestamp = str(int(time.time() * 1000))
    signature = bitget_sign(timestamp, "GET", path, query_string, "", secret)

    headers = {
        "ACCESS-KEY": api_key,
        "ACCESS-SIGN": signature,
        "ACCESS-TIMESTAMP": timestamp,
        "ACCESS-PASSPHRASE": passphrase,
        "Content-Type": "application/json",
        "locale": "en-US",
    }

    url = "https://api.bitget.com" + path
    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.get(url, params=params, headers=headers)
        data = response.json()
        if str(data.get("code")) != "00000":
            raise RuntimeError(f"Bitget API error: {data}")
        return data

async def fetch_bitget_tpsl_map(product_type: str, creds: dict):
    """
    Returns:
    {
        ("SUIUSDT", "long"): {
            "take_profit": Decimal(...),
            "stop_loss": Decimal(...),
            "take_profit_order_id": "...",
            "stop_loss_order_id": "...",
            "take_profit_orders": [...],
            "stop_loss_orders": [...],
        }
    }
    """
    result = {}

    try:
        response = await bitget_get(
            "/api/v2/mix/order/orders-plan-pending",
            {
                "productType": product_type,
                "planType": "profit_loss",
            },
            creds,
        )
    except Exception as e:
        print(f"Bitget TP/SL fetch failed for {product_type}: {e}")
        return result

    data = response.get("data") or {}
    orders = data.get("entrustedList") or []

    for order in orders:
        symbol = order.get("symbol")
        pos_side = (order.get("posSide") or "").lower()

        if not symbol or not pos_side:
            continue

        key = (symbol, pos_side)

        if key not in result:
            result[key] = {
                "take_profit": None,
                "stop_loss": None,
                "take_profit_order_id": None,
                "stop_loss_order_id": None,
                "take_profit_orders": [],
                "stop_loss_orders": [],
            }

        plan_type = order.get("planType")

        tp_raw = (
            order.get("stopSurplusTriggerPrice")
            or order.get("takeProfit")
            or order.get("triggerPrice")
            or None
        )

        sl_raw = (
            order.get("stopLossTriggerPrice")
            or order.get("stopLoss")
            or order.get("triggerPrice")
            or None
        )

        size_raw = order.get("size")
        order_id = order.get("orderId") or None

        if plan_type == "profit_plan" and tp_raw:
            tp_price = parse_decimal(tp_raw, None)

            tp_order = {
                "price": float(tp_price) if tp_price is not None else None,
                "size": float(parse_decimal(size_raw, 0)) if size_raw is not None else None,
                "order_id": order_id,
                "client_oid": order.get("clientOid") or None,
                "plan_type": plan_type,
                "trigger_type": order.get("triggerType") or None,
                "order_type": order.get("orderType") or None,
                "trade_side": order.get("tradeSide") or None,
                "status": order.get("planStatus") or None,
                "created_at_ms": order.get("cTime") or None,
                "updated_at_ms": order.get("uTime") or None,
            }

            result[key]["take_profit_orders"].append(tp_order)

            if result[key]["take_profit"] is None:
                result[key]["take_profit"] = tp_price
                result[key]["take_profit_order_id"] = order_id

        if plan_type == "pos_loss" and sl_raw:
            sl_price = parse_decimal(sl_raw, None)

            sl_order = {
                "price": float(sl_price) if sl_price is not None else None,
                "size": float(parse_decimal(size_raw, 0)) if size_raw is not None else None,
                "order_id": order_id,
                "client_oid": order.get("clientOid") or None,
                "plan_type": plan_type,
                "trigger_type": order.get("triggerType") or None,
                "order_type": order.get("orderType") or None,
                "trade_side": order.get("tradeSide") or None,
                "status": order.get("planStatus") or None,
                "created_at_ms": order.get("cTime") or None,
                "updated_at_ms": order.get("uTime") or None,
            }

            result[key]["stop_loss_orders"].append(sl_order)

            if result[key]["stop_loss"] is None:
                result[key]["stop_loss"] = sl_price
                result[key]["stop_loss_order_id"] = order_id

    return result


# Coins treated as 1.0 USDT (face value). Bitget's USDT/USDC settled wallets and
# common stablecoins in spot are valued at par instead of via a ticker.
BITGET_STABLE_COINS = {
    "USDT", "USDC", "DAI", "TUSD", "USDD", "FDUSD", "BUSD", "USDE", "USDP", "GUSD",
}


async def fetch_bitget_spot_price(coin: str, creds: dict) -> float:
    """
    Last spot price of `coin` expressed in USDT, via the public spot ticker.
    Stablecoins return 1.0. Raises RuntimeError if the price is unreadable so the
    caller can graceful-skip that single coin (it must NOT fail the whole snapshot).
    """
    c = (coin or "").strip().upper()
    if not c:
        raise RuntimeError("empty coin symbol")
    if c in BITGET_STABLE_COINS:
        return 1.0
    resp = await bitget_get(
        "/api/v2/spot/market/tickers",
        {"symbol": f"{c}USDT"},
        creds,
    )
    rows = resp.get("data") or []
    if isinstance(rows, dict):
        rows = [rows]
    if not rows:
        raise RuntimeError(f"no ticker for {c}USDT")
    last = parse_decimal(rows[0].get("lastPr"), None)
    if last is None or last == 0:
        raise RuntimeError(f"no usable lastPr for {c}USDT")
    return last


async def bitget_mix_equity_usdt(product_type: str, creds: dict):
    """
    Total account equity of a mix product line ('USDC-FUTURES', 'COIN-FUTURES', ...)
    expressed in USDT, plus a list of per-coin skips.

    Valuation source priority (per account row):
      1. Bitget's own `usdtEquity` field, IF present and > 0. This is the
         exchange's AUTHORITATIVE USDT valuation — we deliberately prefer it so
         that later validation against the Bitget app shows the same number
         instead of tiny drift from our own ticker math.
      2. Fallback only when usdtEquity is missing/null/0 but `accountEquity`
         (denominated in marginCoin) is present:
            - stablecoin margin (USDT/USDC/...) → par (1:1)
            - other margin coins (BTC/ETH/...) → accountEquity * spot ticker
         A single unreadable ticker skips only that coin (appended to `skipped`),
         it does NOT abort the wallet.

    !!! NOT LIVE-VERIFIED !!!
    The USDC-FUTURES / COIN-FUTURES paths cannot currently be verified: both
    wallets are empty and return 40009 (product line not activated). This logic
    is built defensively (graceful-skip) but its arithmetic is UNVALIDATED. On
    the first real USDC-M / Coin-M trade it MUST be cross-checked against the
    Bitget app to ensure it does not silently compute a wrong NAV.

    Raises only if the accounts API call itself fails (e.g. 40009 on an
    unactivated product line) so the caller can graceful-skip the whole wallet.
    Returns: (value_in_usdt: float, skipped: list[dict])
    """
    data = await bitget_get(
        "/api/v2/mix/account/accounts",
        {"productType": product_type},
        creds,
    )
    rows = data.get("data") or []
    if isinstance(rows, dict):
        rows = [rows]

    total = 0.0
    skipped = []
    for acct in rows:
        # 1. Prefer Bitget's authoritative USDT valuation.
        usdt_equity = parse_decimal(acct.get("usdtEquity"), 0)
        if usdt_equity > 0:
            total += usdt_equity
            continue

        # 2. Fallback: value the margin-coin equity ourselves.
        equity = parse_decimal(acct.get("accountEquity"), 0)
        if equity == 0:
            continue
        coin = (acct.get("marginCoin") or "").strip().upper()
        if coin in BITGET_STABLE_COINS:
            total += equity
            continue
        try:
            price = await fetch_bitget_spot_price(coin, creds)
            total += equity * price
        except Exception as e:
            skipped.append({"wallet": product_type, "coin": coin, "reason": str(e)[:160]})
    return total, skipped


async def bitget_spot_value_usdt(creds: dict):
    """
    Total spot wallet value in USDT: stablecoins at face value, other coins via
    spot ticker. A single unreadable ticker skips only that coin.

    Raises only if the spot assets API call itself fails so the caller can
    graceful-skip the whole spot wallet.
    Returns: (value_in_usdt: float, skipped: list[dict])
    """
    data = await bitget_get("/api/v2/spot/account/assets", {}, creds)
    rows = data.get("data") or []
    if isinstance(rows, dict):
        rows = [rows]

    total = 0.0
    skipped = []
    for asset in rows:
        coin = (asset.get("coin") or "").strip().upper()
        amount = (
            parse_decimal(asset.get("available"), 0)
            + parse_decimal(asset.get("frozen"), 0)
            + parse_decimal(asset.get("locked"), 0)
        )
        if amount == 0:
            continue
        if coin in BITGET_STABLE_COINS:
            total += amount
            continue
        try:
            price = await fetch_bitget_spot_price(coin, creds)
            total += amount * price
        except Exception as e:
            skipped.append({"wallet": "SPOT", "coin": coin, "reason": str(e)[:160]})
    return total, skipped


@app.post("/debug/bitget/account")
async def debug_bitget_account(x_admin_token: Optional[str] = Header(None)):
    try:
        require_admin_token(x_admin_token)
    except PermissionError:
        return JSONResponse(content={"error": "Unauthorized"}, status_code=401)

    results = {}
    creds = get_bitget_creds("Global")

    try:
        for product_type in ["USDT-FUTURES", "USDC-FUTURES", "COIN-FUTURES"]:
            try:
                account_data = await bitget_get(
                    "/api/v2/mix/account/accounts",
                    {"productType": product_type},
                    creds,
                )

                positions_data = await bitget_get(
                    "/api/v2/mix/position/all-position",
                    {
                        "productType": product_type,
                        "marginCoin": "USDT" if product_type == "USDT-FUTURES" else None,
                    },
                    creds,
                )

                results[product_type] = {
                    "account_status": "success",
                    "account_sample": account_data.get("data"),
                    "positions_status": "success",
                    "positions_sample": positions_data.get("data"),
                }

            except Exception as inner_e:
                results[product_type] = {
                    "status": "failed",
                    "error": str(inner_e),
                }

        return JSONResponse(content={"status": "success", "results": results})

    except Exception as e:
        return JSONResponse(content={"status": "failed", "error": str(e)}, status_code=500)


@app.post("/debug/verify/separation")
async def debug_verify_separation(x_admin_token: Optional[str] = Header(None)):
    """Read-only Vorab-Verifikation des Cutovers: beweist, dass Global und
    Alternatives auf physisch GETRENNTE Konten zeigen — OHNE echte
    Kontonummern oder Secrets auszugeben. Kein DB-Write.

    IBKR: zieht beide Flex-Queries, gibt je einen anonymisierten
      Diskriminator SHA256(accountId)[:12] zurück + accounts_differ.
      Roh-ErrorMessage von IBKR wird unmaskiert durchgereicht (zur Diagnose).
    Bitget: nur Boolean global_key_equals_alternatives + je USDT-Equity,
      plus (falls Spot-Scope aktiv) anonymisierter userId-Vergleich.
      Key/Secret/Passphrase werden NIEMALS ausgegeben.
    """
    try:
        require_admin_token(x_admin_token)
    except PermissionError:
        return JSONResponse(content={"error": "Unauthorized"}, status_code=401)

    def _h(value: str) -> str:
        return hashlib.sha256(str(value).encode()).hexdigest()[:12]

    # ---------- IBKR ----------
    ibkr: dict = {}
    for label, env_name in [
        ("Global", "IBKR_ACTIVITY_QUERY_ID_GLOBAL"),
        ("Alternatives", "IBKR_ACTIVITY_QUERY_ID_ALTERNATIVES"),
    ]:
        entry: dict = {}
        query_id = os.getenv(env_name)
        entry["query_present"] = bool(query_id)
        if not query_id:
            entry["error"] = f"{env_name} missing"
            ibkr[label] = entry
            continue
        try:
            xml_text = await fetch_ibkr_flex_report(query_id)
            root = ET.fromstring(xml_text)
            account_ids: set = set()
            ibkr_error_code = None
            ibkr_error_message = None
            for elem in root.iter():
                tag = elem.tag.split("}")[-1].lower()
                if tag == "errorcode":
                    ibkr_error_code = elem.text
                elif tag == "errormessage":
                    ibkr_error_message = elem.text
                for k, v in elem.attrib.items():
                    if k.lower() in ("accountid", "accountnumber", "acctid") and v:
                        account_ids.add(v)
            entry["account_hashes"] = sorted(_h(a) for a in account_ids)
            entry["account_count"] = len(account_ids)
            if ibkr_error_code or ibkr_error_message:
                # Roh durchgereicht (kein Kontonummer-Leak: IBKR-Fehlertexte
                # enthalten keine Kontonummern).
                entry["ibkr_error_code"] = ibkr_error_code
                entry["ibkr_error_message"] = ibkr_error_message
        except Exception as e:
            entry["error"] = str(e)
        ibkr[label] = entry

    g_hashes = set(ibkr.get("Global", {}).get("account_hashes") or [])
    a_hashes = set(ibkr.get("Alternatives", {}).get("account_hashes") or [])
    if g_hashes and a_hashes:
        ibkr["accounts_differ"] = g_hashes.isdisjoint(a_hashes)
    else:
        ibkr["accounts_differ"] = None  # unbekannt (mind. eine Query lieferte kein Statement)

    # ---------- Bitget ----------
    bitget: dict = {}
    try:
        gc = get_bitget_creds("Global")
        ac = get_bitget_creds("Alternatives")
        bitget["global_key_equals_alternatives"] = (gc["api_key"] == ac["api_key"])

        spot_user_hashes: dict = {}
        for label, creds in [("Global", gc), ("Alternatives", ac)]:
            side: dict = {}
            try:
                acct = await bitget_get(
                    "/api/v2/mix/account/accounts",
                    {"productType": "USDT-FUTURES"},
                    creds,
                )
                data = acct.get("data") or []
                side["usdt_futures_equity"] = data[0].get("accountEquity") if data else None
            except Exception as ex:
                side["equity_error"] = str(ex)
            try:
                spot = await bitget_get("/api/v2/spot/account/info", {}, creds)
                uid = (spot.get("data") or {}).get("userId")
                if uid:
                    h = _h(uid)
                    side["spot_user_hash"] = h
                    spot_user_hashes[label] = h
                else:
                    side["spot_user_hash"] = None
            except Exception as ex:
                side["spot_error"] = str(ex)
            bitget[label] = side

        if "Global" in spot_user_hashes and "Alternatives" in spot_user_hashes:
            bitget["spot_user_differs"] = (
                spot_user_hashes["Global"] != spot_user_hashes["Alternatives"]
            )
        else:
            bitget["spot_user_differs"] = None  # Spot-Scope nicht aktiv → kein definitiver Beweis
    except Exception as e:
        bitget["error"] = str(e)

    return JSONResponse(content={"status": "success", "ibkr": ibkr, "bitget": bitget})


def bitget_side_to_side(row: dict):
    side = (
        row.get("side")
        or row.get("tradeSide")
        or row.get("posSide")
        or ""
    ).lower()
    if "buy" in side or "open_long" in side or "close_short" in side:
        return "BUY"
    if "sell" in side or "open_short" in side or "close_long" in side:
        return "SELL"
    return side.upper() or "UNKNOWN"


async def upsert_bitget_rows(conn, portfolio_name: str, rows: list, product_type: str):
    portfolio_id = await get_portfolio_id(conn, portfolio_name)

    rows_seen = 0
    rows_inserted = 0
    rows_updated = 0

    for row in rows:
        rows_seen += 1

        raw_trade_id = (
            row.get("tradeId")
            or row.get("fillId")
            or row.get("orderId")
            or f"{product_type}-{rows_seen}-{row.get('cTime','')}"
        )

        trade_id = f"{portfolio_name}:{raw_trade_id}"
        symbol = row.get("symbol") or row.get("instId") or "UNKNOWN"
        side = bitget_side_to_side(row)
        quantity = parse_decimal(row.get("baseVolume") or row.get("size") or row.get("qty"))
        price = parse_decimal(row.get("price"))
        fee = parse_decimal(row.get("fee"), 0)
        currency = row.get("feeCoin") or row.get("marginCoin") or "USDT"

        trade_time_ms = row.get("cTime") or row.get("uTime") or row.get("tradeTime")
        try:
            trade_time = datetime.fromtimestamp(int(trade_time_ms) / 1000, tz=timezone.utc)
        except Exception:
            trade_time = datetime.now(timezone.utc)

        existing_trade = await conn.fetchrow(
            """
            SELECT id
            FROM public.trades
            WHERE broker = 'Bitget'
            AND external_trade_id = $1
            """,
            trade_id,
        )
        
        result = await conn.execute(
            """
            INSERT INTO public.trades (
                portfolio_id, broker, symbol, asset_class, instrument_type,
                side, quantity, price, currency, fee, trade_date,
                execution_time, external_trade_id, raw_payload
            )
            VALUES (
                $1, 'Bitget', $2, 'Crypto Futures', $3,
                $4, $5, $6, $7, $8, $9,
                $9, $10, $11::jsonb
            )
            ON CONFLICT (broker, external_trade_id)
            DO UPDATE SET
                portfolio_id = EXCLUDED.portfolio_id,
                symbol = EXCLUDED.symbol,
                asset_class = EXCLUDED.asset_class,
                instrument_type = EXCLUDED.instrument_type,
                side = EXCLUDED.side,
                quantity = EXCLUDED.quantity,
                price = EXCLUDED.price,
                currency = EXCLUDED.currency,
                fee = EXCLUDED.fee,
                trade_date = EXCLUDED.trade_date,
                execution_time = EXCLUDED.execution_time,
                raw_payload = EXCLUDED.raw_payload,
                imported_at = now()
            """,
            portfolio_id,
            symbol,
            product_type,
            side,
            quantity,
            price,
            currency,
            fee,
            trade_time,
            trade_id,
            json.dumps(row),
        )

        if existing_trade:
            rows_updated += 1
        else:
            rows_inserted += 1

    return {
        "portfolio": portfolio_name,
        "product_type": product_type,
        "rows_seen": rows_seen,
        "rows_inserted": rows_inserted,
        "rows_updated": rows_updated,
    }

async def upsert_bitget_snapshot_and_positions(conn, portfolio_name: str, creds: dict):
    portfolio_id = await get_portfolio_id(conn, portfolio_name)

    product_type = "USDT-FUTURES"
    margin_coin = "USDT"

    account_data = await bitget_get(
        "/api/v2/mix/account/accounts",
        {"productType": product_type},
        creds,
    )

    positions_data = await bitget_get(
        "/api/v2/mix/position/all-position",
        {
            "productType": product_type,
            "marginCoin": margin_coin,
        },
        creds,
    )

    # TP/SL orders are not always attached directly to the position.
    # Bitget exposes active TP/SL through plan orders with planType=profit_loss.
    tpsl_map = await fetch_bitget_tpsl_map(product_type, creds)

    accounts = account_data.get("data") or []
    if isinstance(accounts, dict):
        accounts = [accounts]

    account = None
    for item in accounts:
        if item.get("marginCoin") == margin_coin:
            account = item
            break

    if account is None and accounts:
        account = accounts[0]

    if account is None:
        return {
            "portfolio": portfolio_name,
            "snapshot_imported": False,
            "reason": "No Bitget account data found",
        }

    now_dt = datetime.now(timezone.utc)
    snapshot_date = now_dt.date()

    # --- Composite NAV in USDT ----------------------------------------------
    # nav = USDT-FUTURES equity (base, just fetched)
    #     + USDC-FUTURES equity (USDC ~= 1:1 USDT)
    #     + COIN-FUTURES equity (coin amount * spot ticker)
    #     + Spot wallet (stables at face value + coins * spot ticker)
    #
    # Each non-base wallet is valued with graceful-skip: a 40009 / unreadable
    # wallet (or a single unreadable coin ticker) is logged in
    # `skipped_valuations` and excluded from the sum — it NEVER fails the whole
    # snapshot. Exactly ONE row per (portfolio,'Bitget',date,currency='USDT')
    # is written, so v_portfolio_daily_nav (LIMIT 1) and TWR/MWR stay correct.
    usdt_futures_equity = parse_decimal(
        account.get("accountEquity") or account.get("usdtEquity"), 0
    )
    cash = parse_decimal(account.get("available"), 0)
    open_pnl = parse_decimal(account.get("unrealizedPL"), 0)

    composite_nav = usdt_futures_equity
    nav_breakdown = {"USDT-FUTURES": usdt_futures_equity}
    skipped_valuations = []

    # USDC-FUTURES / COIN-FUTURES below: NOT LIVE-VERIFIED. Both wallets are
    # currently empty (40009, product line not activated), so this valuation
    # has never run against real balances. It prefers Bitget's own usdtEquity
    # (authoritative) and only falls back to our coin*ticker math. On the first
    # real USDC-M / Coin-M trade, cross-check the resulting NAV against the
    # Bitget app before trusting it. See bitget_mix_equity_usdt() docstring.
    #
    # USDC-FUTURES (USDC ~= 1:1 USDT)
    try:
        v, sk = await bitget_mix_equity_usdt("USDC-FUTURES", creds)
        composite_nav += v
        nav_breakdown["USDC-FUTURES"] = v
        skipped_valuations.extend(sk)
    except Exception as e:
        skipped_valuations.append({"wallet": "USDC-FUTURES", "reason": str(e)[:200]})

    # COIN-FUTURES (coin-margined; prefers usdtEquity, else equity-in-coin * ticker)
    try:
        v, sk = await bitget_mix_equity_usdt("COIN-FUTURES", creds)
        composite_nav += v
        nav_breakdown["COIN-FUTURES"] = v
        skipped_valuations.extend(sk)
    except Exception as e:
        skipped_valuations.append({"wallet": "COIN-FUTURES", "reason": str(e)[:200]})

    # Spot wallet (stables face value + coins * spot ticker)
    try:
        v, sk = await bitget_spot_value_usdt(creds)
        composite_nav += v
        nav_breakdown["SPOT"] = v
        skipped_valuations.extend(sk)
    except Exception as e:
        skipped_valuations.append({"wallet": "SPOT", "reason": str(e)[:200]})

    nav = composite_nav
    # market_value = everything that is not USDT-FUTURES free cash (positions +
    # all non-base wallets). cash / open_pnl stay USDT-FUTURES-native.
    market_value = nav - cash

    await conn.execute(
        """
        INSERT INTO public.portfolio_nav_snapshots (
            portfolio_id, broker, snapshot_date, currency,
            nav, cash, market_value, open_pnl, closed_pnl,
            deposits_withdrawals, source
        )
        VALUES (
            $1, 'Bitget', $2, 'USDT',
            $3, $4, $5, $6, NULL,
            NULL, 'bitget_account_snapshot'
        )
        ON CONFLICT (portfolio_id, broker, snapshot_date, currency)
        DO UPDATE SET
            nav = EXCLUDED.nav,
            cash = EXCLUDED.cash,
            market_value = EXCLUDED.market_value,
            open_pnl = EXCLUDED.open_pnl,
            source = EXCLUDED.source,
            created_at = now()
        """,
        portfolio_id,
        snapshot_date,
        nav,
        cash,
        market_value,
        open_pnl,
    )

    await conn.execute(
        """
        INSERT INTO public.portfolio_cash (
            portfolio_id, broker, currency, cash_balance,
            cash_balance_base, updated_at
        )
        VALUES ($1, 'Bitget', 'USDT', $2, $2, now())
        ON CONFLICT (portfolio_id, broker, currency)
        DO UPDATE SET
            cash_balance = EXCLUDED.cash_balance,
            cash_balance_base = EXCLUDED.cash_balance_base,
            updated_at = now()
        """,
        portfolio_id,
        cash,
    )

    await conn.execute(
        """
        DELETE FROM public.positions
        WHERE portfolio_id = $1
        AND broker = 'Bitget'
        """,
        portfolio_id,
    )

    positions = positions_data.get("data") or []
    if isinstance(positions, dict):
        positions = [positions]

    positions_seen = 0
    positions_inserted = 0

    for pos in positions:
        symbol = pos.get("symbol") or "UNKNOWN"
        total = parse_decimal(pos.get("total"), 0)

        # Skip empty/closed positions
        if total == 0:
            continue

        positions_seen += 1

        hold_side = (pos.get("holdSide") or "").lower()
        quantity = total
        if hold_side == "short":
            quantity = -abs(quantity)

        position_side = "SHORT" if hold_side == "short" else "LONG" if hold_side == "long" else None

        avg_cost = parse_decimal(pos.get("openPriceAvg"), 0)
        mark_price = parse_decimal(pos.get("markPrice"), 0)
        open_pnl_native = parse_decimal(pos.get("unrealizedPL"), 0)

        tpsl = tpsl_map.get((symbol, hold_side), {})

        take_profit = (
            parse_decimal(pos.get("takeProfit"), None)
            or tpsl.get("take_profit")
        )
        stop_loss = (
            parse_decimal(pos.get("stopLoss"), None)
            or tpsl.get("stop_loss")
        )
        take_profit_order_id = (
            pos.get("takeProfitId")
            or tpsl.get("take_profit_order_id")
            or None
        )
        stop_loss_order_id = (
            pos.get("stopLossId")
            or tpsl.get("stop_loss_order_id")
            or None
        )

        source_position_id = (
            pos.get("posId")
            or pos.get("positionId")
            or f"{product_type}:{symbol}:{hold_side}"
        )

        entry_time_raw = pos.get("cTime") or pos.get("uTime")
        try:
            entry_date = datetime.fromtimestamp(int(entry_time_raw) / 1000, tz=timezone.utc)
        except Exception:
            entry_date = None

        # For USDT futures, approximate position notional as quantity * mark price.
        market_value_native = abs(quantity * mark_price)
        market_value_base = market_value_native
        open_pnl_base = open_pnl_native

        take_profit_orders = tpsl.get("take_profit_orders") or []
        stop_loss_orders = tpsl.get("stop_loss_orders") or []

        await conn.execute(
            """
            INSERT INTO public.positions (
                portfolio_id, broker, symbol, asset_class,
                quantity, avg_cost, currency, market_price,
                market_value_native, market_value_base,
                open_pnl_native, open_pnl_base,
                entry_date, position_side,
                take_profit, stop_loss,
                take_profit_order_id, stop_loss_order_id,
                source_position_id,
                updated_at,
                take_profit_orders,
                stop_loss_orders
            )
            VALUES (
                $1, 'Bitget', $2, 'Crypto Futures',
                $3, $4, 'USDT', $5,
                $6, $7,
                $8, $9,
                $10, $11,
                $12, $13,
                $14, $15,
                $16,
                now(),
                $17::jsonb,
                $18::jsonb
            )
            ON CONFLICT (portfolio_id, broker, symbol)
            DO UPDATE SET
                asset_class = EXCLUDED.asset_class,
                quantity = EXCLUDED.quantity,
                avg_cost = EXCLUDED.avg_cost,
                currency = EXCLUDED.currency,
                market_price = EXCLUDED.market_price,
                market_value_native = EXCLUDED.market_value_native,
                market_value_base = EXCLUDED.market_value_base,
                open_pnl_native = EXCLUDED.open_pnl_native,
                open_pnl_base = EXCLUDED.open_pnl_base,
                entry_date = EXCLUDED.entry_date,
                position_side = EXCLUDED.position_side,
                take_profit = EXCLUDED.take_profit,
                stop_loss = EXCLUDED.stop_loss,
                take_profit_order_id = EXCLUDED.take_profit_order_id,
                stop_loss_order_id = EXCLUDED.stop_loss_order_id,
                source_position_id = EXCLUDED.source_position_id,
                updated_at = now(),
                take_profit_orders = EXCLUDED.take_profit_orders,
                stop_loss_orders = EXCLUDED.stop_loss_orders
            """,
            portfolio_id,
            symbol,
            quantity,
            avg_cost,
            mark_price,
            market_value_native,
            market_value_base,
            open_pnl_native,
            open_pnl_base,
            entry_date,
            position_side,
            take_profit,
            stop_loss,
            take_profit_order_id,
            stop_loss_order_id,
            source_position_id,
            json.dumps(take_profit_orders),
            json.dumps(stop_loss_orders),
        )

        positions_inserted += 1

    return {
        "portfolio": portfolio_name,
        "snapshot_imported": True,
        "currency": "USDT",
        "nav": nav,
        "nav_usdt_futures": usdt_futures_equity,
        "nav_breakdown": nav_breakdown,
        "skipped_valuations": skipped_valuations,
        "cash": cash,
        "market_value": market_value,
        "open_pnl": open_pnl,
        "positions_seen": positions_seen,
        "positions_inserted": positions_inserted,
        "tpsl_orders_seen": len(tpsl_map),
    }


async def upsert_bitget_cashflows(
    conn,
    portfolio_name: str,
    portfolio_id,
    base_currency: str,
    creds: dict,
) -> dict:
    """
    Import Bitget deposits and withdrawals into portfolio_cashflows.

    Strategy:
    - Loops in 90-day windows from 2024-01-01 to today.
    - Uses idLessThan cursor pagination within each window (newest → oldest).
    - Only records with status='success' are imported.
    - Deposits  → positive amount_native (money entering portfolio).
    - Withdrawals → negative amount_native = -(size - fee), consistent with MT5.
    - amount_base = amount_native * FX rate (coin → base_currency).
    - If no FX rate is found: amount_base = NULL, warning is printed.
    """
    today = datetime.now(timezone.utc).date()
    window_start = date(2024, 1, 1)

    cashflows_seen = 0
    cashflows_inserted = 0
    cashflows_updated = 0
    cashflows_skipped_no_fx = 0

    endpoints = [
        (
            "/api/v2/spot/wallet/deposit-records",
            "DEPOSIT",
            "bitget_deposit_record",
        ),
        (
            "/api/v2/spot/wallet/withdrawal-records",
            "WITHDRAWAL",
            "bitget_withdrawal_record",
        ),
    ]

    for api_path, cashflow_type, source_key in endpoints:
        current_start = window_start

        while current_start <= today:
            current_end = min(current_start + timedelta(days=89), today)

            start_ms = str(int(
                datetime(
                    current_start.year,
                    current_start.month,
                    current_start.day,
                    tzinfo=timezone.utc,
                ).timestamp() * 1000
            ))
            end_ms = str(int(
                datetime(
                    current_end.year,
                    current_end.month,
                    current_end.day,
                    23, 59, 59,
                    tzinfo=timezone.utc,
                ).timestamp() * 1000
            ))

            id_less_than = None

            while True:
                params: dict = {
                    "startTime": start_ms,
                    "endTime": end_ms,
                    "limit": "100",
                }
                if id_less_than is not None:
                    params["idLessThan"] = id_less_than

                data = await bitget_get(api_path, params, creds)
                records = data.get("data") or []
                if isinstance(records, dict):
                    records = (
                        records.get("depositList")
                        or records.get("withdrawList")
                        or records.get("list")
                        or []
                    )

                if not records:
                    break

                for record in records:
                    cashflows_seen += 1

                    status = (record.get("status") or "").lower()
                    if status != "success":
                        continue

                    order_id = record.get("orderId") or record.get("id") or ""
                    coin = (record.get("coin") or "USDT").upper()
                    size = parse_decimal(record.get("size"), 0)

                    ctime_ms = record.get("cTime")
                    try:
                        cashflow_dt = datetime.fromtimestamp(
                            int(ctime_ms) / 1000, tz=timezone.utc
                        )
                    except Exception:
                        cashflow_dt = datetime.now(timezone.utc)

                    cashflow_date = cashflow_dt.date()

                    if cashflow_type == "DEPOSIT":
                        amount_native = size
                        external_id = f"Bitget:{portfolio_name}:deposit:{order_id}"
                    else:
                        # Withdrawal: net = gross - fee; stored negative (money out).
                        fee = parse_decimal(record.get("fee") or "0", 0)
                        amount_native = -(size - fee)
                        external_id = f"Bitget:{portfolio_name}:withdrawal:{order_id}"

                    # FX conversion: coin → portfolio base currency
                    fx_rate = await get_fx_rate(conn, coin, base_currency, cashflow_date)

                    if fx_rate is None:
                        amount_base = None
                        cashflows_skipped_no_fx += 1
                        print(
                            f"⚠️ WARNING: No FX rate for {coin}→{base_currency} "
                            f"on {cashflow_date} — amount_base set to NULL | {external_id}"
                        )
                    else:
                        amount_base = amount_native * fx_rate

                    # Track insert vs. update for counters
                    existing = await conn.fetchrow(
                        """
                        SELECT id FROM public.portfolio_cashflows
                        WHERE portfolio_id = $1
                          AND broker       = 'Bitget'
                          AND source       = $2
                          AND external_id  = $3
                        """,
                        portfolio_id,
                        source_key,
                        external_id,
                    )

                    await conn.execute(
                        """
                        INSERT INTO public.portfolio_cashflows (
                            portfolio_id, broker, cashflow_date, currency,
                            amount_native, amount_base,
                            cashflow_type, source, external_id, raw_payload,
                            updated_at
                        )
                        VALUES (
                            $1, 'Bitget', $2, $3,
                            $4, $5,
                            $6, $7, $8, $9::jsonb,
                            now()
                        )
                        ON CONFLICT (portfolio_id, broker, source, external_id)
                        DO UPDATE SET
                            cashflow_date = EXCLUDED.cashflow_date,
                            currency      = EXCLUDED.currency,
                            amount_native = EXCLUDED.amount_native,
                            amount_base   = EXCLUDED.amount_base,
                            cashflow_type = EXCLUDED.cashflow_type,
                            raw_payload   = EXCLUDED.raw_payload,
                            updated_at    = now()
                        """,
                        portfolio_id,
                        cashflow_date,
                        coin,
                        amount_native,
                        amount_base,
                        cashflow_type,
                        source_key,
                        external_id,
                        json.dumps(record),
                    )

                    if existing:
                        cashflows_updated += 1
                    else:
                        cashflows_inserted += 1

                # Cursor pagination: fewer than 100 records means last page.
                if len(records) < 100:
                    break

                # Next page: pass the smallest orderId as idLessThan.
                try:
                    id_less_than = str(
                        min(int(r.get("orderId") or "0") for r in records)
                    )
                except Exception:
                    break

            current_start = current_end + timedelta(days=1)

    return {
        "portfolio": portfolio_name,
        "cashflows_seen": cashflows_seen,
        "cashflows_inserted": cashflows_inserted,
        "cashflows_updated": cashflows_updated,
        "cashflows_skipped_no_fx": cashflows_skipped_no_fx,
    }


async def upsert_bitget_funding_fees(
    conn,
    portfolio_name: str,
    portfolio_id,
    base_currency: str,
    creds: dict,
) -> dict:
    """
    Import Bitget perpetual-futures funding fees into realized_pnl_events.

    Funding fees are trading P&L, not capital flows — they must NOT go into
    portfolio_cashflows, which feeds the TWR/performance calculation as external
    capital adjustments. The NAV snapshot already reflects funding fees in the
    account balance; storing them in portfolio_cashflows would hide losses from
    performance and inflate net_profit.

    realized_pnl_events is isolated from the TWR chain and feeds only analytics
    views (v_closed_positions_public, v_closed_positions_detail_public,
    v_public_portfolio_summary.closed_pnl).

    Strategy:
    - Loops in 90-day windows from 2025-08-01 to today
      (first funding fee observed 2025-08-27; 2025-08-01 gives a safe margin).
    - Iterates over all three product types: USDT-FUTURES, USDC-FUTURES, COIN-FUTURES.
    - Uses idLessThan cursor pagination within each window (newest → oldest).
    - Endpoint: GET /api/v2/mix/account/bill with businessType=contract_settle_fee
    - coin field is 'USDT' for USDT-FUTURES, 'USDC' for USDC-FUTURES,
      and the base asset (BTC, ETH …) for COIN-FUTURES.
    - amount is already signed: negative = paid out, positive = received.
    - realized_pnl_base = realized_pnl * FX rate (coin → base_currency).
    - If no FX rate is found: realized_pnl_base = NULL, warning is printed.
    - UPSERT key: UNIQUE (broker, external_id).
    """
    today = datetime.now(timezone.utc).date()
    # First funding fee observed 2025-08-27; start 2025-08-01 for safety.
    window_start = date(2025, 8, 1)

    funding_fees_seen = 0
    funding_fees_inserted = 0
    funding_fees_updated = 0
    funding_fees_skipped_no_fx = 0

    product_types = ["USDT-FUTURES", "USDC-FUTURES", "COIN-FUTURES"]

    for product_type in product_types:
        current_start = window_start

        while current_start <= today:
            current_end = min(current_start + timedelta(days=89), today)

            start_ms = str(int(
                datetime(
                    current_start.year,
                    current_start.month,
                    current_start.day,
                    tzinfo=timezone.utc,
                ).timestamp() * 1000
            ))
            end_ms = str(int(
                datetime(
                    current_end.year,
                    current_end.month,
                    current_end.day,
                    23, 59, 59,
                    tzinfo=timezone.utc,
                ).timestamp() * 1000
            ))

            id_less_than = None

            while True:
                params: dict = {
                    "productType": product_type,
                    "businessType": "contract_settle_fee",
                    "startTime": start_ms,
                    "endTime": end_ms,
                    "limit": "100",
                }
                if id_less_than is not None:
                    params["idLessThan"] = id_less_than

                data = await bitget_get("/api/v2/mix/account/bill", params, creds)
                records = data.get("data") or []
                if isinstance(records, dict):
                    records = records.get("bills") or records.get("list") or []

                # Defensive: skip non-dict entries (malformed API responses)
                records = [r for r in records if isinstance(r, dict)]

                if not records:
                    break

                for bill in records:
                    funding_fees_seen += 1

                    bill_id   = bill.get("billId") or bill.get("id") or ""
                    symbol    = bill.get("symbol") or ""
                    coin      = (bill.get("coin") or "USDT").upper()
                    realized_pnl = parse_decimal(bill.get("amount"), 0)

                    ctime_ms = bill.get("cTime")
                    try:
                        event_time = datetime.fromtimestamp(
                            int(ctime_ms) / 1000, tz=timezone.utc
                        )
                    except Exception:
                        event_time = datetime.now(timezone.utc)

                    event_date  = event_time.date()
                    external_id = f"Bitget:{portfolio_name}:funding_fee:{bill_id}"

                    # FX conversion: coin → portfolio base currency
                    fx_rate = await get_fx_rate(conn, coin, base_currency, event_date)

                    if fx_rate is None:
                        realized_pnl_base = None
                        funding_fees_skipped_no_fx += 1
                        print(
                            f"⚠️ WARNING: No FX rate for {coin}→{base_currency} "
                            f"on {event_date} — realized_pnl_base set to NULL | {external_id}"
                        )
                    else:
                        realized_pnl_base = realized_pnl * fx_rate

                    # Track insert vs. update for counters
                    existing = await conn.fetchrow(
                        """
                        SELECT id FROM public.realized_pnl_events
                        WHERE broker      = 'Bitget'
                          AND external_id = $1
                        """,
                        external_id,
                    )

                    await conn.execute(
                        """
                        INSERT INTO public.realized_pnl_events (
                            portfolio_id, broker, symbol, asset_class,
                            realized_pnl, currency, realized_pnl_base,
                            event_time, external_id, raw_payload
                        )
                        VALUES (
                            $1, 'Bitget', $2, 'FundingFee',
                            $3, $4, $5,
                            $6, $7, $8::jsonb
                        )
                        ON CONFLICT (broker, external_id)
                        DO UPDATE SET
                            portfolio_id      = EXCLUDED.portfolio_id,
                            symbol            = EXCLUDED.symbol,
                            realized_pnl      = EXCLUDED.realized_pnl,
                            currency          = EXCLUDED.currency,
                            realized_pnl_base = EXCLUDED.realized_pnl_base,
                            event_time        = EXCLUDED.event_time,
                            raw_payload       = EXCLUDED.raw_payload
                        """,
                        portfolio_id,
                        symbol,
                        realized_pnl,
                        coin,
                        realized_pnl_base,
                        event_time,
                        external_id,
                        json.dumps(bill),
                    )

                    if existing:
                        funding_fees_updated += 1
                    else:
                        funding_fees_inserted += 1

                # Fewer than 100 records → last page of this window.
                if len(records) < 100:
                    break

                # Next page: smallest billId becomes idLessThan cursor.
                try:
                    id_less_than = str(
                        min(int(r.get("billId") or "0") for r in records)
                    )
                except Exception:
                    break

            current_start = current_end + timedelta(days=1)

    return {
        "portfolio": portfolio_name,
        "funding_fees_seen": funding_fees_seen,
        "funding_fees_inserted": funding_fees_inserted,
        "funding_fees_updated": funding_fees_updated,
        "funding_fees_skipped_no_fx": funding_fees_skipped_no_fx,
    }


@app.post("/debug/bitget/tpsl")
async def debug_bitget_tpsl(x_admin_token: Optional[str] = Header(None)):
    try:
        require_admin_token(x_admin_token)
    except PermissionError:
        return JSONResponse(content={"error": "Unauthorized"}, status_code=401)

    product_types = ["USDT-FUTURES"]

    plan_types = [
        "normal_plan",
        "track_plan",
        "profit_loss",
        "profit_plan",
        "loss_plan",
        "pos_profit",
        "pos_loss",
    ]

    results = {}
    creds = get_bitget_creds("Global")

    for product_type in product_types:
        product_result = {}

        try:
            product_result["orders_pending"] = await bitget_get(
                "/api/v2/mix/order/orders-pending",
                {
                    "productType": product_type,
                },
                creds,
            )
        except Exception as e:
            product_result["orders_pending_error"] = str(e)

        product_result["plan_orders"] = {}

        for plan_type in plan_types:
            try:
                product_result["plan_orders"][plan_type] = await bitget_get(
                    "/api/v2/mix/order/orders-plan-pending",
                    {
                        "productType": product_type,
                        "planType": plan_type,
                    },
                    creds,
                )
            except Exception as e:
                product_result["plan_orders"][f"{plan_type}_error"] = str(e)

        results[product_type] = product_result

    return JSONResponse(content=json_safe({
        "status": "success",
        "results": results,
    }))


BITGET_LOCK_KEY = "sync:bitget"  # advisory-lock key, shared by handler + worker

# asyncio only keeps weak references to tasks, so a fire-and-forget task can be
# garbage-collected mid-run. Hold a strong reference until it finishes.
_bitget_tasks: set = set()


async def run_bitget_sync_job(jobs: list):
    """Background worker for /sync/bitget.

    Opens and OWNS its own fresh connection for its entire lifetime. The two
    import_jobs rows were already created (status='started') and COMMITTED by
    the request handler; this worker only receives their ids + portfolio names
    and drives the actual import, finishing each job independently.

    No advisory lock is held here. Concurrency is gated entirely in the handler
    by a transaction-scoped lock + a row-based "is one already running?" check —
    the only approach that survives the Supavisor transaction pooler (a
    session-scoped pg_advisory_lock does not, because each autocommit statement
    can land on a different pooled backend).

    Contract:
      * Each portfolio runs in its own try/except — one failing does NOT
        abort the other.
      * The `finally` block ALWAYS closes the connection. On a hard crash the
        jobs stay 'started' and are reclaimed by the startup sweeper once they
        exceed STALE_JOB_MINUTES (and the handler's row check stops treating
        them as live past the same threshold).
    """
    conn = await get_conn()
    try:
        product_types = ["USDT-FUTURES", "USDC-FUTURES", "COIN-FUTURES"]

        for job in jobs:
            portfolio_name = job["portfolio"]
            job_id = job["job_id"]
            try:
                # Stamp the moment THIS worker actually starts this portfolio.
                # Both jobs were created up front sharing one started_at, but are
                # processed sequentially, so started_at would overstate the second
                # job's age. The sweeper + lock-check read
                # COALESCE(work_started_at, started_at), so this keeps each job's
                # measured age tied to its OWN work. started_at stays as the
                # acceptance time. finish_import_job later replaces metadata, but
                # by then status != 'started' so neither check looks at this row.
                await conn.execute(
                    """
                    UPDATE public.import_jobs
                    SET metadata = COALESCE(metadata, '{}'::jsonb)
                                   || jsonb_build_object('work_started_at', now())
                    WHERE id = $1
                    """,
                    job_id,
                )

                # Resolve portfolio_id and base_currency from DB (not hardcoded).
                portfolio_row = await conn.fetchrow(
                    "SELECT id, base_currency FROM public.portfolios WHERE name = $1",
                    portfolio_name,
                )
                if not portfolio_row:
                    raise RuntimeError(f"Portfolio not found: {portfolio_name}")
                portfolio_id = portfolio_row["id"]
                base_currency = portfolio_row["base_currency"]

                # Resolve the per-portfolio Bitget credential set ONCE, then
                # thread it through every downstream call. No silent fallback: a
                # missing Alternatives set raises here and fails THIS portfolio's
                # job (caught below) — it never mirrors the Global account.
                creds = get_bitget_creds(portfolio_name)

                portfolio_seen = 0
                portfolio_inserted = 0
                portfolio_updated = 0
                results = []  # per-portfolio — no cross-contamination in metadata

                for product_type in product_types:
                    data = await bitget_get(
                        "/api/v2/mix/order/fill-history",
                        {"productType": product_type, "limit": "100"},
                        creds,
                    )

                    rows = data.get("data") or []
                    if isinstance(rows, dict):
                        rows = rows.get("fillList") or rows.get("list") or []

                    result = await upsert_bitget_rows(
                        conn,
                        portfolio_name,
                        rows,
                        product_type,
                    )

                    portfolio_seen += result["rows_seen"]
                    portfolio_inserted += result["rows_inserted"]
                    portfolio_updated += result["rows_updated"]
                    results.append(result)

                # A2.1 — import deposits and withdrawals
                cashflow_result = await upsert_bitget_cashflows(
                    conn,
                    portfolio_name,
                    portfolio_id,
                    base_currency,
                    creds,
                )

                # A2.2 — import perpetual-futures funding fees
                funding_fee_result = await upsert_bitget_funding_fees(
                    conn,
                    portfolio_name,
                    portfolio_id,
                    base_currency,
                    creds,
                )

                snapshot_result = await upsert_bitget_snapshot_and_positions(
                    conn, portfolio_name, creds
                )

                await finish_import_job(
                    conn,
                    job_id,
                    "success",
                    rows_seen=portfolio_seen,
                    rows_inserted=portfolio_inserted,
                    rows_updated=portfolio_updated,
                    metadata={
                        "creds_source": creds["source"],  # GLOBAL/ALTERNATIVES — never a secret
                        "results": results,
                        "cashflows": cashflow_result,
                        "funding_fees": funding_fee_result,
                        "snapshot": snapshot_result,
                    },
                )
            except Exception as e:
                # Isolate the failure to THIS portfolio; the loop continues.
                print(f"[bitget] portfolio {portfolio_name} failed: {e}")
                try:
                    await finish_import_job(conn, job_id, "failed", error_message=str(e))
                    await log_sync_error(conn, "Bitget", portfolio_name, str(e))
                except Exception as inner:
                    print(f"[bitget] could not record failure for {portfolio_name}: {inner}")
    finally:
        # ALWAYS close the worker's own connection.
        await conn.close()


@app.post("/sync/bitget")
async def sync_bitget(x_admin_token: Optional[str] = Header(None)):
    try:
        require_admin_token(x_admin_token)
    except PermissionError:
        return JSONResponse(content={"error": "Unauthorized"}, status_code=401)

    product_types = ["USDT-FUTURES", "USDC-FUTURES", "COIN-FUTURES"]
    jobs = []
    already_running = False

    conn = await get_conn()
    try:
        # (4) Sweep stale zombies first — never let a sweep failure block the sync.
        try:
            swept = await sweep_stale_import_jobs(conn)
            if swept > 0:
                print(f"[bitget] swept {swept} stale import_jobs before sync")
        except Exception as e:
            print(f"[bitget] pre-sync sweep failed (ignored): {e}")

        # (2) Pooler-safe mutex. Supabase routes us through Supavisor in
        #     transaction-pooling mode, so a SESSION-scoped pg_advisory_lock does
        #     NOT provide mutual exclusion (each autocommit statement may land on
        #     a different backend). Instead:
        #       * a TRANSACTION-scoped advisory lock serializes the check+insert
        #         (one transaction = one pinned backend), and
        #       * the mutex STATE is row-based: "is there a Bitget job already in
        #         status='started' younger than the sweeper threshold?".
        #     Check AND insert happen in the SAME transaction under the SAME lock,
        #     so there is no TOCTOU window. The xact lock auto-releases at COMMIT.
        #
        #     NOTE: STALE_JOB_MINUTES must exceed a healthy run's duration, and it
        #     is deliberately the SAME constant the sweeper uses (see its comment).
        #     If the two ever diverged, a still-running healthy sync could be both
        #     swept to 'failed' and treated as "not running" here -> duplicate run.
        async with conn.transaction():
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext($1))", BITGET_LOCK_KEY
            )
            running = await conn.fetchval(
                f"""
                SELECT count(*) FROM public.import_jobs
                WHERE broker = 'Bitget'
                  AND status = 'started'
                  AND COALESCE((metadata->>'work_started_at')::timestamptz, started_at)
                        > now() - interval '{STALE_JOB_MINUTES} minutes'
                """
            )
            if running and running > 0:
                already_running = True
            else:
                # (3) Create both jobs INSIDE the locked transaction so their ids
                #     ride along in the 202 and a concurrent caller sees them.
                for portfolio_name in ["Global", "Alternatives"]:
                    job_id = await start_import_job(
                        conn,
                        "Bitget",
                        portfolio_name,
                        {"product_types": product_types},
                    )
                    jobs.append({
                        "job_id": job_id,                   # UUID, used by the worker
                        "broker": "Bitget",
                        "portfolio": portfolio_name,
                        "poll_url": f"/sync/status/{job_id}",
                    })
        # transaction committed: jobs are persisted and the xact lock is released.
    except Exception as e:
        # Any failure here rolled the transaction back (no half-created jobs) and
        # released the lock; just report it.
        await conn.close()
        return JSONResponse(content={"status": "failed", "error": str(e)}, status_code=500)
    finally:
        if not conn.is_closed():
            await conn.close()

    if already_running:
        return JSONResponse(
            content={
                "status": "in_progress",
                "broker": "Bitget",
                "message": "a Bitget sync is already running",
            },
            status_code=409,
        )

    # (1) Spawn the worker, which opens and owns its OWN fresh connection — no
    #     handoff. Keep a strong reference so the task can't be GC'd mid-run.
    task = asyncio.create_task(run_bitget_sync_job(jobs))
    _bitget_tasks.add(task)
    task.add_done_callback(_bitget_tasks.discard)

    return JSONResponse(
        content=json_safe({"status": "accepted", "broker": "Bitget", "jobs": jobs}),
        status_code=202,
    )

@app.post("/sync/fx")
async def sync_fx_rates(x_admin_token: Optional[str] = Header(None)):
    try:
        require_admin_token(x_admin_token)
    except PermissionError:
        return JSONResponse(content={"error": "Unauthorized"}, status_code=401)

    conn = await get_conn()

    try:
        today = datetime.now(timezone.utc).date()

        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get("https://open.er-api.com/v6/latest/USD")
            data = response.json()

        if data.get("result") != "success":
            raise RuntimeError(f"FX API error: {data}")

        rates = data.get("rates") or {}

        usd_chf = parse_decimal(rates.get("CHF"), None)
        usd_eur = parse_decimal(rates.get("EUR"), None)
        usd_gbp = parse_decimal(rates.get("GBP"), None)

        if not usd_chf:
            raise RuntimeError("USDCHF rate missing from FX API")

        # IMPORTANT — stablecoin handling:
        #   Stablecoins (USDT, USDC) are set ~1 USD, so they are made equal to
        #   *each other and to USD only*. The conversion to a fiat target (CHF,
        #   EUR) then runs through the REAL fiat rate (usd_chf / usd_eur).
        #   => USDT→CHF and USDC→CHF both take the SAME real `usd_chf` rate.
        #      They are NOT 1:1 to CHF (that was a past bug); 1.0 is used ONLY
        #      for stable→USD and the self-identities.
        rows = [
            # --- to CHF (base currency): REAL usd_chf for USD and both stables
            ("USD",  "CHF", usd_chf, "open_er_api"),
            ("USDT", "CHF", usd_chf, "open_er_api_usdt_as_usd"),
            ("USDC", "CHF", usd_chf, "open_er_api_usdc_as_usd"),
            # --- to USD: stables ~1 USD (1.0), USD self-identity
            ("USD",  "USD", 1, "system"),
            ("USDT", "USD", 1, "system_usdt_as_usd"),
            ("USDC", "USD", 1, "system_usdc_as_usd"),
            # --- self-identities
            ("USDT", "USDT", 1, "system"),
            ("USDC", "USDC", 1, "system"),
            ("CHF",  "CHF",  1, "system"),
        ]

        # --- CHF fiat-cross pairs (display-currency toggle pivots through CHF):
        #     CHF->USD = 1 / usd_chf,  CHF->EUR = usd_eur / usd_chf.
        #     These are DERIVED real fiat cross rates, never literal 1:1.
        #     usd_chf > 0 is already guaranteed by the `if not usd_chf: raise`
        #     guard above, so the division is safe (div-by-zero guard).
        chf_usd = 1.0 / usd_chf
        rows.append(("CHF", "USD", chf_usd, "open_er_api_chf_cross"))

        if usd_eur:
            # to EUR: REAL usd_eur for USD and both stables; EUR self-identity
            rows.append(("USD",  "EUR", usd_eur, "open_er_api"))
            rows.append(("USDT", "EUR", usd_eur, "open_er_api_usdt_as_usd"))
            rows.append(("USDC", "EUR", usd_eur, "open_er_api_usdc_as_usd"))
            rows.append(("EUR",  "EUR", 1, "system"))
            # CHF->EUR fiat cross (only when usd_eur is present)
            chf_eur = usd_eur / usd_chf
            rows.append(("CHF", "EUR", chf_eur, "open_er_api_chf_cross"))

        if usd_gbp:
            rows.append(("USD",  "GBP", usd_gbp, "open_er_api"))
            rows.append(("USDT", "GBP", usd_gbp, "open_er_api_usdt_as_usd"))
            rows.append(("GBP",  "GBP", 1, "system"))

        # --- Anti-bug check (parity-SAFE) ----------------------------------
        #   The past bug class was a stablecoin/fiat pair silently collapsing to
        #   a literal 1.0 (e.g. USDC->CHF = 1). For the new CHF fiat-crosses the
        #   analogous bug would be CHF->USD or CHF->EUR landing on 1.0 instead of
        #   the derived rate. We do NOT flag "rate ~= 1" by magnitude alone:
        #   CHF/USD and CHF/EUR can LEGITIMATELY trade near parity. Instead we
        #   flag structurally — a cross that reads ~1 while its underlying USD
        #   legs are NOT at parity means the real rate was lost.
        EPS = 1e-4
        for fc, tc, rt, _src in rows:
            if fc == "CHF" and tc == "USD" and abs(rt - 1.0) < EPS and abs(usd_chf - 1.0) >= EPS:
                raise RuntimeError(
                    f"FX anti-bug: CHF->USD collapsed to ~1.0 but usd_chf={usd_chf} "
                    f"is not at parity (expected ~{1.0/usd_chf:.4f})"
                )
            if fc == "CHF" and tc == "EUR" and abs(rt - 1.0) < EPS and usd_eur and abs(usd_eur - usd_chf) >= EPS:
                raise RuntimeError(
                    f"FX anti-bug: CHF->EUR collapsed to ~1.0 but usd_eur={usd_eur}, "
                    f"usd_chf={usd_chf} are not at parity (expected ~{usd_eur/usd_chf:.4f})"
                )

        for from_currency, to_currency, rate, source in rows:
            await conn.execute(
                """
                INSERT INTO public.fx_rates (
                    rate_date, from_currency, to_currency, rate, source
                )
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (rate_date, from_currency, to_currency)
                DO UPDATE SET
                    rate = EXCLUDED.rate,
                    source = EXCLUDED.source,
                    created_at = now()
                """,
                today,
                from_currency,
                to_currency,
                rate,
                source,
            )

        return JSONResponse(
            content={
                "status": "success",
                "date": today.isoformat(),
                "usd_chf": usd_chf,
                "usd_eur": usd_eur,
                "usd_gbp": usd_gbp,
                "rows_upserted": len(rows),
                # Echo the exact rows upserted (read-only, no secrets) so the
                # stablecoin->fiat conversion can be verified without direct DB
                # access: USD/USDT/USDC -> CHF must all equal the real usd_chf.
                "rows": [
                    {
                        "from_currency": f,
                        "to_currency": t,
                        "rate": r,
                        "source": s,
                    }
                    for (f, t, r, s) in rows
                ],
            }
        )

    except Exception as e:
        return JSONResponse(content={"status": "failed", "error": str(e)}, status_code=500)

    finally:
        await conn.close()


# -------------------------
# MT5 ingest endpoint
# -------------------------

@app.post("/sync/mt5/deals")
async def sync_mt5_deals(request: Request, x_valbura_token: Optional[str] = Header(None)):
    expected_token = os.getenv("MT5_INGEST_TOKEN")
    if expected_token and x_valbura_token != expected_token:
        return JSONResponse(content={"error": "Unauthorized"}, status_code=401)

    payload = await request.json()
    if not isinstance(payload, list):
        return JSONResponse(content={"error": "Payload must be a list"}, status_code=400)

    conn = await get_conn()
    job_id = None

    try:
        job_id = await start_import_job(conn, "MT5", "Day Trading", {"rows": len(payload)})
        portfolio_id = await get_portfolio_id(conn, "Day Trading")

        rows_seen = 0
        rows_inserted = 0
        rows_updated = 0
        cashflows_seen = 0

        for deal in payload:
            rows_seen += 1

            external_id = str(deal.get("ticket") or deal.get("deal") or f"mt5-{rows_seen}")
            symbol = deal.get("symbol") or "UNKNOWN"
            side_raw = str(deal.get("type") or deal.get("entry") or deal.get("side") or "")
            side = "BUY" if side_raw in ["0", "BUY", "buy"] else "SELL" if side_raw in ["1", "SELL", "sell"] else "UNKNOWN"

            quantity = parse_decimal(deal.get("volume"), 0)
            price = parse_decimal(deal.get("price"), 0)
            profit = parse_decimal(deal.get("profit"), 0)
            commission = parse_decimal(deal.get("commission"), 0)
            fee = commission

            time_value = deal.get("time") or deal.get("time_msc")
            try:
                if time_value and int(time_value) > 10_000_000_000:
                    trade_time = datetime.fromtimestamp(int(time_value) / 1000, tz=timezone.utc)
                elif time_value:
                    trade_time = datetime.fromtimestamp(int(time_value), tz=timezone.utc)
                else:
                    trade_time = datetime.now(timezone.utc)
            except Exception:
                trade_time = datetime.now(timezone.utc)

            # MT5 external cashflows.
            # MT5 balance/deposit/withdrawal deals usually have type=2 or type='balance',
            # often no symbol, no volume and the amount in profit.
            deal_type_raw = str(deal.get("type") or "").strip().lower()
            deal_comment = str(deal.get("comment") or "").strip()
            account_currency = deal.get("account_currency") or "USD"

            is_balance_cashflow = (
                deal_type_raw in ["2", "balance"]
                or (
                    not deal.get("symbol")
                    and quantity == 0
                    and price == 0
                    and profit != 0
                    and "balance" in deal_comment.lower()
                )
            )

            if is_balance_cashflow:
                cashflows_seen += 1

                amount_native = profit
                cashflow_type = "DEPOSIT" if amount_native > 0 else "WITHDRAWAL"
                cashflow_external_id = f"MT5:Day Trading:cashflow:{external_id}"

                await conn.execute(
                    """
                    INSERT INTO public.portfolio_cashflows (
                        portfolio_id, broker, cashflow_date, currency,
                        amount_native, amount_base,
                        cashflow_type, source, external_id, raw_payload,
                        updated_at
                    )
                    VALUES (
                        $1, 'MT5', $2, $3,
                        $4, $4,
                        $5, 'mt5_deal_balance',
                        $6, $7::jsonb,
                        now()
                    )
                    ON CONFLICT (portfolio_id, broker, source, external_id)
                    DO UPDATE SET
                        cashflow_date = EXCLUDED.cashflow_date,
                        currency = EXCLUDED.currency,
                        amount_native = EXCLUDED.amount_native,
                        amount_base = EXCLUDED.amount_base,
                        cashflow_type = EXCLUDED.cashflow_type,
                        raw_payload = EXCLUDED.raw_payload,
                        updated_at = now()
                    """,
                    portfolio_id,
                    trade_time.date(),
                    account_currency,
                    amount_native,
                    cashflow_type,
                    cashflow_external_id,
                    json.dumps(deal),
                )

                # Do not import deposits/withdrawals as trades or realized PnL.
                continue
            
            existing_trade = await conn.fetchrow(
                """
                SELECT id
                FROM public.trades
                WHERE broker = 'MT5'
                AND external_trade_id = $1
                """,
                external_id,
            )
            
            result = await conn.execute(
                """
                INSERT INTO public.trades (
                    portfolio_id, broker, symbol, asset_class, instrument_type,
                    side, quantity, price, currency, fee, trade_date,
                    execution_time, external_trade_id, raw_payload
                )
                VALUES (
                    $1, 'MT5', $2, 'Index', 'CFD',
                    $3, $4, $5, 'USD', $6, $7,
                    $7, $8, $9::jsonb
                )
                ON CONFLICT (broker, external_trade_id)
                DO UPDATE SET
                    portfolio_id = EXCLUDED.portfolio_id,
                    symbol = EXCLUDED.symbol,
                    side = EXCLUDED.side,
                    quantity = EXCLUDED.quantity,
                    price = EXCLUDED.price,
                    fee = EXCLUDED.fee,
                    trade_date = EXCLUDED.trade_date,
                    execution_time = EXCLUDED.execution_time,
                    raw_payload = EXCLUDED.raw_payload,
                    imported_at = now()
                """,
                portfolio_id,
                symbol,
                side,
                quantity,
                price,
                fee,
                trade_time,
                external_id,
                json.dumps(deal),
            )

            if existing_trade:
                rows_updated += 1
            else:
                rows_inserted += 1

            if profit != 0:
                await conn.execute(
                    """
                    INSERT INTO public.realized_pnl_events (
                        portfolio_id, broker, symbol, asset_class,
                        realized_pnl, currency, realized_pnl_base,
                        event_time, external_id, raw_payload
                    )
                    VALUES (
                        $1, 'MT5', $2, 'Index',
                        $3, 'USD', $3,
                        $4, $5, $6::jsonb
                    )
                    ON CONFLICT (broker, external_id)
                    DO UPDATE SET
                        realized_pnl = EXCLUDED.realized_pnl,
                        realized_pnl_base = EXCLUDED.realized_pnl_base,
                        raw_payload = EXCLUDED.raw_payload
                    """,
                    portfolio_id,
                    symbol,
                    profit,
                    trade_time,
                    external_id,
                    json.dumps(deal),
                )

        await finish_import_job(
            conn,
            job_id,
            "success",
            rows_seen=rows_seen,
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            metadata={
                "cashflows_seen": cashflows_seen,
            },
        )

        return JSONResponse(
            content={
                "status": "success",
                "broker": "MT5",
                "rows_seen": rows_seen,
                "rows_inserted": rows_inserted,
                "rows_updated": rows_updated,
                "cashflows_seen": cashflows_seen,
            }
        )

    except Exception as e:
        if job_id:
            await finish_import_job(conn, job_id, "failed", error_message=str(e))
        await log_sync_error(conn, "MT5", "Day Trading", str(e), {"payload_size": len(payload)})
        return JSONResponse(content={"status": "failed", "error": str(e)}, status_code=500)

    finally:
        await conn.close()

@app.post("/sync/mt5/snapshot")
async def sync_mt5_snapshot(request: Request, x_valbura_token: Optional[str] = Header(None)):
    expected_token = os.getenv("MT5_INGEST_TOKEN")
    if expected_token and x_valbura_token != expected_token:
        return JSONResponse(content={"error": "Unauthorized"}, status_code=401)

    payload = await request.json()

    if not isinstance(payload, dict):
        return JSONResponse(content={"error": "Payload must be an object"}, status_code=400)

    account = payload.get("account") or {}
    positions = payload.get("positions") or []
    synced_at = payload.get("synced_at")

    account_login = str(account.get("login") or "")
    currency = account.get("currency") or "USD"

    conn = await get_conn()
    job_id = None

    try:
        job_id = await start_import_job(
            conn,
            "MT5",
            "Day Trading",
            {
                "type": "snapshot",
                "account_login": account_login,
                "positions": len(positions),
            },
        )

        portfolio_id = await get_portfolio_id(conn, "Day Trading")

        snapshot_time = parse_dt(synced_at)
        snapshot_date = snapshot_time.date()

        balance = parse_decimal(account.get("balance"), 0)
        equity = parse_decimal(account.get("equity"), 0)
        margin = parse_decimal(account.get("margin"), 0)
        profit = parse_decimal(account.get("profit"), 0)

        # NAV snapshot
        await conn.execute(
            """
            INSERT INTO public.portfolio_nav_snapshots (
                portfolio_id, broker, snapshot_date, currency,
                nav, cash, market_value, open_pnl, closed_pnl,
                deposits_withdrawals, source
            )
            VALUES (
                $1, 'MT5', $2, $3,
                $4, $5, NULL, $6, NULL,
                NULL, 'mt5_snapshot'
            )
            ON CONFLICT (portfolio_id, broker, snapshot_date, currency)
            DO UPDATE SET
                nav = EXCLUDED.nav,
                cash = EXCLUDED.cash,
                open_pnl = EXCLUDED.open_pnl,
                source = EXCLUDED.source,
                created_at = now()
            """,
            portfolio_id,
            snapshot_date,
            currency,
            equity,
            balance,
            profit,
        )

        # Cash snapshot
        await conn.execute(
            """
            INSERT INTO public.portfolio_cash (
                portfolio_id, broker, currency, cash_balance,
                cash_balance_base, updated_at
            )
            VALUES ($1, 'MT5', $2, $3, $3, now())
            ON CONFLICT (portfolio_id, broker, currency)
            DO UPDATE SET
                cash_balance = EXCLUDED.cash_balance,
                cash_balance_base = EXCLUDED.cash_balance_base,
                updated_at = now()
            """,
            portfolio_id,
            currency,
            balance,
        )

        # Clear old MT5 positions for this portfolio before inserting fresh snapshot
        await conn.execute(
            """
            DELETE FROM public.positions
            WHERE portfolio_id = $1
            AND broker = 'MT5'
            """,
            portfolio_id,
        )

        rows_seen = 0
        rows_inserted = 0

        for pos in positions:
            rows_seen += 1

            symbol = pos.get("symbol") or "UNKNOWN"
            volume = parse_decimal(pos.get("volume"), 0)
            price_open = parse_decimal(pos.get("price_open"), 0)
            price_current = parse_decimal(pos.get("price_current"), 0)
            position_profit = parse_decimal(pos.get("profit"), 0)

            position_type = str(pos.get("type") or pos.get("position_type") or "").lower()
            position_side = None
            if position_type in ["0", "buy", "long"]:
                position_side = "LONG"
            elif position_type in ["1", "sell", "short"]:
                position_side = "SHORT"
                volume = -abs(volume)

            take_profit = parse_decimal(pos.get("tp"), None)
            stop_loss = parse_decimal(pos.get("sl"), None)
            source_position_id = str(pos.get("ticket") or pos.get("identifier") or pos.get("position_id") or symbol)

            entry_time_raw = pos.get("time") or pos.get("time_msc")
            try:
                if entry_time_raw and int(entry_time_raw) > 10_000_000_000:
                    entry_date = datetime.fromtimestamp(int(entry_time_raw) / 1000, tz=timezone.utc)
                elif entry_time_raw:
                    entry_date = datetime.fromtimestamp(int(entry_time_raw), tz=timezone.utc)
                else:
                    entry_date = None
            except Exception:
                entry_date = None

            market_value_native = abs(volume * price_current)
            open_pnl_native = position_profit

            take_profit_orders = pos.get("take_profit_orders") or []
            stop_loss_orders = pos.get("stop_loss_orders") or []

            await conn.execute(
                """
                INSERT INTO public.positions (
                    portfolio_id, broker, symbol, asset_class,
                    quantity, avg_cost, currency, market_price,
                    market_value_native, market_value_base,
                    open_pnl_native, open_pnl_base,
                    entry_date, position_side,
                    take_profit, stop_loss,
                    take_profit_order_id, stop_loss_order_id,
                    source_position_id,
                    updated_at,
                    take_profit_orders,
                    stop_loss_orders
                )
                VALUES (
                    $1, 'MT5', $2, 'Index',
                    $3, $4, $5, $6,
                    $7, $7,
                    $8, $8,
                    $9, $10,
                    $11, $12,
                    NULL, NULL,
                    $13,
                    now(),
                    $14::jsonb,
                    $15::jsonb
                )
                ON CONFLICT (portfolio_id, broker, symbol)
                DO UPDATE SET
                    quantity = EXCLUDED.quantity,
                    avg_cost = EXCLUDED.avg_cost,
                    currency = EXCLUDED.currency,
                    market_price = EXCLUDED.market_price,
                    market_value_native = EXCLUDED.market_value_native,
                    market_value_base = EXCLUDED.market_value_base,
                    open_pnl_native = EXCLUDED.open_pnl_native,
                    open_pnl_base = EXCLUDED.open_pnl_base,
                    entry_date = EXCLUDED.entry_date,
                    position_side = EXCLUDED.position_side,
                    take_profit = EXCLUDED.take_profit,
                    stop_loss = EXCLUDED.stop_loss,
                    source_position_id = EXCLUDED.source_position_id,
                    take_profit_orders = EXCLUDED.take_profit_orders,
                    stop_loss_orders = EXCLUDED.stop_loss_orders,
                    updated_at = now()
                """,
                portfolio_id,
                symbol,
                volume,
                price_open,
                currency,
                price_current,
                market_value_native,
                open_pnl_native,
                entry_date,
                position_side,
                take_profit,
                stop_loss,
                source_position_id,
                json.dumps(take_profit_orders),
                json.dumps(stop_loss_orders),
            )
            rows_inserted += 1

        await finish_import_job(
            conn,
            job_id,
            "success",
            rows_seen=rows_seen,
            rows_inserted=rows_inserted,
            rows_updated=0,
            metadata={
                "type": "snapshot",
                "account_login": account_login,
                "currency": currency,
                "nav": equity,
                "cash": balance,
                "open_pnl": profit,
                "margin": margin,
                "positions": rows_seen,
            },
        )

        return JSONResponse(
            content={
                "status": "success",
                "broker": "MT5",
                "type": "snapshot",
                "account_login": account_login,
                "currency": currency,
                "nav": equity,
                "cash": balance,
                "open_pnl": profit,
                "positions": rows_seen,
            }
        )

    except Exception as e:
        if job_id:
            await finish_import_job(conn, job_id, "failed", error_message=str(e))
        await log_sync_error(conn, "MT5", "Day Trading", str(e), payload)
        return JSONResponse(content={"status": "failed", "error": str(e)}, status_code=500)

    finally:
        await conn.close()
