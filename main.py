from fastapi import FastAPI, Request, Header, BackgroundTasks
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
from datetime import datetime, timezone, timedelta, date
from decimal import Decimal
from uuid import UUID
from typing import Optional, Any

import asyncpg
import httpx


app = FastAPI(title="Valbura Portfolio API")

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
async def get_public_dashboard(portfolio: str, trade_limit: int = 25):
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
                    "allocation": {
                        "asset_class": [dict(row) for row in allocation_asset_class_rows],
                        "broker": [dict(row) for row in allocation_broker_rows],
                        "symbol": [dict(row) for row in allocation_symbol_rows],
                    },
                    "positions": [dict(row) for row in position_rows],
                    "recent_trades": [dict(row) for row in trade_rows],
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

        # IBKR sometimes needs a short delay before report retrieval.
        await asyncio.sleep(20)

        fetch_url = f"{base_url}/GetStatement"
        fetch_params = {
            "t": token,
            "q": ref_code,
            "v": "3",
        }

        fetch_resp = await client.get(fetch_url, params=fetch_params)
        fetch_text = fetch_resp.text.strip()

        if not fetch_text:
            raise RuntimeError("IBKR GetStatement returned empty response")

        return fetch_text

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
                NULL, 'ibkr_flex_equity_summary'
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
            currency,
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
                    updated_at
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
                    now()
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


async def run_ibkr_sync_job():
    conn = await get_conn()
    job_id = None
    total_seen = 0
    total_inserted = 0
    total_updated = 0
    results = []

    try:
        global_query = os.getenv("IBKR_ACTIVITY_QUERY_ID_GLOBAL")
        alternatives_query = os.getenv("IBKR_ACTIVITY_QUERY_ID_ALTERNATIVES") or global_query

        if not global_query:
            raise RuntimeError("IBKR_ACTIVITY_QUERY_ID_GLOBAL missing")

        for portfolio_name, query_id in [
            ("Global", global_query),
            ("Alternatives", alternatives_query),
        ]:
            job_id = await start_import_job(
                conn,
                "IBKR",
                portfolio_name,
                {"query_id": query_id},
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
            
            total_seen += result["rows_seen"]
            total_inserted += result["rows_inserted"]
            total_updated += result["rows_updated"]
            results.append(result)

        return {
            "status": "success",
            "broker": "IBKR",
            "rows_seen": total_seen,
            "rows_inserted": total_inserted,
            "rows_updated": total_updated,
            "results": results,
        }

    except Exception as e:
        if job_id:
            await finish_import_job(conn, job_id, "failed", error_message=str(e))
        await log_sync_error(conn, "IBKR", None, str(e))
        return {
            "status": "failed",
            "broker": "IBKR",
            "error": str(e),
        }

    finally:
        await conn.close()


@app.post("/sync/ibkr")
async def sync_ibkr(x_admin_token: Optional[str] = Header(None)):
    try:
        require_admin_token(x_admin_token)
    except PermissionError:
        return JSONResponse(content={"error": "Unauthorized"}, status_code=401)

    result = await run_ibkr_sync_job()

    status_code = 200 if result.get("status") == "success" else 500
    return JSONResponse(content=result, status_code=status_code)


@app.post("/sync/ibkr/trigger")
async def trigger_ibkr(background_tasks: BackgroundTasks, x_admin_token: Optional[str] = Header(None)):
    try:
        require_admin_token(x_admin_token)
    except PermissionError:
        return JSONResponse(content={"error": "Unauthorized"}, status_code=401)

    background_tasks.add_task(run_ibkr_sync_job)

    return JSONResponse(
        content={
            "status": "accepted",
            "broker": "IBKR",
            "message": "IBKR sync started in background",
        },
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


async def bitget_get(path: str, params: dict):
    api_key = os.getenv("BITGET_GLOBAL_API_KEY")
    secret = os.getenv("BITGET_GLOBAL_SECRET")
    passphrase = os.getenv("BITGET_GLOBAL_PASSPHRASE")

    if not api_key or not secret or not passphrase:
        raise RuntimeError("Bitget API credentials missing")

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

@app.post("/debug/bitget/account")
async def debug_bitget_account(x_admin_token: Optional[str] = Header(None)):
    try:
        require_admin_token(x_admin_token)
    except PermissionError:
        return JSONResponse(content={"error": "Unauthorized"}, status_code=401)

    results = {}

    try:
        for product_type in ["USDT-FUTURES", "USDC-FUTURES", "COIN-FUTURES"]:
            try:
                account_data = await bitget_get(
                    "/api/v2/mix/account/accounts",
                    {"productType": product_type}
                )

                positions_data = await bitget_get(
                    "/api/v2/mix/position/all-position",
                    {
                        "productType": product_type,
                        "marginCoin": "USDT" if product_type == "USDT-FUTURES" else None,
                    }
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

async def upsert_bitget_snapshot_and_positions(conn, portfolio_name: str):
    portfolio_id = await get_portfolio_id(conn, portfolio_name)

    product_type = "USDT-FUTURES"
    margin_coin = "USDT"

    account_data = await bitget_get(
        "/api/v2/mix/account/accounts",
        {"productType": product_type},
    )

    positions_data = await bitget_get(
        "/api/v2/mix/position/all-position",
        {
            "productType": product_type,
            "marginCoin": margin_coin,
        },
    )

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

    nav = parse_decimal(account.get("accountEquity") or account.get("usdtEquity"), 0)
    cash = parse_decimal(account.get("available"), 0)
    open_pnl = parse_decimal(account.get("unrealizedPL"), 0)
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

        take_profit = parse_decimal(pos.get("takeProfit"), None)
        stop_loss = parse_decimal(pos.get("stopLoss"), None)
        take_profit_order_id = pos.get("takeProfitId") or None
        stop_loss_order_id = pos.get("stopLossId") or None
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
                updated_at
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
                now()
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
                updated_at = now()
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
        )

        positions_inserted += 1

    return {
        "portfolio": portfolio_name,
        "snapshot_imported": True,
        "currency": "USDT",
        "nav": nav,
        "cash": cash,
        "market_value": market_value,
        "open_pnl": open_pnl,
        "positions_seen": positions_seen,
        "positions_inserted": positions_inserted,
    }


@app.post("/sync/bitget")
async def sync_bitget(x_admin_token: Optional[str] = Header(None)):
    try:
        require_admin_token(x_admin_token)
    except PermissionError:
        return JSONResponse(content={"error": "Unauthorized"}, status_code=401)
    conn = await get_conn()
    job_id = None

    try:
        total_seen = 0
        total_inserted = 0
        total_updated = 0
        results = []

        product_types = ["USDT-FUTURES", "USDC-FUTURES", "COIN-FUTURES"]

        # For now, mirror to both portfolios until you have separated Bitget accounts/API keys.
        for portfolio_name in ["Global", "Alternatives"]:
            job_id = await start_import_job(
                conn,
                "Bitget",
                portfolio_name,
                {"product_types": product_types},
            )

            portfolio_seen = 0
            portfolio_inserted = 0
            portfolio_updated = 0

            for product_type in product_types:
                data = await bitget_get(
                    "/api/v2/mix/order/fill-history",
                    {"productType": product_type, "limit": "100"},
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

            snapshot_result = await upsert_bitget_snapshot_and_positions(conn, portfolio_name)

            await finish_import_job(
                conn,
                job_id,
                "success",
                rows_seen=portfolio_seen,
                rows_inserted=portfolio_inserted,
                rows_updated=portfolio_updated,
                metadata={
                    "results": results,
                    "snapshot": snapshot_result,
                },
            )

            total_seen += portfolio_seen
            total_inserted += portfolio_inserted
            total_updated += portfolio_updated

        return JSONResponse(
            content={
                "status": "success",
                "broker": "Bitget",
                "rows_seen": total_seen,
                "rows_inserted": total_inserted,
                "rows_updated": total_updated,
                "results": results,
             }
        )

    except Exception as e:
        if job_id:
            await finish_import_job(conn, job_id, "failed", error_message=str(e))
        await log_sync_error(conn, "Bitget", None, str(e))
        return JSONResponse(content={"status": "failed", "error": str(e)}, status_code=500)

    finally:
        await conn.close()

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

        rows = [
            ("USD", "CHF", usd_chf, "open_er_api"),
            ("USDT", "CHF", usd_chf, "open_er_api_usdt_as_usd"),
            ("USD", "USD", 1, "system"),
            ("USDT", "USDT", 1, "system"),
            ("CHF", "CHF", 1, "system"),
        ]

        if usd_eur:
            rows.append(("USD", "EUR", usd_eur, "open_er_api"))
            rows.append(("USDT", "EUR", usd_eur, "open_er_api_usdt_as_usd"))
            rows.append(("EUR", "EUR", 1, "system"))

        if usd_gbp:
            rows.append(("USD", "GBP", usd_gbp, "open_er_api"))
            rows.append(("USDT", "GBP", usd_gbp, "open_er_api_usdt_as_usd"))
            rows.append(("GBP", "GBP", 1, "system"))

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
        )

        return JSONResponse(
            content={
                "status": "success",
                "broker": "MT5",
                "rows_seen": rows_seen,
                "rows_inserted": rows_inserted,
                "rows_updated": rows_updated,
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
                    updated_at
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
                    now()
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
