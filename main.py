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
                FROM public.v_positions_public
                WHERE portfolio_name = $1
                ORDER BY market_value_base DESC NULLS LAST
                """,
                portfolio,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT *
                FROM public.v_positions_public
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


@app.get("/health")
async def health():
    return {"status": "ok"}


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

            await finish_import_job(
                conn,
                job_id,
                "success",
                rows_seen=portfolio_seen,
                rows_inserted=portfolio_inserted,
                rows_updated=portfolio_updated,
                metadata={"results": results},
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
