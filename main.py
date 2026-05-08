from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import os
import asyncpg

app = FastAPI(title="Valbura Portfolio API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

async def get_conn():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    return await asyncpg.connect(db_url)

@app.get("/public/overview")
async def get_overview():
    conn = await get_conn()
    try:
        rows = await conn.fetch("SELECT * FROM public.v_portfolio_overview_base ORDER BY portfolio_name")
        return [dict(row) for row in rows]
    finally:
        await conn.close()

@app.get("/public/allocation")
async def get_allocation(portfolio: str, group_by: str = "asset_class"):
    view_map = {
        "asset_class": "v_allocation_asset_class_base",
        "broker": "v_allocation_broker_base",
        "currency": "v_allocation_currency_base"
    }
    view = view_map.get(group_by.lower())
    if not view:
        return JSONResponse(content={"error": "Invalid group_by"}, status_code=400)
    conn = await get_conn()
    try:
        rows = await conn.fetch(f"SELECT * FROM public.{view} WHERE portfolio_name = $1 ORDER BY allocation_percent DESC", portfolio)
        return [dict(row) for row in rows]
    finally:
        await conn.close()

@app.get("/public/trades")
async def get_trades(portfolio: str = None, limit: int = 100):
    conn = await get_conn()
    try:
        if portfolio:
            rows = await conn.fetch(
                """
                SELECT portfolio_name, base_currency, broker, symbol, asset_class, instrument_type, side, quantity, price, currency, gross_value_native, gross_value_base, fee_base, trade_timestamp 
                FROM public.v_recent_trades_base 
                WHERE portfolio_name = $1 
                ORDER BY trade_timestamp DESC 
                LIMIT $2
                """, 
                portfolio, limit
            )
        else:
            rows = await conn.fetch(
                """
                SELECT portfolio_name, base_currency, broker, symbol, asset_class, instrument_type, side, quantity, price, currency, gross_value_native, gross_value_base, fee_base, trade_timestamp 
                FROM public.v_recent_trades_base 
                ORDER BY trade_timestamp DESC 
                LIMIT $1
                """, 
                limit
            )
        return [dict(row) for row in rows]
    finally:
        await conn.close()

@app.get("/public/missing-fx")
async def get_missing_fx():
    conn = await get_conn()
    try:
        rows = await conn.fetch("SELECT * FROM public.v_currency_exposure_missing_fx")
        return [dict(row) for row in rows]
    finally:
        await conn.close()

@app.get("/health")
async def health():
    return {"status": "ok"}
