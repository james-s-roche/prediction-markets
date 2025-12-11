"""Database helper using `asyncpg` to persist Kalshi markets and events.

This module creates flattened, typed tables for `markets` and `events` and
provides upsert helpers that extract fields from the Kalshi payloads and
persist them with appropriate SQL types.
"""
import os
from typing import Optional, Dict, Any
from decimal import Decimal
import json
from pathlib import Path

import asyncpg
from dotenv import load_dotenv

# Load .env file from project root
project_root = Path(__file__).parent.parent.parent
env_file = project_root / ".env"
if env_file.exists():
    load_dotenv(env_file)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://james@localhost:5432/prediction_markets")

_pool: Optional[asyncpg.pool.Pool] = None


async def get_pool() -> asyncpg.pool.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL)
    return _pool


def _serialize_jsonb(val):
    """Serialize Python object to JSON text for JSONB insertion.
    
    asyncpg cannot directly pass Python dicts/lists to JSONB columns.
    We must JSON-encode them, but NOT double-encode.
    """
    if val is None:
        return None
    if isinstance(val, str):
        return val  # Already JSON-encoded string
    return json.dumps(val)


def _as_int(v):
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return None


def _as_decimal(v):
    if v is None:
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None


def _as_datetime(v):
    """Convert ISO 8601 string to datetime with timezone, or return None/leave datetime as-is."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        # epoch seconds
        try:
            from datetime import datetime

            return datetime.fromtimestamp(v)
        except Exception:
            return None
    if hasattr(v, "tzinfo"):
        return v
    try:
        # Handle trailing Z (UTC) by replacing with +00:00
        s = str(v)
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        # fromisoformat handles offsets like +00:00
        from datetime import datetime

        return datetime.fromisoformat(s)
    except Exception:
        return None


async def create_tables() -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        # markets: flattened columns for common Kalshi market keys
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS markets (
                ticker TEXT PRIMARY KEY,
                event_ticker TEXT,
                market_type TEXT,
                title TEXT,
                subtitle TEXT,
                yes_sub_title TEXT,
                no_sub_title TEXT,
                created_time TIMESTAMPTZ,
                open_time TIMESTAMPTZ,
                close_time TIMESTAMPTZ,
                expiration_time TIMESTAMPTZ,
                latest_expiration_time TIMESTAMPTZ,
                expected_expiration_time TIMESTAMPTZ,
                settlement_timer_seconds INTEGER,
                status TEXT,
                response_price_units TEXT,
                yes_bid BIGINT,
                yes_bid_dollars NUMERIC(18,4),
                yes_ask BIGINT,
                yes_ask_dollars NUMERIC(18,4),
                no_bid BIGINT,
                no_bid_dollars NUMERIC(18,4),
                no_ask BIGINT,
                no_ask_dollars NUMERIC(18,4),
                last_price BIGINT,
                last_price_dollars NUMERIC(18,4),
                volume BIGINT,
                volume_24h BIGINT,
                result TEXT,
                can_close_early BOOLEAN,
                open_interest BIGINT,
                notional_value BIGINT,
                notional_value_dollars NUMERIC(18,4),
                previous_yes_bid BIGINT,
                previous_yes_bid_dollars NUMERIC(18,4),
                previous_yes_ask BIGINT,
                previous_yes_ask_dollars NUMERIC(18,4),
                previous_price BIGINT,
                previous_price_dollars NUMERIC(18,4),
                liquidity BIGINT,
                liquidity_dollars NUMERIC(18,4),
                expiration_value TEXT,
                category TEXT,
                risk_limit_cents BIGINT,
                tick_size BIGINT,
                rules_primary TEXT,
                rules_secondary TEXT,
                price_level_structure TEXT,
                price_ranges JSONB,
                settlement_value BIGINT,
                settlement_value_dollars NUMERIC(18,4),
                fee_waiver_expiration_time TIMESTAMPTZ,
                early_close_condition TEXT,
                strike_type TEXT,
                floor_strike NUMERIC(18,8),
                cap_strike NUMERIC(18,8),
                functional_strike TEXT,
                custom_strike JSONB,
                mve_collection_ticker TEXT,
                mve_selected_legs JSONB,
                primary_participant_key TEXT,
                created_at TIMESTAMPTZ DEFAULT now(),
                updated_at TIMESTAMPTZ DEFAULT now()
            );

            CREATE TABLE IF NOT EXISTS events (
                event_ticker TEXT PRIMARY KEY,
                series_ticker TEXT,
                sub_title TEXT,
                title TEXT,
                collateral_return_type TEXT,
                mutually_exclusive BOOLEAN,
                category TEXT,
                available_on_brokers BOOLEAN,
                product_metadata JSONB,
                strike_date TIMESTAMPTZ,
                strike_period TEXT,
                milestones JSONB,
                created_at TIMESTAMPTZ DEFAULT now(),
                updated_at TIMESTAMPTZ DEFAULT now()
            );

            CREATE INDEX IF NOT EXISTS idx_markets_event_ticker ON markets(event_ticker);
            CREATE INDEX IF NOT EXISTS idx_markets_status ON markets(status);
            CREATE INDEX IF NOT EXISTS idx_events_series_ticker ON events(series_ticker);
            """
        )


async def upsert_market(raw: Dict[str, Any]) -> None:
    """Extract fields from a Kalshi market payload and upsert into `markets`.

    The function tolerates missing keys and converts basic numeric/string types
    into appropriate DB types.
    """
    pool = await get_pool()
    # extract common fields
    ticker = raw.get("ticker")
    if not ticker:
        return
    event_ticker = raw.get("event_ticker")
    values = {
        "ticker": ticker,
        "event_ticker": event_ticker,
        "market_type": raw.get("market_type"),
        "title": raw.get("title"),
        "subtitle": raw.get("subtitle"),
        "yes_sub_title": raw.get("yes_sub_title"),
        "no_sub_title": raw.get("no_sub_title"),
        "created_time": raw.get("created_time"),
        "open_time": raw.get("open_time"),
        "close_time": raw.get("close_time"),
        "expiration_time": raw.get("expiration_time"),
        "latest_expiration_time": raw.get("latest_expiration_time"),
        "expected_expiration_time": raw.get("expected_expiration_time"),
        "settlement_timer_seconds": _as_int(raw.get("settlement_timer_seconds")),
        "status": raw.get("status"),
        "response_price_units": raw.get("response_price_units"),
        "yes_bid": _as_int(raw.get("yes_bid")),
        "yes_bid_dollars": _as_decimal(raw.get("yes_bid_dollars")),
        "yes_ask": _as_int(raw.get("yes_ask")),
        "yes_ask_dollars": _as_decimal(raw.get("yes_ask_dollars")),
        "no_bid": _as_int(raw.get("no_bid")),
        "no_bid_dollars": _as_decimal(raw.get("no_bid_dollars")),
        "no_ask": _as_int(raw.get("no_ask")),
        "no_ask_dollars": _as_decimal(raw.get("no_ask_dollars")),
        "last_price": _as_int(raw.get("last_price")),
        "last_price_dollars": _as_decimal(raw.get("last_price_dollars")),
        "volume": _as_int(raw.get("volume")),
        "volume_24h": _as_int(raw.get("volume_24h")),
        "result": raw.get("result"),
        "can_close_early": raw.get("can_close_early"),
        "open_interest": _as_int(raw.get("open_interest")),
        "notional_value": _as_int(raw.get("notional_value")),
        "notional_value_dollars": _as_decimal(raw.get("notional_value_dollars")),
        "previous_yes_bid": _as_int(raw.get("previous_yes_bid")),
        "previous_yes_bid_dollars": _as_decimal(raw.get("previous_yes_bid_dollars")),
        "previous_yes_ask": _as_int(raw.get("previous_yes_ask")),
        "previous_yes_ask_dollars": _as_decimal(raw.get("previous_yes_ask_dollars")),
        "previous_price": _as_int(raw.get("previous_price")),
        "previous_price_dollars": _as_decimal(raw.get("previous_price_dollars")),
        "liquidity": _as_int(raw.get("liquidity")),
        "liquidity_dollars": _as_decimal(raw.get("liquidity_dollars")),
        "expiration_value": raw.get("expiration_value"),
        "category": raw.get("category"),
        "risk_limit_cents": _as_int(raw.get("risk_limit_cents")),
        "tick_size": _as_int(raw.get("tick_size")),
        "rules_primary": raw.get("rules_primary"),
        "rules_secondary": raw.get("rules_secondary"),
        "price_level_structure": raw.get("price_level_structure"),
        "price_ranges": raw.get("price_ranges"),
        "settlement_value": _as_int(raw.get("settlement_value")),
        "settlement_value_dollars": _as_decimal(raw.get("settlement_value_dollars")),
        "fee_waiver_expiration_time": _as_datetime(raw.get("fee_waiver_expiration_time")),
        "early_close_condition": raw.get("early_close_condition"),
        "strike_type": raw.get("strike_type"),
        "floor_strike": _as_decimal(raw.get("floor_strike")),
        "cap_strike": _as_decimal(raw.get("cap_strike")),
        "functional_strike": raw.get("functional_strike"),
        "custom_strike": json.dumps(raw.get("custom_strike")) if raw.get("custom_strike") is not None else None,
        "mve_collection_ticker": raw.get("mve_collection_ticker"),
        "mve_selected_legs": raw.get("mve_selected_legs"),
        "primary_participant_key": raw.get("primary_participant_key"),
    }

    # Build columns and values dynamically to avoid placeholder mismatches
    cols = [
        "ticker", "event_ticker", "market_type", "title", "subtitle", "yes_sub_title", "no_sub_title",
        "created_time", "open_time", "close_time", "expiration_time", "latest_expiration_time", "expected_expiration_time",
        "settlement_timer_seconds", "status", "response_price_units",
        "yes_bid", "yes_bid_dollars", "yes_ask", "yes_ask_dollars", "no_bid", "no_bid_dollars", "no_ask", "no_ask_dollars",
        "last_price", "last_price_dollars", "volume", "volume_24h", "result", "can_close_early", "open_interest",
        "notional_value", "notional_value_dollars", "previous_yes_bid", "previous_yes_bid_dollars", "previous_yes_ask",
        "previous_yes_ask_dollars", "previous_price", "previous_price_dollars", "liquidity", "liquidity_dollars",
        "expiration_value", "category", "risk_limit_cents", "tick_size", "rules_primary", "rules_secondary",
        "price_level_structure", "price_ranges", "settlement_value", "settlement_value_dollars", "fee_waiver_expiration_time",
        "early_close_condition", "strike_type", "floor_strike", "cap_strike", "functional_strike", "custom_strike",
        "mve_collection_ticker", "mve_selected_legs", "primary_participant_key",
    ]

    # Convert datetime-like fields to actual datetime objects for asyncpg
    for dt_col in ("created_time", "open_time", "close_time", "expiration_time", "latest_expiration_time", "expected_expiration_time"):
        if values.get(dt_col) is not None:
            values[dt_col] = _as_datetime(values[dt_col])

    vals = [values.get(c) for c in cols]
    placeholders = ",".join(f"${i+1}" for i in range(len(cols)))
    insert_cols = ",".join(cols) + ", updated_at"
    update_set = ",".join(f"{c} = EXCLUDED.{c}" for c in cols if c != "ticker") + ", updated_at = now()"

    sql = f"""
    INSERT INTO markets({insert_cols})
    VALUES({placeholders}, now())
    ON CONFLICT (ticker) DO UPDATE SET {update_set};
    """

    async with pool.acquire() as conn:
        await conn.execute(sql, *vals)


async def upsert_event(raw: Dict[str, Any]) -> None:
    pool = await get_pool()
    event_ticker = raw.get("event_ticker")
    if not event_ticker:
        return
    values = {
        "event_ticker": event_ticker,
        "series_ticker": raw.get("series_ticker"),
        "sub_title": raw.get("sub_title"),
        "title": raw.get("title"),
        "collateral_return_type": raw.get("collateral_return_type"),
        "mutually_exclusive": raw.get("mutually_exclusive"),
        "category": raw.get("category"),
        "available_on_brokers": raw.get("available_on_brokers"),
        "product_metadata": raw.get("product_metadata"),
        "strike_date": _as_datetime(raw.get("strike_date")),
        "strike_period": raw.get("strike_period"),
        "milestones": raw.get("milestones"),
    }
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO events(
                event_ticker, series_ticker, sub_title, title, collateral_return_type,
                mutually_exclusive, category, available_on_brokers, product_metadata, strike_date,
                strike_period, milestones, updated_at
            ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12, now())
            ON CONFLICT (event_ticker) DO UPDATE SET
                series_ticker = EXCLUDED.series_ticker,
                sub_title = EXCLUDED.sub_title,
                title = EXCLUDED.title,
                collateral_return_type = EXCLUDED.collateral_return_type,
                mutually_exclusive = EXCLUDED.mutually_exclusive,
                category = EXCLUDED.category,
                available_on_brokers = EXCLUDED.available_on_brokers,
                product_metadata = EXCLUDED.product_metadata,
                strike_date = EXCLUDED.strike_date,
                strike_period = EXCLUDED.strike_period,
                milestones = EXCLUDED.milestones,
                updated_at = now();
            """,
            values["event_ticker"], values["series_ticker"], values["sub_title"], values["title"], values["collateral_return_type"],
            values["mutually_exclusive"], values["category"], values["available_on_brokers"], values["product_metadata"], values["strike_date"],
            values["strike_period"], values["milestones"],
        )


async def batch_upsert_markets(raw_markets: list) -> int:
    """Batch insert/upsert markets using INSERT...ON CONFLICT.
    
    Args:
        raw_markets: list of raw Kalshi market dicts
    
    Returns:
        number of rows processed
    """
    if not raw_markets:
        return 0

    pool = await get_pool()
    
    # Column names (must NOT include created_at, updated_at which have DEFAULT values)
    col_names = [
        "ticker", "event_ticker", "market_type", "title", "subtitle", "yes_sub_title", "no_sub_title",
        "created_time", "open_time", "close_time", "expiration_time", "latest_expiration_time",
        "expected_expiration_time", "settlement_timer_seconds", "status", "response_price_units",
        "yes_bid", "yes_bid_dollars", "yes_ask", "yes_ask_dollars", "no_bid", "no_bid_dollars",
        "no_ask", "no_ask_dollars", "last_price", "last_price_dollars", "volume", "volume_24h",
        "result", "can_close_early", "open_interest", "notional_value", "notional_value_dollars",
        "previous_yes_bid", "previous_yes_bid_dollars", "previous_yes_ask", "previous_yes_ask_dollars",
        "previous_price", "previous_price_dollars", "liquidity", "liquidity_dollars",
        "expiration_value", "category", "risk_limit_cents", "tick_size", "rules_primary", "rules_secondary",
        "price_level_structure", "price_ranges", "settlement_value", "settlement_value_dollars",
        "fee_waiver_expiration_time", "early_close_condition", "strike_type", "floor_strike",
        "cap_strike", "functional_strike", "custom_strike", "mve_collection_ticker",
        "mve_selected_legs", "primary_participant_key"
    ]
    
    # Build records as dicts
    records = []
    for raw in raw_markets:
        ticker = raw.get("ticker")
        if not ticker:
            continue
        
        rec = {
            "ticker": ticker,
            "event_ticker": raw.get("event_ticker"),
            "market_type": raw.get("market_type"),
            "title": raw.get("title"),
            "subtitle": raw.get("subtitle"),
            "yes_sub_title": raw.get("yes_sub_title"),
            "no_sub_title": raw.get("no_sub_title"),
            "created_time": _as_datetime(raw.get("created_time")),
            "open_time": _as_datetime(raw.get("open_time")),
            "close_time": _as_datetime(raw.get("close_time")),
            "expiration_time": _as_datetime(raw.get("expiration_time")),
            "latest_expiration_time": _as_datetime(raw.get("latest_expiration_time")),
            "expected_expiration_time": _as_datetime(raw.get("expected_expiration_time")),
            "settlement_timer_seconds": _as_int(raw.get("settlement_timer_seconds")),
            "status": raw.get("status"),
            "response_price_units": raw.get("response_price_units"),
            "yes_bid": _as_int(raw.get("yes_bid")),
            "yes_bid_dollars": _as_decimal(raw.get("yes_bid_dollars")),
            "yes_ask": _as_int(raw.get("yes_ask")),
            "yes_ask_dollars": _as_decimal(raw.get("yes_ask_dollars")),
            "no_bid": _as_int(raw.get("no_bid")),
            "no_bid_dollars": _as_decimal(raw.get("no_bid_dollars")),
            "no_ask": _as_int(raw.get("no_ask")),
            "no_ask_dollars": _as_decimal(raw.get("no_ask_dollars")),
            "last_price": _as_int(raw.get("last_price")),
            "last_price_dollars": _as_decimal(raw.get("last_price_dollars")),
            "volume": _as_int(raw.get("volume")),
            "volume_24h": _as_int(raw.get("volume_24h")),
            "result": raw.get("result"),
            "can_close_early": raw.get("can_close_early"),
            "open_interest": _as_int(raw.get("open_interest")),
            "notional_value": _as_int(raw.get("notional_value")),
            "notional_value_dollars": _as_decimal(raw.get("notional_value_dollars")),
            "previous_yes_bid": _as_int(raw.get("previous_yes_bid")),
            "previous_yes_bid_dollars": _as_decimal(raw.get("previous_yes_bid_dollars")),
            "previous_yes_ask": _as_int(raw.get("previous_yes_ask")),
            "previous_yes_ask_dollars": _as_decimal(raw.get("previous_yes_ask_dollars")),
            "previous_price": _as_int(raw.get("previous_price")),
            "previous_price_dollars": _as_decimal(raw.get("previous_price_dollars")),
            "liquidity": _as_int(raw.get("liquidity")),
            "liquidity_dollars": _as_decimal(raw.get("liquidity_dollars")),
            "expiration_value": raw.get("expiration_value"),
            "category": raw.get("category"),
            "risk_limit_cents": _as_int(raw.get("risk_limit_cents")),
            "tick_size": _as_int(raw.get("tick_size")),
            "rules_primary": raw.get("rules_primary"),
            "rules_secondary": raw.get("rules_secondary"),
            "price_level_structure": raw.get("price_level_structure"),
            "price_ranges": raw.get("price_ranges"),
            "settlement_value": _as_int(raw.get("settlement_value")),
            "settlement_value_dollars": _as_decimal(raw.get("settlement_value_dollars")),
            "fee_waiver_expiration_time": _as_datetime(raw.get("fee_waiver_expiration_time")),
            "early_close_condition": raw.get("early_close_condition"),
            "strike_type": raw.get("strike_type"),
            "floor_strike": _as_decimal(raw.get("floor_strike")),
            "cap_strike": _as_decimal(raw.get("cap_strike")),
            "functional_strike": raw.get("functional_strike"),
            "custom_strike": raw.get("custom_strike"),
            "mve_collection_ticker": raw.get("mve_collection_ticker"),
            "mve_selected_legs": raw.get("mve_selected_legs"),
            "primary_participant_key": raw.get("primary_participant_key"),
        }
        records.append(rec)

    if not records:
        return 0

    async with pool.acquire() as conn:
        async with conn.transaction():
            # Generate placeholder-based INSERT...ON CONFLICT statement
            col_list = ', '.join(col_names)
            
            # Build placeholders with ::jsonb cast for JSONB columns
            jsonb_cols = {'price_ranges', 'custom_strike', 'mve_selected_legs'}
            placeholders = []
            for i, col in enumerate(col_names):
                placeholder = f'${i+1}'
                if col in jsonb_cols:
                    # Cast string to JSONB in SQL (not using to_jsonb to avoid double-encoding)
                    placeholder = f'{placeholder}::jsonb'
                placeholders.append(placeholder)
            placeholders_str = ', '.join(placeholders)
            
            # Generate UPDATE SET clause with ::jsonb for JSONB fields
            update_sets = []
            for col in col_names:
                if col != 'ticker':
                    if col in jsonb_cols:
                        # EXCLUDED.col is already jsonb type, no need for cast
                        update_sets.append(f'{col} = EXCLUDED.{col}')
                    else:
                        update_sets.append(f'{col} = EXCLUDED.{col}')
            
            query = f"""
                INSERT INTO markets ({col_list})
                VALUES ({placeholders_str})
                ON CONFLICT (ticker) DO UPDATE SET {', '.join(update_sets)}, updated_at = now()
            """
            
            # Execute upsert for each record
            for rec in records:
                values = []
                for col in col_names:
                    val = rec.get(col)
                    # JSON-encode JSONB fields
                    if col in jsonb_cols and val is not None:
                        val = _serialize_jsonb(val)
                    values.append(val)
                await conn.execute(query, *values)
            
            return len(records)


async def batch_upsert_events(raw_events: list) -> int:
    """Batch insert/upsert events using INSERT...ON CONFLICT.
    
    Args:
        raw_events: list of raw Kalshi event dicts
    
    Returns:
        number of rows processed
    """
    if not raw_events:
        return 0

    pool = await get_pool()

    col_names = ["event_ticker", "series_ticker", "sub_title", "title", "collateral_return_type",
                 "mutually_exclusive", "category", "available_on_brokers", "product_metadata",
                 "strike_date", "strike_period", "milestones"]

    # Build records with typed fields
    records = []
    for raw in raw_events:
        event_ticker = raw.get("event_ticker")
        if not event_ticker:
            continue

        rec = {
            "event_ticker": event_ticker,
            "series_ticker": raw.get("series_ticker"),
            "sub_title": raw.get("sub_title"),
            "title": raw.get("title"),
            "collateral_return_type": raw.get("collateral_return_type"),
            "mutually_exclusive": raw.get("mutually_exclusive"),
            "category": raw.get("category"),
            "available_on_brokers": raw.get("available_on_brokers"),
            "product_metadata": raw.get("product_metadata"),
            "strike_date": _as_datetime(raw.get("strike_date")),
            "strike_period": raw.get("strike_period"),
            "milestones": raw.get("milestones"),
        }
        records.append(rec)

    if not records:
        return 0

    async with pool.acquire() as conn:
        async with conn.transaction():
            # Generate placeholder-based INSERT...ON CONFLICT statement
            col_list = ', '.join(col_names)
            
            # Build placeholders with ::jsonb cast for JSONB columns
            jsonb_cols = {'product_metadata', 'milestones'}
            placeholders = []
            for i, col in enumerate(col_names):
                placeholder = f'${i+1}'
                if col in jsonb_cols:
                    # Cast string to JSONB in SQL
                    placeholder = f'{placeholder}::jsonb'
                placeholders.append(placeholder)
            placeholders_str = ', '.join(placeholders)
            
            # Generate UPDATE SET clause with ::jsonb for JSONB fields
            update_sets = []
            for col in col_names:
                if col != 'event_ticker':
                    update_sets.append(f'{col} = EXCLUDED.{col}')
            
            query = f"""
                INSERT INTO events ({col_list})
                VALUES ({placeholders_str})
                ON CONFLICT (event_ticker) DO UPDATE SET {', '.join(update_sets)}, updated_at = now()
            """
            
            # Execute upsert for each record
            for rec in records:
                values = []
                for col in col_names:
                    val = rec.get(col)
                    # JSON-encode JSONB fields
                    if col in jsonb_cols and val is not None:
                        val = _serialize_jsonb(val)
                    values.append(val)
                await conn.execute(query, *values)
            
            return len(records)


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
