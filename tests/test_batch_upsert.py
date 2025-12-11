"""
Unit tests for batch_upsert_markets and batch_upsert_events.

Tests validate:
- Type conversion (datetime, Decimal, integers)
- COPY + staging table + upsert flow
- Empty batch handling
- Duplicate key handling (upsert behavior)
- Field extraction from raw Kalshi payloads
"""
import asyncio
import pytest
import pytest_asyncio
from datetime import datetime
from decimal import Decimal

import asyncpg

from backend.common.db import (
    get_pool,
    create_tables,
    batch_upsert_markets,
    batch_upsert_events,
    close_pool,
)


@pytest_asyncio.fixture(scope="function")
async def test_db():
    """Setup and teardown test database."""
    # Create tables
    await create_tables()
    yield
    # Cleanup: truncate tables after each test
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("TRUNCATE markets, events CASCADE")
    await close_pool()


@pytest.mark.asyncio
async def test_batch_upsert_markets_empty(test_db):
    """Empty batch should return 0 and not error."""
    result = await batch_upsert_markets([])
    assert result == 0


@pytest.mark.asyncio
async def test_batch_upsert_markets_single_row(test_db):
    """Single market should be inserted correctly with type conversions."""
    market = {
        "ticker": "TEST-MARKET-001",
        "event_ticker": "TEST-EVENT-001",
        "market_type": "binary",
        "title": "Test Market",
        "subtitle": "A test",
        "yes_sub_title": "Yes",
        "no_sub_title": "No",
        "created_time": "2025-12-11T12:00:00Z",
        "open_time": "2025-12-11T12:30:00Z",
        "close_time": "2025-12-25T00:00:00Z",
        "expiration_time": "2025-12-25T00:00:00Z",
        "latest_expiration_time": "2025-12-25T00:00:00Z",
        "expected_expiration_time": "2025-12-25T00:00:00Z",
        "settlement_timer_seconds": 1800,
        "status": "initialized",
        "response_price_units": "usd_cent",
        "yes_bid": 45,
        "yes_bid_dollars": "0.4500",
        "yes_ask": 55,
        "yes_ask_dollars": "0.5500",
        "no_bid": 45,
        "no_bid_dollars": "0.4500",
        "no_ask": 55,
        "no_ask_dollars": "0.5500",
        "last_price": 50,
        "last_price_dollars": "0.5000",
        "volume": 1000,
        "volume_24h": 2000,
        "result": None,
        "can_close_early": True,
        "open_interest": 5000,
        "notional_value": 100,
        "notional_value_dollars": "1.0000",
        "previous_yes_bid": 40,
        "previous_yes_bid_dollars": "0.4000",
        "previous_yes_ask": 60,
        "previous_yes_ask_dollars": "0.6000",
        "previous_price": 50,
        "previous_price_dollars": "0.5000",
        "liquidity": 10000,
        "liquidity_dollars": "100.0000",
        "expiration_value": None,
        "category": "sports",
        "risk_limit_cents": 10000,
        "tick_size": 1,
        "rules_primary": "Test rule",
        "rules_secondary": None,
        "price_level_structure": "linear_cent",
        "price_ranges": None,
        "settlement_value": None,
        "settlement_value_dollars": None,
        "fee_waiver_expiration_time": None,
        "early_close_condition": None,
        "strike_type": "binary",
        "floor_strike": None,
        "cap_strike": None,
        "functional_strike": None,
        "custom_strike": None,
        "mve_collection_ticker": None,
        "mve_selected_legs": None,
        "primary_participant_key": None,
    }

    result = await batch_upsert_markets([market])
    assert result == 1

    # Verify in DB
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM markets WHERE ticker = $1", "TEST-MARKET-001")
        assert row is not None
        assert row["title"] == "Test Market"
        assert row["status"] == "initialized"
        assert row["yes_bid"] == 45
        # Verify datetime was parsed
        assert isinstance(row["created_time"], datetime)
        assert row["created_time"].year == 2025
        # Verify Decimal conversion
        assert isinstance(row["yes_bid_dollars"], Decimal)
        assert row["yes_bid_dollars"] == Decimal("0.4500")


@pytest.mark.asyncio
async def test_batch_upsert_markets_multiple_rows(test_db):
    """Multiple markets should be inserted in one batch."""
    markets = [
        {
            "ticker": f"TEST-MARKET-{i:03d}",
            "event_ticker": f"TEST-EVENT-{i // 5:02d}",
            "market_type": "binary",
            "title": f"Test Market {i}",
            "subtitle": None,
            "yes_sub_title": None,
            "no_sub_title": None,
            "created_time": "2025-12-11T12:00:00Z",
            "open_time": "2025-12-11T12:30:00Z",
            "close_time": "2025-12-25T00:00:00Z",
            "expiration_time": "2025-12-25T00:00:00Z",
            "latest_expiration_time": "2025-12-25T00:00:00Z",
            "expected_expiration_time": "2025-12-25T00:00:00Z",
            "settlement_timer_seconds": 1800,
            "status": "initialized",
            "response_price_units": "usd_cent",
            "yes_bid": 45 + i,
            "yes_bid_dollars": f"0.{45 + i:04d}",
            "yes_ask": 55 + i,
            "yes_ask_dollars": f"0.{55 + i:04d}",
            "no_bid": None,
            "no_bid_dollars": None,
            "no_ask": None,
            "no_ask_dollars": None,
            "last_price": None,
            "last_price_dollars": None,
            "volume": None,
            "volume_24h": None,
            "result": None,
            "can_close_early": False,
            "open_interest": None,
            "notional_value": None,
            "notional_value_dollars": None,
            "previous_yes_bid": None,
            "previous_yes_bid_dollars": None,
            "previous_yes_ask": None,
            "previous_yes_ask_dollars": None,
            "previous_price": None,
            "previous_price_dollars": None,
            "liquidity": None,
            "liquidity_dollars": None,
            "expiration_value": None,
            "category": None,
            "risk_limit_cents": None,
            "tick_size": None,
            "rules_primary": None,
            "rules_secondary": None,
            "price_level_structure": None,
            "price_ranges": None,
            "settlement_value": None,
            "settlement_value_dollars": None,
            "fee_waiver_expiration_time": None,
            "early_close_condition": None,
            "strike_type": None,
            "floor_strike": None,
            "cap_strike": None,
            "functional_strike": None,
            "custom_strike": None,
            "mve_collection_ticker": None,
            "mve_selected_legs": None,
            "primary_participant_key": None,
        }
        for i in range(10)
    ]

    result = await batch_upsert_markets(markets)
    assert result == 10

    # Verify count in DB
    pool = await get_pool()
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM markets")
        assert count == 10


@pytest.mark.asyncio
async def test_batch_upsert_markets_upsert_behavior(test_db):
    """Upserting the same ticker should update, not insert duplicate."""
    market = {
        "ticker": "TEST-UPSERT-001",
        "event_ticker": "TEST-EVENT-001",
        "market_type": "binary",
        "title": "Original Title",
        "subtitle": None,
        "yes_sub_title": None,
        "no_sub_title": None,
        "created_time": "2025-12-11T12:00:00Z",
        "open_time": None,
        "close_time": None,
        "expiration_time": None,
        "latest_expiration_time": None,
        "expected_expiration_time": None,
        "settlement_timer_seconds": None,
        "status": "initialized",
        "response_price_units": None,
        "yes_bid": 50,
        "yes_bid_dollars": "0.5000",
        "yes_ask": None,
        "yes_ask_dollars": None,
        "no_bid": None,
        "no_bid_dollars": None,
        "no_ask": None,
        "no_ask_dollars": None,
        "last_price": None,
        "last_price_dollars": None,
        "volume": None,
        "volume_24h": None,
        "result": None,
        "can_close_early": None,
        "open_interest": None,
        "notional_value": None,
        "notional_value_dollars": None,
        "previous_yes_bid": None,
        "previous_yes_bid_dollars": None,
        "previous_yes_ask": None,
        "previous_yes_ask_dollars": None,
        "previous_price": None,
        "previous_price_dollars": None,
        "liquidity": None,
        "liquidity_dollars": None,
        "expiration_value": None,
        "category": None,
        "risk_limit_cents": None,
        "tick_size": None,
        "rules_primary": None,
        "rules_secondary": None,
        "price_level_structure": None,
        "price_ranges": None,
        "settlement_value": None,
        "settlement_value_dollars": None,
        "fee_waiver_expiration_time": None,
        "early_close_condition": None,
        "strike_type": None,
        "floor_strike": None,
        "cap_strike": None,
        "functional_strike": None,
        "custom_strike": None,
        "mve_collection_ticker": None,
        "mve_selected_legs": None,
        "primary_participant_key": None,
    }

    # First insert
    result = await batch_upsert_markets([market])
    assert result == 1

    # Upsert with updated title
    market["title"] = "Updated Title"
    market["yes_bid"] = 60
    result = await batch_upsert_markets([market])
    assert result == 1

    # Verify only one row exists with updated values
    pool = await get_pool()
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM markets WHERE ticker = $1", "TEST-UPSERT-001")
        assert count == 1
        row = await conn.fetchrow("SELECT * FROM markets WHERE ticker = $1", "TEST-UPSERT-001")
        assert row["title"] == "Updated Title"
        assert row["yes_bid"] == 60


@pytest.mark.asyncio
async def test_batch_upsert_events_single_row(test_db):
    """Single event should be inserted correctly."""
    event = {
        "event_ticker": "TEST-EVENT-001",
        "series_ticker": "TEST-SERIES-001",
        "sub_title": "A subtitle",
        "title": "Test Event",
        "collateral_return_type": "standard",
        "mutually_exclusive": True,
        "category": "sports",
        "available_on_brokers": False,
        "product_metadata": {"key": "value"},
        "strike_date": "2025-12-25T00:00:00Z",
        "strike_period": "2025-12",
        "milestones": [{"name": "milestone1"}],
    }

    result = await batch_upsert_events([event])
    assert result == 1

    # Verify in DB
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM events WHERE event_ticker = $1", "TEST-EVENT-001")
        assert row is not None
        assert row["title"] == "Test Event"
        assert isinstance(row["strike_date"], datetime)
        assert row["strike_date"].year == 2025
        # product_metadata should be stored as JSONB
        assert row["product_metadata"] is not None


@pytest.mark.asyncio
async def test_batch_upsert_events_multiple_rows(test_db):
    """Multiple events should be inserted in one batch."""
    events = [
        {
            "event_ticker": f"TEST-EVENT-{i:03d}",
            "series_ticker": f"TEST-SERIES-{i // 3:02d}",
            "sub_title": None,
            "title": f"Test Event {i}",
            "collateral_return_type": None,
            "mutually_exclusive": None,
            "category": None,
            "available_on_brokers": None,
            "product_metadata": None,
            "strike_date": None,
            "strike_period": None,
            "milestones": None,
        }
        for i in range(10)
    ]

    result = await batch_upsert_events(events)
    assert result == 10

    # Verify count in DB
    pool = await get_pool()
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM events")
        assert count == 10


@pytest.mark.asyncio
async def test_batch_upsert_events_empty(test_db):
    """Empty batch should return 0 and not error."""
    result = await batch_upsert_events([])
    assert result == 0


@pytest.mark.asyncio
async def test_batch_upsert_markets_with_jsonb_fields(test_db):
    """Markets with JSONB fields (price_ranges, custom_strike, etc) should be stored correctly."""
    market = {
        "ticker": "TEST-JSONB-001",
        "event_ticker": "TEST-EVENT-JSONB",
        "market_type": "binary",
        "title": "Market with JSONB",
        "subtitle": None,
        "yes_sub_title": None,
        "no_sub_title": None,
        "created_time": "2025-12-11T12:00:00Z",
        "open_time": None,
        "close_time": None,
        "expiration_time": None,
        "latest_expiration_time": None,
        "expected_expiration_time": None,
        "settlement_timer_seconds": None,
        "status": "initialized",
        "response_price_units": None,
        "yes_bid": None,
        "yes_bid_dollars": None,
        "yes_ask": None,
        "yes_ask_dollars": None,
        "no_bid": None,
        "no_bid_dollars": None,
        "no_ask": None,
        "no_ask_dollars": None,
        "last_price": None,
        "last_price_dollars": None,
        "volume": None,
        "volume_24h": None,
        "result": None,
        "can_close_early": None,
        "open_interest": None,
        "notional_value": None,
        "notional_value_dollars": None,
        "previous_yes_bid": None,
        "previous_yes_bid_dollars": None,
        "previous_yes_ask": None,
        "previous_yes_ask_dollars": None,
        "previous_price": None,
        "previous_price_dollars": None,
        "liquidity": None,
        "liquidity_dollars": None,
        "expiration_value": None,
        "category": None,
        "risk_limit_cents": None,
        "tick_size": None,
        "rules_primary": None,
        "rules_secondary": None,
        "price_level_structure": None,
        "price_ranges": [{"start": 0, "end": 100, "step": 1}],
        "settlement_value": None,
        "settlement_value_dollars": None,
        "fee_waiver_expiration_time": None,
        "early_close_condition": None,
        "strike_type": None,
        "floor_strike": None,
        "cap_strike": None,
        "functional_strike": None,
        "custom_strike": {"key": "custom_value"},
        "mve_collection_ticker": None,
        "mve_selected_legs": None,
        "primary_participant_key": None,
    }

    result = await batch_upsert_markets([market])
    assert result == 1

    # Verify JSONB fields were stored
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM markets WHERE ticker = $1", "TEST-JSONB-001")
        assert row is not None
        assert row["price_ranges"] is not None
        assert row["custom_strike"] is not None
        # asyncpg should return JSONB as dicts (or might be strings depending on version)
        custom_strike = row["custom_strike"]
        if isinstance(custom_strike, str):
            import json
            custom_strike = json.loads(custom_strike)
        assert isinstance(custom_strike, dict), f"Expected dict, got {type(custom_strike)}"
        assert custom_strike["key"] == "custom_value"
