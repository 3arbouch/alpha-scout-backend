"""
Market data models.

Represents price data, company profiles, and macro indicators
as typed Python objects instead of raw dicts/tuples.

Used by: market data repo, signal engine, API responses.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class Price(BaseModel):
    """Single day OHLCV bar."""
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: int = 0


class CompanyProfile(BaseModel):
    """Company metadata from universe_profiles table."""
    symbol: str
    name: str | None = None
    sector: str | None = None
    industry: str | None = None
    market_cap: float | None = None
    exchange: str | None = None
    country: str | None = None
    description: str | None = None
    ipo_date: str | None = None
    image: str | None = None


class MacroDataPoint(BaseModel):
    """Single macro indicator observation."""
    series: str = Field(description="Series key (e.g. 'vix', 'brent', 'fed_funds').")
    date: str
    value: float
