# Market Data Backend — Design

Implementation-ready design for the FinAlly market data subsystem. This document
describes the **unified market data interface** and its two concrete
implementations — the **GBM simulator** (default) and the **Massive (Polygon.io)
API client** (optional) — along with the shared price cache, the SSE streaming
endpoint, the factory, and FastAPI lifecycle integration.

It is written to be a complete, self-contained build guide: every module includes
the code as it ships, plus usage examples and the reasoning behind each decision.
Everything described lives under `backend/app/market/`.

> This document reflects the **as-built** subsystem. An earlier draft lives in
> `planning/archive/MARKET_DATA_DESIGN.md`; this version supersedes it and matches
> the code currently in `backend/app/market/` after code review (top-level
> `massive` imports, public `get_tickers()`, typed SSE generator, consolidated
> correlation constants).

---

## Table of Contents

1. [Architecture at a Glance](#1-architecture-at-a-glance)
2. [File Structure](#2-file-structure)
3. [Data Model — `models.py`](#3-data-model--modelspy)
4. [Price Cache — `cache.py`](#4-price-cache--cachepy)
5. [Unified Interface — `interface.py`](#5-unified-interface--interfacepy)
6. [Seed Prices & Parameters — `seed_prices.py`](#6-seed-prices--parameters--seed_pricespy)
7. [GBM Simulator — `simulator.py`](#7-gbm-simulator--simulatorpy)
8. [Massive API Client — `massive_client.py`](#8-massive-api-client--massive_clientpy)
9. [Factory — `factory.py`](#9-factory--factorypy)
10. [SSE Streaming Endpoint — `stream.py`](#10-sse-streaming-endpoint--streampy)
11. [Public API — `__init__.py`](#11-public-api--__init__py)
12. [FastAPI Lifecycle Integration](#12-fastapi-lifecycle-integration)
13. [Watchlist Coordination](#13-watchlist-coordination)
14. [Testing Strategy](#14-testing-strategy)
15. [Error Handling & Edge Cases](#15-error-handling--edge-cases)
16. [Configuration Summary](#16-configuration-summary)

---

## 1. Architecture at a Glance

```
                  create_market_data_source(cache)   ← reads MASSIVE_API_KEY
                              │
              ┌───────────────┴────────────────┐
              ▼                                ▼
   SimulatorDataSource                 MassiveDataSource
   (GBM, default, no key)              (Polygon.io REST poller)
              │                                │
              └──────────────┬─────────────────┘
                             ▼
                   ┌──────────────────┐
                   │    PriceCache    │  thread-safe, in-memory,
                   │  (single truth)  │  version-counted
                   └──────────────────┘
                             │
          ┌──────────────────┼───────────────────┐
          ▼                  ▼                    ▼
   SSE /api/stream/prices   Portfolio valuation   Trade execution
   (push to browser)        (cash + Σ qty×price)   (fill at current px)
```

The design rests on three ideas:

1. **One interface, two implementations.** `MarketDataSource` is an abstract base
   class. The simulator and the Massive client both implement it, so all
   downstream code is agnostic to which is running.
2. **The cache is the only contract between producers and consumers.** Data
   sources *write* to `PriceCache`; SSE, portfolio, and trades *read* from it.
   Neither side knows about the other.
3. **Push on the producer's schedule, read on the consumer's schedule.** The
   simulator ticks at 500ms, Massive polls at 15s, and SSE reads at 500ms — fully
   decoupled by the cache's version counter.

---

## 2. File Structure

```
backend/app/market/
  __init__.py          # Public re-exports
  models.py            # PriceUpdate dataclass
  cache.py             # PriceCache (thread-safe in-memory store)
  interface.py         # MarketDataSource ABC (the unified API)
  seed_prices.py       # SEED_PRICES, TICKER_PARAMS, correlation constants
  simulator.py         # GBMSimulator + SimulatorDataSource
  massive_client.py    # MassiveDataSource
  factory.py           # create_market_data_source()
  stream.py            # SSE endpoint (FastAPI router factory)
```

Each file has a single responsibility. `__init__.py` re-exports the public API so
the rest of the backend imports from `app.market` without reaching into
submodules.

---

## 3. Data Model — `models.py`

`PriceUpdate` is the only data structure that leaves the market data layer. Every
downstream consumer — SSE streaming, portfolio valuation, trade execution — works
exclusively with this type.

```python
"""Data models for market data."""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        """Absolute price change from previous update."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percentage change from previous update."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat'."""
        if self.price > self.previous_price:
            return "up"
        elif self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
        }
```

**Design decisions**

- **`frozen=True`** — Price updates are immutable value objects, safe to share
  across async tasks without copying.
- **`slots=True`** — Minor memory win; we create many of these per second.
- **Computed properties** (`change`, `change_percent`, `direction`) — Derived from
  `price` and `previous_price`, so they can never be inconsistent or stale.
- **`to_dict()`** — A single serialization point used by both the SSE endpoint and
  REST responses. This is exactly the per-ticker shape the frontend expects (see
  the SSE wire format in §10).

---

## 4. Price Cache — `cache.py`

The cache is the central data hub. Sources write to it; SSE and portfolio
valuation read from it. It must be thread-safe: the Massive client's synchronous
call runs in a worker thread (`asyncio.to_thread`) while SSE reads happen on the
event loop.

```python
"""Thread-safe in-memory price cache."""

from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Thread-safe in-memory cache of the latest price for each ticker.

    Writers: SimulatorDataSource or MassiveDataSource (one at a time).
    Readers: SSE streaming endpoint, portfolio valuation, trade execution.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # Monotonically increasing; bumped on every update

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        """Record a new price for a ticker. Returns the created PriceUpdate.

        Automatically computes direction and change from the previous price.
        If this is the first update for the ticker, previous_price == price
        (direction='flat').
        """
        with self._lock:
            ts = timestamp or time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        """Get the latest price for a single ticker, or None if unknown."""
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        """Snapshot of all current prices. Returns a shallow copy."""
        with self._lock:
            return dict(self._prices)

    def get_price(self, ticker: str) -> float | None:
        """Convenience: get just the price float, or None."""
        update = self.get(ticker)
        return update.price if update else None

    def remove(self, ticker: str) -> None:
        """Remove a ticker from the cache (e.g., when removed from watchlist)."""
        with self._lock:
            self._prices.pop(ticker, None)

    @property
    def version(self) -> int:
        """Current version counter. Useful for SSE change detection."""
        return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

**Why the previous price is stored by the cache, not the source.** `update()`
looks up the prior `PriceUpdate` and threads its `price` into `previous_price`.
This means *every* source gets correct `change`/`direction` for free — the
simulator and the Massive client never have to track deltas themselves.

**Why a version counter?** The SSE loop polls the cache every ~500ms. If Massive
only refreshes every 15s, most polls would re-send identical data. The counter
lets the SSE loop skip a send when nothing changed:

```python
last_version = -1
while True:
    if price_cache.version != last_version:
        last_version = price_cache.version
        yield format_sse(price_cache.get_all())
    await asyncio.sleep(0.5)
```

**Why `threading.Lock` (not `asyncio.Lock`)?** The Massive client's synchronous
`get_snapshot_all()` runs in a real OS thread via `asyncio.to_thread()`. An
`asyncio.Lock` only coordinates coroutines on the event loop and would not protect
against that thread. `threading.Lock` works correctly from both a worker thread and
the event loop, and the critical section (one dict op) is tiny, so contention is
negligible.

---

## 5. Unified Interface — `interface.py`

This abstract base class **is** the unified market data API. Both data sources
implement it; the factory returns it; downstream code depends only on it.

```python
"""Abstract interface for market data sources."""

from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push price updates into a shared PriceCache on their own
    schedule. Downstream code never calls the data source directly for prices —
    it reads from the cache.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])
        # ... app runs ...
        await source.add_ticker("TSLA")
        await source.remove_ticker("GOOGL")
        # ... app shutting down ...
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates for the given tickers.

        Starts a background task that periodically writes to the PriceCache.
        Must be called exactly once. Calling start() twice is undefined behavior.
        """

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task and release resources.

        Safe to call multiple times. After stop(), the source will not write
        to the cache again.
        """

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present.

        The next update cycle will include this ticker.
        """

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set. No-op if not present.

        Also removes the ticker from the PriceCache.
        """

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of actively tracked tickers."""
```

**Five methods, one cache.** Note that *no method returns a price*. Sources only
manage the *set* of tracked tickers and push values into the cache. This push
model is what decouples producer cadence from consumer cadence (§1).

---

## 6. Seed Prices & Parameters — `seed_prices.py`

Constants only — no logic, no imports beyond stdlib. Shared by the simulator (for
initial prices, GBM parameters, and correlation structure).

```python
"""Seed prices and per-ticker parameters for the market simulator."""

# Realistic starting prices for the default watchlist (as of project creation)
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00,
    "GOOGL": 175.00,
    "MSFT": 420.00,
    "AMZN": 185.00,
    "TSLA": 250.00,
    "NVDA": 800.00,
    "META": 500.00,
    "JPM": 195.00,
    "V": 280.00,
    "NFLX": 600.00,
}

# Per-ticker GBM parameters
# sigma: annualized volatility (higher = more price movement)
# mu: annualized drift / expected return
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05},
    "AMZN": {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},  # High volatility
    "NVDA": {"sigma": 0.40, "mu": 0.08},  # High volatility, strong drift
    "META": {"sigma": 0.30, "mu": 0.05},
    "JPM": {"sigma": 0.18, "mu": 0.04},  # Low volatility (bank)
    "V": {"sigma": 0.17, "mu": 0.04},  # Low volatility (payments)
    "NFLX": {"sigma": 0.35, "mu": 0.05},
}

# Default parameters for tickers not in the list above (dynamically added)
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

# Correlation groups for the simulator's Cholesky decomposition
# Tickers in the same group have higher intra-group correlation
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

# Correlation coefficients
INTRA_TECH_CORR = 0.6  # Tech stocks move together
INTRA_FINANCE_CORR = 0.5  # Finance stocks move together
CROSS_GROUP_CORR = 0.3  # Between sectors / unknown tickers
TSLA_CORR = 0.3  # TSLA does its own thing
```

These values are tuned for *visual drama*, not financial realism — high-sigma
names (TSLA, NVDA) visibly jump around while low-sigma names (JPM, V) are calm.
`CROSS_GROUP_CORR` doubles as the fallback for any unknown / cross-sector pair, so
no separate "default correlation" constant is needed.

---

## 7. GBM Simulator — `simulator.py`

Two classes:

- **`GBMSimulator`** — pure math engine. Stateful: holds current prices and
  advances them one step at a time. No async, no I/O — trivially unit-testable.
- **`SimulatorDataSource`** — the `MarketDataSource` implementation that wraps the
  engine in an async loop and writes to the `PriceCache`.

### 7.1 The math

Each tick advances every price by one step of **Geometric Brownian Motion**:

```
S(t+dt) = S(t) · exp((μ − σ²/2)·dt + σ·√dt·Z)
```

where `μ` is annualized drift, `σ` annualized volatility, `dt` the time step as a
fraction of a trading year, and `Z` a **correlated** standard-normal draw. Because
the price is multiplied by `exp(...)`, it can never go negative — a key property
for a price series. The `dt` is tiny (~8.5e-8 for a 500ms tick across 252 trading
days × 6.5h), so each tick is a sub-cent nudge that accumulates naturally.

Correlation is introduced by drawing `n` independent normals and multiplying by the
**Cholesky factor** `L` of the correlation matrix `C` (where `C = L·Lᵀ`). The
product `L·z` is a vector of correlated normals, so tech names tend to move
together while finance names form their own cluster.

### 7.2 `GBMSimulator` — the math engine

```python
"""GBM-based market simulator."""

from __future__ import annotations

import asyncio
import logging
import math
import random

import numpy as np

from .cache import PriceCache
from .interface import MarketDataSource
from .seed_prices import (
    CORRELATION_GROUPS,
    CROSS_GROUP_CORR,
    DEFAULT_PARAMS,
    INTRA_FINANCE_CORR,
    INTRA_TECH_CORR,
    SEED_PRICES,
    TICKER_PARAMS,
    TSLA_CORR,
)

logger = logging.getLogger(__name__)


class GBMSimulator:
    """Geometric Brownian Motion simulator for correlated stock prices.

    Math:
        S(t+dt) = S(t) * exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z)
    """

    # 500ms expressed as a fraction of a trading year
    # 252 trading days * 6.5 hours/day * 3600 seconds/hour = 5,896,800 seconds
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR  # ~8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
    ) -> None:
        self._dt = dt
        self._event_prob = event_probability

        # Per-ticker state
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}

        # Cholesky decomposition of the correlation matrix (for correlated moves)
        self._cholesky: np.ndarray | None = None

        # Initialize all starting tickers
        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    # --- Public API ---

    def step(self) -> dict[str, float]:
        """Advance all tickers by one time step. Returns {ticker: new_price}.

        This is the hot path — called every 500ms. Keep it fast.
        """
        n = len(self._tickers)
        if n == 0:
            return {}

        # Generate n independent standard normal draws
        z_independent = np.random.standard_normal(n)

        # Apply Cholesky to get correlated draws
        if self._cholesky is not None:
            z_correlated = self._cholesky @ z_independent
        else:
            z_correlated = z_independent

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            params = self._params[ticker]
            mu = params["mu"]
            sigma = params["sigma"]

            # GBM: S(t+dt) = S(t) * exp((mu - 0.5*sigma^2)*dt + sigma*sqrt(dt)*Z)
            drift = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z_correlated[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            # Random event: ~0.1% chance per tick per ticker
            # With 10 tickers at 2 ticks/sec, expect an event ~every 50 seconds
            if random.random() < self._event_prob:
                shock_magnitude = random.uniform(0.02, 0.05)
                shock_sign = random.choice([-1, 1])
                self._prices[ticker] *= 1 + shock_magnitude * shock_sign
                logger.debug(
                    "Random event on %s: %.1f%% %s",
                    ticker,
                    shock_magnitude * 100,
                    "up" if shock_sign > 0 else "down",
                )

            result[ticker] = round(self._prices[ticker], 2)

        return result

    def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the simulation. Rebuilds the correlation matrix."""
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the simulation. Rebuilds the correlation matrix."""
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        """Current price for a ticker, or None if not tracked."""
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        """Return the list of currently tracked tickers."""
        return list(self._tickers)

    # --- Internals ---

    def _add_ticker_internal(self, ticker: str) -> None:
        """Add a ticker without rebuilding Cholesky (for batch initialization)."""
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        """Rebuild the Cholesky decomposition of the ticker correlation matrix.

        Called whenever tickers are added or removed. O(n^2) but n < 50.
        """
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return

        # Build the correlation matrix
        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = rho
                corr[j, i] = rho

        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        """Determine correlation between two tickers based on sector grouping."""
        tech = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]

        # TSLA is in the tech set but behaves independently
        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR

        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR

        return CROSS_GROUP_CORR
```

**Notes on the engine**

- **Vectorized draws.** `np.random.standard_normal(n)` plus a single matrix-vector
  product is the whole correlation cost per tick; the per-ticker Python loop only
  applies the scalar GBM formula.
- **Cholesky rebuilds on membership change only**, not per tick. With `n < 50`
  the O(n²) rebuild is trivial and happens at most a few times per session.
- **`n <= 1 → no matrix.** A 1×1 correlation matrix is pointless, so `step()`
  falls back to the independent draw. This is also why a brand-new simulator with
  one ticker has `_cholesky is None`.
- **Random shock events** give the demo its drama: ~0.1% per ticker per tick of a
  2–5% jump in either direction.
- **Unknown tickers** (added dynamically, not in `SEED_PRICES`) get a random seed
  price in `[50, 300]` and the default σ/μ — so the AI or user can add any symbol
  and it behaves plausibly.

### 7.3 `SimulatorDataSource` — async wrapper

```python
class SimulatorDataSource(MarketDataSource):
    """MarketDataSource backed by the GBM simulator.

    Runs a background asyncio task that calls GBMSimulator.step() every
    `update_interval` seconds and writes results to the PriceCache.
    """

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(
            tickers=tickers,
            event_probability=self._event_prob,
        )
        # Seed the cache with initial prices so SSE has data immediately
        for ticker in tickers:
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(tickers))

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("Simulator stopped")

    async def add_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.add_ticker(ticker)
            # Seed cache immediately so the ticker has a price right away
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
            logger.info("Simulator: added ticker %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        """Core loop: step the simulation, write to cache, sleep."""
        while True:
            try:
                if self._sim:
                    prices = self._sim.step()
                    for ticker, price in prices.items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

**Key behaviors**

- **Immediate seeding.** `start()` and `add_ticker()` write seed prices to the
  cache *before* the loop ticks, so SSE has data on its very first send and a newly
  added ticker is immediately tradeable — no blank-screen gap.
- **Graceful cancellation.** `stop()` cancels the task and awaits it, swallowing
  `CancelledError`. Safe to call twice (the `not self._task.done()` guard), which
  matters for clean FastAPI lifespan teardown.
- **Per-tick exception isolation.** The loop wraps `step()` in try/except so a
  single bad tick logs and continues rather than killing the entire feed.
- **`get_tickers()` is public on the engine**, so the data source never reaches
  into a private attribute.

---

## 8. Massive API Client — `massive_client.py`

Polls the Massive (Polygon.io) REST snapshot endpoint for the union of watched
tickers in one call, then writes results into the same `PriceCache`. The Massive
`RESTClient` is synchronous, so each fetch runs in `asyncio.to_thread()` to avoid
blocking the event loop.

```python
"""Massive (Polygon.io) API client for real market data."""

from __future__ import annotations

import asyncio
import logging

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by the Massive (Polygon.io) REST API.

    Polls GET /v2/snapshot/locale/us/markets/stocks/tickers for all watched
    tickers in a single API call, then writes results to the PriceCache.

    Rate limits:
      - Free tier: 5 req/min → poll every 15s (default)
      - Paid tiers: higher limits → poll every 2-5s
    """

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)

        # Do an immediate first poll so the cache has data right away
        await self._poll_once()

        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info(
            "Massive poller started: %d tickers, %.1fs interval",
            len(tickers),
            self._interval,
        )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            logger.info("Massive: added ticker %s (will appear on next poll)", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Internal ---

    async def _poll_loop(self) -> None:
        """Poll on interval. First poll already happened in start()."""
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        """Execute one poll cycle: fetch snapshots, update cache."""
        if not self._tickers or not self._client:
            return

        try:
            # The Massive RESTClient is synchronous — run in a thread to
            # avoid blocking the event loop.
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            processed = 0
            for snap in snapshots:
                try:
                    price = snap.last_trade.price
                    # Massive timestamps are Unix milliseconds → convert to seconds
                    timestamp = snap.last_trade.timestamp / 1000.0
                    self._cache.update(
                        ticker=snap.ticker,
                        price=price,
                        timestamp=timestamp,
                    )
                    processed += 1
                except (AttributeError, TypeError) as e:
                    logger.warning(
                        "Skipping snapshot for %s: %s",
                        getattr(snap, "ticker", "???"),
                        e,
                    )
            logger.debug("Massive poll: updated %d/%d tickers", processed, len(self._tickers))

        except Exception as e:
            logger.error("Massive poll failed: %s", e)
            # Don't re-raise — the loop will retry on the next interval.
            # Common failures: 401 (bad key), 429 (rate limit), network errors.

    def _fetch_snapshots(self) -> list:
        """Synchronous call to the Massive REST API. Runs in a thread."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=self._tickers,
        )
```

**Symmetry with the simulator.** The two sources are interchangeable because both:
implement the same five-method interface; write `PriceUpdate`s into the same cache
via `cache.update(...)`; do an immediate first fill in `start()`; and cancel a
single background task in `stop()`. The only differences are timing (15s poll vs
500ms tick) and the source of the numbers.

**Why `massive` is a top-level import.** `massive` is a declared core dependency
(`backend/pyproject.toml`), so the import lives at module top. The factory (§9)
never constructs a `MassiveDataSource` unless `MASSIVE_API_KEY` is set, so the
simulator path never exercises the Massive code — but the package is always present
in the environment, which keeps imports simple and type-checkable.

**Error-handling philosophy — the poller is deliberately resilient:**

| Error | Behavior |
|-------|----------|
| **401 Unauthorized** (bad key) | Logged as error; poller keeps running so a fixed key + restart recovers. |
| **429 Rate Limited** | Logged; next poll retries after `poll_interval`. |
| **Network timeout** | Logged; retries automatically next cycle. |
| **Malformed snapshot** (missing `last_trade`) | That one ticker is skipped with a warning; others still processed. |
| **Whole poll fails** | Cache keeps last-known prices; SSE keeps streaming slightly stale data — better than a blank UI. |

Because `update()` always derives `previous_price` from the cache, even Massive's
15-second gaps produce correct `change`/`direction` on each refresh.

---

## 9. Factory — `factory.py`

A single environment-driven switch. This is the only place that decides which
implementation runs, so the rest of the app is provider-agnostic.

```python
"""Factory for creating market data sources."""

from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Create the appropriate market data source based on environment variables.

    - MASSIVE_API_KEY set and non-empty → MassiveDataSource (real market data)
    - Otherwise → SimulatorDataSource (GBM simulation)

    Returns an unstarted source. Caller must await source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        logger.info("Market data source: Massive API (real data)")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    else:
        logger.info("Market data source: GBM Simulator")
        return SimulatorDataSource(price_cache=price_cache)
```

```python
# Usage at app startup
price_cache = PriceCache()
source = create_market_data_source(price_cache)   # picks Massive or Simulator
await source.start(["AAPL", "GOOGL", "MSFT", ...])  # caller must start it
```

`.strip()` means a whitespace-only `MASSIVE_API_KEY=` in `.env` is treated as
absent, so the default simulator path is robust to an empty assignment.

---

## 10. SSE Streaming Endpoint — `stream.py`

A FastAPI route that holds a long-lived `text/event-stream` connection and pushes
all tracked prices to the browser, which consumes them via the native
`EventSource` API.

```python
"""SSE streaming endpoint for live price updates."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/stream", tags=["streaming"])


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Create the SSE streaming router with a reference to the price cache.

    This factory pattern lets us inject the PriceCache without globals.
    """

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        """SSE endpoint for live price updates.

        Streams all tracked ticker prices every ~500ms. The client connects
        with EventSource and receives events in the format:

            data: {"AAPL": {"ticker": "AAPL", "price": 190.50, ...}, ...}
        """
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Disable nginx buffering if proxied
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
) -> AsyncGenerator[str, None]:
    """Async generator that yields SSE-formatted price events.

    Sends all prices every `interval` seconds. Stops when the client
    disconnects (detected via request.is_disconnected()).
    """
    # Tell the client to retry after 1 second if the connection drops
    yield "retry: 1000\n\n"

    last_version = -1
    client_ip = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client_ip)

    try:
        while True:
            # Check for client disconnect
            if await request.is_disconnected():
                logger.info("SSE client disconnected: %s", client_ip)
                break

            current_version = price_cache.version
            if current_version != last_version:
                last_version = current_version
                prices = price_cache.get_all()

                if prices:
                    data = {ticker: update.to_dict() for ticker, update in prices.items()}
                    payload = json.dumps(data)
                    yield f"data: {payload}\n\n"

            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("SSE stream cancelled for: %s", client_ip)
```

### Wire format

Each event batches **all** tracked tickers into a single JSON object keyed by
symbol (matching PLAN.md §6):

```
data: {"AAPL":{"ticker":"AAPL","price":190.50,"previous_price":190.42,"timestamp":1707580800.5,"change":0.08,"change_percent":0.042,"direction":"up"},"GOOGL":{"ticker":"GOOGL","price":175.12,...}}

```

### Client usage

```javascript
const eventSource = new EventSource('/api/stream/prices');

eventSource.onmessage = (event) => {
  const prices = JSON.parse(event.data);
  // prices = { AAPL: { ticker, price, previous_price, change, direction, ... }, ... }
  for (const [ticker, p] of Object.entries(prices)) {
    updateTicker(ticker, p);   // flash green/up or red/down, append to sparkline
  }
};

eventSource.onerror = () => {
  // EventSource auto-reconnects using the server's `retry: 1000` directive.
};
```

**Design notes**

- **`create_stream_router(cache)` factory** — injects the cache without module
  globals, mirroring the rest of the subsystem.
- **Version-gated sends** — re-serialize and push only when `cache.version`
  changed; idle ticks cost a sleep, nothing more. Important when Massive only
  refreshes every 15s.
- **Disconnect detection** — `request.is_disconnected()` breaks the loop so a
  closed tab frees the connection promptly.
- **`retry: 1000`** — primes `EventSource`'s built-in reconnect to ~1s.
- **`X-Accel-Buffering: no`** — disables nginx response buffering, so events
  aren't held back when deployed behind a proxy.
- **Typed generator** — annotated `AsyncGenerator[str, None]`; it yields SSE-frame
  strings only.

---

## 11. Public API — `__init__.py`

The package re-exports exactly what the rest of the backend needs and nothing
more.

```python
"""Market data subsystem for FinAlly.

Public API:
    PriceUpdate         - Immutable price snapshot dataclass
    PriceCache          - Thread-safe in-memory price store
    MarketDataSource    - Abstract interface for data providers
    create_market_data_source - Factory that selects simulator or Massive
    create_stream_router - FastAPI router factory for SSE endpoint
"""

from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate
from .stream import create_stream_router

__all__ = [
    "PriceUpdate",
    "PriceCache",
    "MarketDataSource",
    "create_market_data_source",
    "create_stream_router",
]
```

```python
# Everything downstream imports from the package root:
from app.market import (
    PriceCache,
    PriceUpdate,
    MarketDataSource,
    create_market_data_source,
    create_stream_router,
)
```

---

## 12. FastAPI Lifecycle Integration

The market data system starts and stops with the application via the `lifespan`
context manager (per PLAN.md §13). State is stashed on `app.state` so routers can
read it through dependencies.

**In `backend/app/main.py`:**

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.market import (
    PriceCache,
    MarketDataSource,
    create_market_data_source,
    create_stream_router,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage startup and shutdown of background services."""
    # --- STARTUP ---
    # 1. Shared price cache
    price_cache = PriceCache()
    app.state.price_cache = price_cache

    # 2. Market data source (Massive if key set, else simulator)
    source = create_market_data_source(price_cache)
    app.state.market_source = source

    # 3. Load initial tickers from the seeded watchlist, then start producing
    initial_tickers = get_watchlist_tickers()   # reads SQLite watchlist table
    await source.start(initial_tickers)

    # 4. Register the SSE streaming router
    app.include_router(create_stream_router(price_cache))

    yield  # ---- app is serving ----

    # --- SHUTDOWN ---
    await source.stop()


app = FastAPI(title="FinAlly", lifespan=lifespan)


# Dependencies for injecting market state into route handlers
def get_price_cache() -> PriceCache:
    return app.state.price_cache


def get_market_source() -> MarketDataSource:
    return app.state.market_source
```

### Consuming market data from other routes

```python
from fastapi import APIRouter, Depends, HTTPException

router = APIRouter(prefix="/api")


@router.post("/portfolio/trade")
async def execute_trade(
    trade: TradeRequest,
    price_cache: PriceCache = Depends(get_price_cache),
):
    current_price = price_cache.get_price(trade.ticker)
    if current_price is None:
        raise HTTPException(400, f"Price not yet available for {trade.ticker}.")
    # ... fill at current_price, update positions/cash ...


@router.get("/portfolio")
async def get_portfolio(price_cache: PriceCache = Depends(get_price_cache)):
    # total_value = cash + Σ(qty × current_price), falling back to avg_cost
    # when the cache has no price for a held ticker yet (startup race).
    ...
```

---

## 13. Watchlist Coordination

When the watchlist changes — via REST or the LLM chat tool — the route must notify
the running source so prices start/stop flowing. This coupling is required for
newly added tickers to appear live (PLAN.md §13).

### Add a ticker

```
POST /api/watchlist {ticker: "PYPL"}
  → INSERT into watchlist table (SQLite)
  → await source.add_ticker("PYPL")
        Simulator: add to GBMSimulator, rebuild Cholesky, seed cache immediately
        Massive:   append to ticker list, appears on next poll
  → respond with ticker (+ current price if cached)
```

### Remove a ticker

```
DELETE /api/watchlist/PYPL
  → DELETE from watchlist table (SQLite)
  → await source.remove_ticker("PYPL")
        Simulator: remove from engine, rebuild Cholesky, drop from cache
        Massive:   drop from ticker list, drop from cache
  → respond success
```

### Edge case: removing a ticker you still hold

If a held ticker leaves the watchlist, keep tracking it so portfolio valuation
stays accurate:

```python
@router.delete("/watchlist/{ticker}")
async def remove_from_watchlist(
    ticker: str,
    source: MarketDataSource = Depends(get_market_source),
):
    await db.delete_watchlist_entry(ticker)

    # Only stop tracking if there is no open position
    position = await db.get_position(ticker)
    if position is None or position.quantity == 0:
        await source.remove_ticker(ticker)

    return {"status": "ok"}
```

Example handler wiring for adds:

```python
@router.post("/watchlist")
async def add_to_watchlist(
    payload: WatchlistAdd,
    source: MarketDataSource = Depends(get_market_source),
    price_cache: PriceCache = Depends(get_price_cache),
):
    ticker = payload.ticker.upper().strip()
    await db.add_watchlist_entry(ticker)
    await source.add_ticker(ticker)               # start live prices flowing
    return {"ticker": ticker, "price": price_cache.get_price(ticker)}
```

---

## 14. Testing Strategy

Tests live in `backend/tests/market/`. Run with:

```bash
cd backend
uv run --extra dev pytest -v
uv run --extra dev pytest --cov=app
```

The split mirrors the modules: pure-math classes are tested synchronously; async
sources use `pytest.mark.asyncio` with short intervals; the Massive client is
tested with mocked snapshots (no network).

### 14.1 `GBMSimulator` (pure, synchronous)

```python
import pytest
from app.market.simulator import GBMSimulator
from app.market.seed_prices import SEED_PRICES


def test_step_returns_all_tickers():
    sim = GBMSimulator(tickers=["AAPL", "GOOGL"])
    assert set(sim.step().keys()) == {"AAPL", "GOOGL"}


def test_prices_are_always_positive():
    """GBM multiplies by exp(...), so price can never go negative."""
    sim = GBMSimulator(tickers=["TSLA"])
    for _ in range(10_000):
        assert sim.step()["TSLA"] > 0


def test_initial_price_matches_seed():
    sim = GBMSimulator(tickers=["AAPL"])
    assert sim.get_price("AAPL") == SEED_PRICES["AAPL"]


def test_add_and_remove_ticker():
    sim = GBMSimulator(tickers=["AAPL"])
    sim.add_ticker("TSLA")
    assert "TSLA" in sim.step()
    sim.remove_ticker("TSLA")
    assert "TSLA" not in sim.step()


def test_unknown_ticker_gets_random_seed():
    sim = GBMSimulator(tickers=["ZZZZ"])
    assert 50.0 <= sim.get_price("ZZZZ") <= 300.0


def test_cholesky_built_only_when_multiple_tickers():
    sim = GBMSimulator(tickers=["AAPL"])
    assert sim._cholesky is None
    sim.add_ticker("GOOGL")
    assert sim._cholesky is not None


def test_empty_simulator_steps_cleanly():
    assert GBMSimulator(tickers=[]).step() == {}
```

### 14.2 `PriceCache`

```python
from app.market.cache import PriceCache


def test_first_update_is_flat():
    cache = PriceCache()
    u = cache.update("AAPL", 190.50)
    assert u.direction == "flat" and u.previous_price == 190.50


def test_direction_and_change():
    cache = PriceCache()
    cache.update("AAPL", 190.00)
    up = cache.update("AAPL", 191.00)
    assert up.direction == "up" and up.change == 1.00
    down = cache.update("AAPL", 189.00)
    assert down.direction == "down" and down.change == -2.00


def test_version_increments_on_every_update():
    cache = PriceCache()
    v0 = cache.version
    cache.update("AAPL", 190.00)
    cache.update("AAPL", 191.00)
    assert cache.version == v0 + 2


def test_remove_and_get_price():
    cache = PriceCache()
    cache.update("AAPL", 190.50)
    assert cache.get_price("AAPL") == 190.50
    cache.remove("AAPL")
    assert cache.get_price("AAPL") is None
```

### 14.3 `SimulatorDataSource` (async integration)

```python
import asyncio
import pytest
from app.market.cache import PriceCache
from app.market.simulator import SimulatorDataSource


@pytest.mark.asyncio
async def test_start_seeds_cache_immediately():
    cache = PriceCache()
    source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
    await source.start(["AAPL", "GOOGL"])
    assert cache.get("AAPL") is not None      # seeded before first tick
    assert cache.get("GOOGL") is not None
    await source.stop()


@pytest.mark.asyncio
async def test_add_remove_ticker_round_trip():
    cache = PriceCache()
    source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
    await source.start(["AAPL"])
    await source.add_ticker("TSLA")
    assert "TSLA" in source.get_tickers() and cache.get("TSLA") is not None
    await source.remove_ticker("TSLA")
    assert "TSLA" not in source.get_tickers() and cache.get("TSLA") is None
    await source.stop()


@pytest.mark.asyncio
async def test_double_stop_is_safe():
    source = SimulatorDataSource(price_cache=PriceCache(), update_interval=0.1)
    await source.start(["AAPL"])
    await source.stop()
    await source.stop()   # must not raise
```

### 14.4 `MassiveDataSource` (mocked — no network)

```python
import pytest
from unittest.mock import MagicMock, patch
from app.market.cache import PriceCache
from app.market.massive_client import MassiveDataSource


def _snapshot(ticker, price, ts_ms):
    snap = MagicMock()
    snap.ticker = ticker
    snap.last_trade.price = price
    snap.last_trade.timestamp = ts_ms
    return snap


@pytest.mark.asyncio
async def test_poll_writes_prices_and_converts_timestamp():
    cache = PriceCache()
    source = MassiveDataSource("test-key", cache, poll_interval=60.0)
    snaps = [_snapshot("AAPL", 190.50, 1707580800000),
             _snapshot("GOOGL", 175.25, 1707580800000)]
    with patch.object(source, "_fetch_snapshots", return_value=snaps):
        await source._poll_once()
    assert cache.get_price("AAPL") == 190.50
    assert cache.get("AAPL").timestamp == 1707580800.0   # ms → s


@pytest.mark.asyncio
async def test_malformed_snapshot_is_skipped():
    cache = PriceCache()
    source = MassiveDataSource("test-key", cache, poll_interval=60.0)
    source._tickers = ["AAPL", "BAD"]
    good = _snapshot("AAPL", 190.50, 1707580800000)
    bad = MagicMock(); bad.ticker = "BAD"; bad.last_trade = None
    with patch.object(source, "_fetch_snapshots", return_value=[good, bad]):
        await source._poll_once()
    assert cache.get_price("AAPL") == 190.50
    assert cache.get_price("BAD") is None


@pytest.mark.asyncio
async def test_api_error_does_not_crash_loop():
    cache = PriceCache()
    source = MassiveDataSource("test-key", cache, poll_interval=60.0)
    source._tickers = ["AAPL"]
    with patch.object(source, "_fetch_snapshots", side_effect=Exception("network")):
        await source._poll_once()        # must swallow and continue
    assert cache.get_price("AAPL") is None
```

### 14.5 `create_market_data_source` (factory)

```python
import pytest
from app.market.cache import PriceCache
from app.market.factory import create_market_data_source
from app.market.simulator import SimulatorDataSource
from app.market.massive_client import MassiveDataSource


def test_no_key_returns_simulator(monkeypatch):
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    assert isinstance(create_market_data_source(PriceCache()), SimulatorDataSource)


def test_key_returns_massive(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "abc123")
    assert isinstance(create_market_data_source(PriceCache()), MassiveDataSource)


def test_whitespace_key_is_treated_as_absent(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "   ")
    assert isinstance(create_market_data_source(PriceCache()), SimulatorDataSource)
```

---

## 15. Error Handling & Edge Cases

| Scenario | Handling |
|----------|----------|
| **Empty watchlist at startup** | `start([])` is valid: simulator produces nothing, Massive skips its call, SSE sends empty events. Adding a ticker later begins tracking immediately. |
| **Trade on a ticker with no cached price** | `price_cache.get_price()` returns `None`; the trade route returns HTTP 400 ("price not yet available"). The simulator avoids this by seeding on `add_ticker()`; Massive may have a brief pre-first-poll gap. |
| **Portfolio valuation race** | If a held ticker has no cache entry yet, fall back to `position.avg_cost` for that line (PLAN.md §13). |
| **Invalid Massive API key** | First poll fails 401; logged; poller keeps running. SSE stays connected but streams empty data until the key is fixed and the app restarts. |
| **Massive rate limit / timeout** | Logged; cache retains last-known prices; next poll retries. UI shows slightly stale prices rather than going blank. |
| **Single bad simulator tick** | `_run_loop` try/except logs and continues; the feed never dies from one exception. |
| **Client disconnect** | `request.is_disconnected()` breaks the SSE loop and releases the connection. |
| **Float precision** | Prices are `round()`ed to 2 dp; the exponential GBM form is numerically stable and always positive. |
| **Lock contention** | At ~10 tickers × 2 updates/sec the `threading.Lock` critical section (one dict op) is negligible. Hundreds of tickers would warrant a read/write lock — unnecessary here. |

---

## 16. Configuration Summary

| Parameter | Location | Default | Description |
|-----------|----------|---------|-------------|
| `MASSIVE_API_KEY` | env var | `""` | Set → Massive API; empty/whitespace → simulator |
| `update_interval` | `SimulatorDataSource.__init__` | `0.5` s | Simulator tick cadence |
| `event_probability` | `GBMSimulator` / `SimulatorDataSource` | `0.001` | Per-ticker, per-tick chance of a 2–5% shock |
| `dt` | `GBMSimulator.__init__` | `~8.48e-8` | GBM step as a fraction of a trading year |
| `poll_interval` | `MassiveDataSource.__init__` | `15.0` s | Massive poll cadence (free tier = 5 req/min) |
| SSE push interval | `_generate_events(interval=)` | `0.5` s | How often the SSE loop checks the cache |
| SSE retry directive | `_generate_events` | `1000` ms | `EventSource` reconnect delay |
| Default watchlist | `seed_prices.SEED_PRICES` | 10 tickers | AAPL, GOOGL, MSFT, AMZN, TSLA, NVDA, META, JPM, V, NFLX |

### Quick start (downstream usage)

```python
from app.market import PriceCache, create_market_data_source

cache = PriceCache()
source = create_market_data_source(cache)         # reads MASSIVE_API_KEY
await source.start(["AAPL", "GOOGL", "MSFT"])     # begin streaming

cache.get("AAPL")        # → PriceUpdate | None
cache.get_price("AAPL")  # → float | None
cache.get_all()          # → dict[str, PriceUpdate]

await source.add_ticker("TSLA")
await source.remove_ticker("GOOGL")

await source.stop()                                # clean shutdown
```
