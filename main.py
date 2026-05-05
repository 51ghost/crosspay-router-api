"""
CrossPay Router API — Cross-border payment routing with live FX rates

FastAPI application exposing:
    /v1/rates     — Live FX rates
    /v1/convert   — Currency conversion
    /v1/routes    — Optimal payment routes
    /v1/fees      — Fee estimation
    /v1/health    — Health check
"""

import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from data_pipeline import (
    clear_cache,
    convert_amount,
    estimate_fees,
    find_routes,
    get_all_currencies,
    get_rates,
)

# ─── App ──────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="CrossPay Router API",
    description="Cross-border payment routing with live FX rates. "
    "Supports 170+ currencies with real-time rates from the European Central Bank.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# ─── CORS ────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Rate Limiter (slowapi) ──────────────────────────────────────────────────
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address, default_limits=["100/minute"])
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# ─── Auth ────────────────────────────────────────────────────────────────────
API_KEY = os.environ.get("CROSSPAY_API_KEY", "dev-key-1234")

# We'll keep a simple auth dependency that checks an X-API-Key header.
# For RapidAPI compatibility, we also check X-RapidAPI-Proxy-Secret and
# X-RapidAPI-Key (standard RapidAPI headers).


async def verify_auth(request: Request) -> None:
    """Verify API key in request headers."""
    # Skip auth for health endpoint
    if request.url.path == "/v1/health":
        return

    provided = (
        request.headers.get("X-API-Key")
        or request.headers.get("X-RapidAPI-Proxy-Secret")
        or request.headers.get("X-RapidAPI-Key")
    )

    if provided != API_KEY:
        # In production, you'd check against a DB of valid keys.
        # For RapidAPI, the platform handles auth — we just pass through.
        # Still, we enforce our key for direct access.
        if provided:
            raise HTTPException(status_code=401, detail="Invalid API key")
        else:
            raise HTTPException(status_code=401, detail="Missing API key")


# ─── Schemas ─────────────────────────────────────────────────────────────────


class RateResponse(BaseModel):
    base: str
    date: str
    rates: Dict[str, float]
    source: str
    timestamp: str


class ConvertRequest(BaseModel):
    from_currency: str = Field(..., alias="from")
    to_currency: str = Field(..., alias="to")
    amount: float = Field(..., gt=0)

    class Config:
        populate_by_name = True


class ConvertResponse(BaseModel):
    from_currency: str
    to_currency: str
    amount: float
    converted_amount: float
    rate: float
    timestamp: str


class FeeRequest(BaseModel):
    from_currency: str = Field(..., alias="from")
    to_currency: str = Field(..., alias="to")
    amount: float = Field(..., gt=0)

    class Config:
        populate_by_name = True


class FeeResponse(BaseModel):
    amount: float
    from_currency: str
    to_currency: str
    fixed_fee: float
    variable_fee: float
    cross_border_surcharge: float
    swift_fee: float
    swift_applies: bool
    total_fee: float
    total_fee_pct: float


class RouteRequest(BaseModel):
    from_currency: str = Field(..., alias="from")
    to_currency: str = Field(..., alias="to")
    amount: float = Field(..., gt=0)

    class Config:
        populate_by_name = True


class RouteOption(BaseModel):
    route: List[str]
    hops: int
    intermediate: Optional[str] = None
    final_amount: float
    total_fees: float
    effective_rate: float


class RouteResponse(BaseModel):
    from_currency: str
    to_currency: str
    amount: float
    routes: List[RouteOption]
    recommended: RouteOption


class HealthResponse(BaseModel):
    status: str
    version: str
    uptime_seconds: float
    currencies_supported: int
    timestamp: str

# ─── Startup ─────────────────────────────────────────────────────────────────

_start_time = time.time()


@app.on_event("startup")
async def startup():
    # Warm the cache with default USD rates
    try:
        await get_rates("USD")
    except Exception:
        pass  # Offline fallback kicks in

# ─── Endpoints ───────────────────────────────────────────────────────────────


@app.get(
    "/v1/health",
    response_model=HealthResponse,
    tags=["System"],
    summary="Health check",
)
async def health():
    """Check API health and status."""
    currencies = await get_all_currencies()
    return HealthResponse(
        status="ok",
        version="1.0.0",
        uptime_seconds=round(time.time() - _start_time, 2),
        currencies_supported=len(currencies),
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


@app.get(
    "/v1/rates",
    response_model=RateResponse,
    tags=["FX Rates"],
    summary="Get live FX rates",
)
@limiter.limit("30/minute")
async def rates(
    request: Request,
    base: str = Query("USD", description="Base currency code"),
    symbols: Optional[str] = Query(
        None, description="Comma-separated list of target currencies"
    ),
    force_refresh: bool = Query(False, description="Bypass cache"),
):
    """Get live foreign exchange rates.

    - **base**: Base currency (default: USD)
    - **symbols**: Comma-separated target currencies (optional, returns all if omitted)
    - **force_refresh**: Bypass 5-min cache
    """
    await verify_auth(request)

    parsed_symbols = [s.strip().upper() for s in symbols.split(",")] if symbols else None

    try:
        data = await get_rates(base.upper(), parsed_symbols, force_refresh)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch rates: {str(e)}")

    return RateResponse(
        base=data["base"],
        date=data["date"],
        rates=data["rates"],
        source=data.get("source", "live"),
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


@app.post(
    "/v1/convert",
    response_model=ConvertResponse,
    tags=["Conversion"],
    summary="Convert currency",
)
@limiter.limit("30/minute")
async def convert(request: Request, body: ConvertRequest):
    """Convert an amount from one currency to another using live rates."""
    await verify_auth(request)

    try:
        rates_data = await get_rates("USD")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch rates: {str(e)}")

    rate = convert_amount(1.0, body.from_currency, body.to_currency, rates_data["rates"], "USD")
    converted = body.amount * rate

    return ConvertResponse(
        from_currency=body.from_currency.upper(),
        to_currency=body.to_currency.upper(),
        amount=body.amount,
        converted_amount=round(converted, 6),
        rate=round(rate, 6),
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


@app.post(
    "/v1/routes",
    response_model=RouteResponse,
    tags=["Routing"],
    summary="Find optimal payment routes",
)
@limiter.limit("20/minute")
async def routes_endpoint(request: Request, body: RouteRequest):
    """Find the best payment routes between two currencies.

    Returns multiple route options (direct, via USD, via EUR) sorted by best rate.
    The **recommended** field contains the optimal route.
    """
    await verify_auth(request)

    try:
        rates_data = await get_rates("USD")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch rates: {str(e)}")

    route_options = find_routes(
        body.from_currency,
        body.to_currency,
        body.amount,
        rates_data["rates"],
        "USD",
        max_hops=2,
    )

    if not route_options:
        raise HTTPException(
            status_code=400,
            detail=f"No routes found between {body.from_currency} and {body.to_currency}",
        )

    return RouteResponse(
        from_currency=body.from_currency.upper(),
        to_currency=body.to_currency.upper(),
        amount=body.amount,
        routes=[RouteOption(**r) for r in route_options],
        recommended=RouteOption(**route_options[0]),
    )


@app.post(
    "/v1/fees",
    response_model=FeeResponse,
    tags=["Fees"],
    summary="Estimate transaction fees",
)
@limiter.limit("30/minute")
async def fees(request: Request, body: FeeRequest):
    """Estimate fees for a cross-border payment.

    Fee model includes:
    - Fixed processing fee ($0.50 USD equivalent)
    - Variable fee (0.5% of amount)
    - Cross-border surcharge (0.25%)
    - SWIFT fee ($15 for inter-region transfers)
    """
    await verify_auth(request)

    fee_data = estimate_fees(body.amount, body.from_currency, body.to_currency)

    return FeeResponse(
        amount=fee_data["amount"],
        from_currency=fee_data["from_currency"],
        to_currency=fee_data["to_currency"],
        fixed_fee=fee_data["fixed_fee"],
        variable_fee=fee_data["variable_fee"],
        cross_border_surcharge=fee_data["cross_border_surcharge"],
        swift_fee=fee_data["swift_fee"],
        swift_applies=fee_data["swift_applies"],
        total_fee=fee_data["total_fee"],
        total_fee_pct=fee_data["total_fee_pct"],
    )


# ─── Admin endpoints ─────────────────────────────────────────────────────────


@app.post(
    "/v1/admin/cache/clear",
    tags=["Admin"],
    summary="Clear rate cache",
)
@limiter.limit("5/minute")
async def clear_cache_endpoint(request: Request):
    """Clear the in-memory FX rate cache (admin only)."""
    await verify_auth(request)
    cleared = clear_cache()
    return {"status": "ok", "cache_entries_cleared": cleared}


# ─── Entrypoint ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run("main:app", host=host, port=port, reload=True)
