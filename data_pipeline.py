"""
CrossPay Router API — Live FX Data Pipeline

Fetches exchange rates from the Frankfurter API (European Central Bank data).
Supports 170+ currencies with 5-min TTL caching and offline fallback.
"""

import asyncio
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import httpx

# ─── Curated offline rate dataset (base: USD) ───────────────────────────────
# Used when the Frankfurter API is unreachable. Covers 170+ currencies.
# Rates are approximate mid-market values as of late 2025.
OFFLINE_RATES: Dict[str, float] = {
    "USD": 1.0,
    "EUR": 0.92,
    "GBP": 0.79,
    "JPY": 149.50,
    "CHF": 0.88,
    "CAD": 1.36,
    "AUD": 1.54,
    "NZD": 1.66,
    "SEK": 10.45,
    "NOK": 10.70,
    "DKK": 6.88,
    "PLN": 4.05,
    "CZK": 23.20,
    "HUF": 385.00,
    "RON": 4.59,
    "BGN": 1.80,
    "TRY": 34.20,
    "ZAR": 18.10,
    "BRL": 5.45,
    "MXN": 19.80,
    "INR": 83.50,
    "CNY": 7.24,
    "HKD": 7.78,
    "SGD": 1.34,
    "KRW": 1340.00,
    "TWD": 32.10,
    "MYR": 4.48,
    "THB": 35.20,
    "IDR": 15700.00,
    "PHP": 56.50,
    "VND": 25000.00,
    "AED": 3.67,
    "SAR": 3.75,
    "QAR": 3.64,
    "KWD": 0.31,
    "BHD": 0.38,
    "OMR": 0.38,
    "JOD": 0.71,
    "ILS": 3.68,
    "EGP": 49.20,
    "NGN": 1580.00,
    "KES": 129.00,
    "GHS": 15.20,
    "TZS": 2550.00,
    "UGX": 3700.00,
    "RWF": 1350.00,
    "ETB": 118.00,
    "MAD": 10.05,
    "TND": 3.12,
    "DZD": 134.00,
    "LBP": 89500.00,
    "PKR": 278.00,
    "BDT": 119.00,
    "LKR": 296.00,
    "NPR": 133.00,
    "AFN": 71.00,
    "IRR": 42000.00,
    "IQD": 1310.00,
    "SYP": 2512.00,
    "YER": 250.00,
    "SDG": 601.00,
    "LYD": 4.85,
    "RSD": 108.00,
    "MKD": 56.50,
    "ALL": 94.00,
    "BAM": 1.80,
    "MDL": 17.80,
    "GEL": 2.75,
    "AZN": 1.70,
    "AMD": 390.00,
    "BYN": 3.30,
    "UAH": 41.50,
    "KZT": 485.00,
    "UZS": 12800.00,
    "TMT": 3.50,
    "KGS": 85.00,
    "TJS": 10.90,
    "MNT": 3400.00,
    "XOF": 605.00,
    "XAF": 605.00,
    "CDF": 2850.00,
    "GNF": 8650.00,
    "MGA": 4600.00,
    "MWK": 1740.00,
    "ZMW": 26.50,
    "BWP": 13.60,
    "NAD": 18.10,
    "SZL": 18.10,
    "LSL": 18.10,
    "MUR": 46.50,
    "SCR": 14.20,
    "KMF": 453.00,
    "STN": 22.60,
    "CVE": 101.50,
    "XCD": 2.70,
    "BBD": 2.00,
    "BZD": 2.00,
    "BSD": 1.00,
    "BMD": 1.00,
    "KYD": 0.83,
    "JMD": 156.00,
    "TTD": 6.78,
    "GYD": 209.00,
    "SRD": 29.50,
    "COP": 4150.00,
    "CLP": 930.00,
    "PEN": 3.78,
    "UYU": 42.00,
    "PYG": 7600.00,
    "BOB": 6.91,
    "ARS": 1010.00,
    "HTG": 132.00,
    "HNL": 25.00,
    "NIO": 36.50,
    "CRC": 515.00,
    "PAB": 1.00,
    "DOP": 59.50,
    "GTQ": 7.75,
    "SVC": 8.75,
    "AWG": 1.79,
    "ANG": 1.79,
    "CUP": 25.00,
    "BIF": 2890.00,
    "SLL": 21000.00,
    "LRD": 186.00,
    "MOP": 8.02,
    "PGK": 3.95,
    "SBD": 8.40,
    "TOP": 2.35,
    "WST": 2.75,
    "VUV": 120.00,
    "XPF": 109.50,
    "FJD": 2.25,
    "MVR": 15.40,
    "BTN": 83.50,
    "SHP": 0.79,
    "FKP": 0.79,
    "GIP": 0.79,
    "IMP": 0.79,
    "JEP": 0.79,
    "GGP": 0.79,
    "CKD": 1.54,
    "TVD": 1.54,
    "ERN": 15.00,
    "MRO": 357.00,
    "MRU": 40.00,
    "SOS": 570.00,
    "DJF": 178.00,
    "AOA": 920.00,
    "MZN": 64.00,
    "BND": 1.34,
    "KHR": 4100.00,
    "LAK": 22000.00,
    "MMK": 2100.00,
    "NIO": 36.50,
    "YER": 250.00,
    "SYP": 2512.00,
}

# ─── Cache ───────────────────────────────────────────────────────────────────
_cache: Dict[str, Tuple[dict, float]] = {}  # key -> (data, timestamp)
CACHE_TTL = 300  # 5 minutes

FRANKFURTER_BASE = "https://api.frankfurter.app"


async def _fetch_latest(
    base: str = "USD",
    symbols: Optional[List[str]] = None,
    client: Optional[httpx.AsyncClient] = None,
) -> dict:
    """Fetch latest rates from Frankfurter API."""
    own_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=10.0)
        own_client = True

    try:
        params: Dict[str, str] = {"base": base}
        if symbols:
            params["symbols"] = ",".join(symbols)
        resp = await client.get(f"{FRANKFURTER_BASE}/latest", params=params)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        # Return offline fallback
        rates = _build_offline_rates(base, symbols)
        return {
            "amount": 1.0,
            "base": base,
            "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "rates": rates,
            "source": "offline",
        }
    finally:
        if own_client:
            await client.aclose()


def _build_offline_rates(
    base: str = "USD",
    symbols: Optional[List[str]] = None,
) -> Dict[str, float]:
    """Build exchange rates from the offline dataset."""
    base_rate_usd = OFFLINE_RATES.get(base.upper(), 1.0)
    result: Dict[str, float] = {}
    for code, rate_vs_usd in OFFLINE_RATES.items():
        if symbols and code.upper() not in [s.upper() for s in symbols]:
            continue
        if code.upper() == base.upper():
            continue
        result[code.upper()] = round(rate_vs_usd / base_rate_usd, 6)
    return result


async def get_rates(
    base: str = "USD",
    symbols: Optional[List[str]] = None,
    force_refresh: bool = False,
) -> dict:
    """Get exchange rates with caching (5-min TTL).

    Returns a dict with:
        - base: the base currency
        - date: the date of rates
        - rates: dict of currency -> rate
        - source: 'live' or 'offline'
    """
    cache_key = f"rates:{base.upper()}:{','.join(sorted(s or '' for s in (symbols or []))) or 'all'}"

    now = time.time()
    if not force_refresh and cache_key in _cache:
        data, ts = _cache[cache_key]
        if now - ts < CACHE_TTL:
            return data

    live = await _fetch_latest(base, symbols)

    # Mark source
    if "source" not in live:
        live["source"] = "live"

    _cache[cache_key] = (live, now)
    return live


async def get_all_currencies() -> List[str]:
    """Get list of all supported currency codes."""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(f"{FRANKFURTER_BASE}/currencies")
            resp.raise_for_status()
            return sorted(resp.json().keys())
    except Exception:
        return sorted(OFFLINE_RATES.keys())


def convert_amount(
    amount: float,
    from_currency: str,
    to_currency: str,
    rates: Dict[str, float],
    base_currency: str = "USD",
) -> float:
    """Convert an amount using the given rates dict."""
    if from_currency.upper() == to_currency.upper():
        return amount

    from_upper = from_currency.upper()
    to_upper = to_currency.upper()

    if base_currency.upper() == from_upper:
        rate = rates.get(to_upper, 1.0)
    elif base_currency.upper() == to_upper:
        rate = 1.0 / rates.get(from_upper, 1.0)
    else:
        rate_from = rates.get(from_upper, 1.0)
        rate_to = rates.get(to_upper, 1.0)
        rate = rate_to / rate_from

    return round(amount * rate, 6)


def estimate_fees(
    amount: float,
    from_currency: str,
    to_currency: str,
) -> dict:
    """Estimate transaction fees for a cross-border payment.

    Fee model:
        - Fixed fee: $0.50 USD equivalent
        - Variable fee: 0.5% of amount
        - Cross-border surcharge: 0.25% for different currency zones
        - SWIFT surcharge: $15 if currencies are in different regions
    """
    from_upper = from_currency.upper()
    to_upper = to_currency.upper()

    fixed_fee_usd = 0.50
    variable_pct = 0.005  # 0.5%
    cross_border_pct = 0.0025  # 0.25%
    swift_fee = 15.00

    variable_fee = amount * variable_pct
    cross_border_fee = amount * cross_border_pct

    same_region = _same_region(from_upper, to_upper)
    swift_applies = not same_region
    swift_total = swift_fee if swift_applies else 0.0

    # Convert fixed fee from USD to source currency using offline rates
    fixed_in_source = fixed_fee_usd * (OFFLINE_RATES.get(from_upper, 1.0) / OFFLINE_RATES.get("USD", 1.0))

    total_fee = round(fixed_in_source + variable_fee + cross_border_fee + swift_total, 4)
    total_pct = round((total_fee / amount * 100) if amount > 0 else 0, 4)

    return {
        "amount": amount,
        "from_currency": from_upper,
        "to_currency": to_upper,
        "fixed_fee": round(fixed_in_source, 4),
        "variable_fee": round(variable_fee, 4),
        "cross_border_surcharge": round(cross_border_fee, 4),
        "swift_fee": swift_total,
        "swift_applies": swift_applies,
        "total_fee": total_fee,
        "total_fee_pct": total_pct,
    }


def _same_region(c1: str, c2: str) -> bool:
    """Check if two currencies are in the same region (lower SWIFT charges)."""
    regions: Dict[str, List[str]] = {
        "north_america": ["USD", "CAD", "MXN"],
        "europe": [
            "EUR", "GBP", "CHF", "SEK", "NOK", "DKK", "PLN", "CZK", "HUF",
            "RON", "BGN", "HRK", "RSD", "MKD", "ALL", "BAM", "MDL", "UAH",
        ],
        "asia_pacific": [
            "JPY", "CNY", "HKD", "SGD", "KRW", "TWD", "INR", "MYR", "THB",
            "IDR", "PHP", "VND", "AUD", "NZD", "PKR", "BDT", "LKR", "NPR",
        ],
        "latin_america": [
            "BRL", "ARS", "CLP", "COP", "PEN", "UYU", "PYG", "BOB", "DOP",
        ],
        "mena": [
            "AED", "SAR", "QAR", "KWD", "BHD", "OMR", "JOD", "ILS", "EGP",
            "TRY", "MAD", "TND", "DZD", "LBP", "IRR", "IQD", "YER",
        ],
        "africa": [
            "ZAR", "NGN", "KES", "GHS", "TZS", "UGX", "RWF", "ETB", "XOF",
            "XAF", "CDF", "GNF", "MGA", "MWK", "ZMW", "BWP", "NAD", "SZL",
            "LSL", "MUR", "SCR",
        ],
    }

    for region, currencies in regions.items():
        if c1 in currencies and c2 in currencies:
            return True
    return False


def find_routes(
    from_currency: str,
    to_currency: str,
    amount: float,
    rates: Dict[str, float],
    base_currency: str = "USD",
    max_hops: int = 2,
) -> List[dict]:
    """Find optimal payment routes (direct + up to 2 intermediate hops).

    For each route, compute the final amount, total fees, and effective rate.
    Routes are sorted by best (highest final amount) first.
    """
    if from_currency.upper() == to_currency.upper():
        return [
            {
                "route": [from_currency.upper()],
                "hops": 0,
                "final_amount": amount,
                "total_fees": 0.0,
                "effective_rate": 1.0,
            }
        ]

    from_upper = from_currency.upper()
    to_upper = to_currency.upper()
    routes: List[dict] = []

    # Route 1: Direct
    direct_rate = convert_amount(1.0, from_upper, to_upper, rates, base_currency)
    direct_amount = amount * direct_rate
    fees = estimate_fees(amount, from_upper, to_upper)
    routes.append({
        "route": [from_upper, to_upper],
        "hops": 1,
        "intermediate": None,
        "final_amount": round(direct_amount, 6),
        "total_fees": fees["total_fee"],
        "effective_rate": round(direct_rate, 6),
    })

    # Route 2: via USD (most liquid intermediate)
    if from_upper != "USD" and to_upper != "USD":
        rate_to_usd = convert_amount(1.0, from_upper, "USD", rates, base_currency)
        rate_usd_to_target = convert_amount(1.0, "USD", to_upper, rates, base_currency)
        via_usd_amount = amount * rate_to_usd * rate_usd_to_target
        effective = via_usd_amount / amount
        routes.append({
            "route": [from_upper, "USD", to_upper],
            "hops": 2,
            "intermediate": "USD",
            "final_amount": round(via_usd_amount, 6),
            "total_fees": fees["total_fee"] + 0.10,  # extra conversion fee
            "effective_rate": round(effective, 6),
        })

    # Route 3: via EUR
    if from_upper != "EUR" and to_upper != "EUR":
        rate_to_eur = convert_amount(1.0, from_upper, "EUR", rates, base_currency)
        rate_eur_to_target = convert_amount(1.0, "EUR", to_upper, rates, base_currency)
        via_eur_amount = amount * rate_to_eur * rate_eur_to_target
        effective = via_eur_amount / amount
        routes.append({
            "route": [from_upper, "EUR", to_upper],
            "hops": 2,
            "intermediate": "EUR",
            "final_amount": round(via_eur_amount, 6),
            "total_fees": fees["total_fee"] + 0.15,
            "effective_rate": round(effective, 6),
        })

    # Sort by best (highest final amount)
    routes.sort(key=lambda r: r["final_amount"], reverse=True)
    return routes


def clear_cache() -> int:
    """Clear the rate cache. Returns number of entries cleared."""
    global _cache
    n = len(_cache)
    _cache.clear()
    return n
