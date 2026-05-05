"""Crosspay Router API — Data Pipeline

Real exchange rates for ~30 major currencies, multi-route fee structures,
in-memory caching, and cross-border payment routing logic.

Exports:
    clear_cache()       → int
    convert_amount(...) → float
    estimate_fees(...)  → dict
    find_routes(...)    → list[dict]
    get_all_currencies() → list[str]
    get_rates(...)      → dict
"""

import asyncio
import math
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

# ─── Constants ─────────────────────────────────────────────────────────────────

# ECB-style rates: how many units of each currency per 1 EUR
# These are periodically refreshed to mimic a live feed.
EUR_BASE_RATES: Dict[str, float] = {
    "EUR": 1.0,
    "USD": 1.0840,
    "GBP": 0.8612,
    "JPY": 162.43,
    "CHF": 0.9398,
    "AUD": 1.6480,
    "CAD": 1.4715,
    "CNY": 7.7880,
    "INR": 89.54,
    "BRL": 5.9570,
    "MXN": 18.215,
    "SGD": 1.4535,
    "HKD": 8.4470,
    "KRW": 1439.0,
    "SEK": 11.298,
    "NOK": 11.482,
    "NZD": 1.7820,
    "ZAR": 19.821,
    "TRY": 34.215,
    "RUB": 97.021,
    "ILS": 3.9500,
    "PLN": 4.3015,
    "CZK": 24.765,
    "DKK": 7.4585,
    "HUF": 394.95,
    "MYR": 5.0510,
    "PHP": 61.478,
    "THB": 38.785,
    "IDR": 17098.0,
    "AED": 3.9695,
    "SAR": 4.0610,
}

# Currency metadata
CURRENCY_INFO: Dict[str, dict] = {
    "USD": {"name": "US Dollar", "symbol": "$", "decimals": 2, "region": "americas"},
    "EUR": {"name": "Euro", "symbol": "€", "decimals": 2, "region": "europe"},
    "GBP": {"name": "British Pound", "symbol": "£", "decimals": 2, "region": "europe"},
    "JPY": {"name": "Japanese Yen", "symbol": "¥", "decimals": 0, "region": "asia"},
    "CHF": {"name": "Swiss Franc", "symbol": "Fr", "decimals": 2, "region": "europe"},
    "AUD": {"name": "Australian Dollar", "symbol": "A$", "decimals": 2, "region": "asia"},
    "CAD": {"name": "Canadian Dollar", "symbol": "C$", "decimals": 2, "region": "americas"},
    "CNY": {"name": "Chinese Yuan", "symbol": "¥", "decimals": 2, "region": "asia"},
    "INR": {"name": "Indian Rupee", "symbol": "₹", "decimals": 2, "region": "asia"},
    "BRL": {"name": "Brazilian Real", "symbol": "R$", "decimals": 2, "region": "americas"},
    "MXN": {"name": "Mexican Peso", "symbol": "Mex$", "decimals": 2, "region": "americas"},
    "SGD": {"name": "Singapore Dollar", "symbol": "S$", "decimals": 2, "region": "asia"},
    "HKD": {"name": "Hong Kong Dollar", "symbol": "HK$", "decimals": 2, "region": "asia"},
    "KRW": {"name": "South Korean Won", "symbol": "₩", "decimals": 0, "region": "asia"},
    "SEK": {"name": "Swedish Krona", "symbol": "kr", "decimals": 2, "region": "europe"},
    "NOK": {"name": "Norwegian Krone", "symbol": "kr", "decimals": 2, "region": "europe"},
    "NZD": {"name": "New Zealand Dollar", "symbol": "NZ$", "decimals": 2, "region": "asia"},
    "ZAR": {"name": "South African Rand", "symbol": "R", "decimals": 2, "region": "africa"},
    "TRY": {"name": "Turkish Lira", "symbol": "₺", "decimals": 2, "region": "europe"},
    "RUB": {"name": "Russian Ruble", "symbol": "₽", "decimals": 2, "region": "europe"},
    "ILS": {"name": "Israeli Shekel", "symbol": "₪", "decimals": 2, "region": "asia"},
    "PLN": {"name": "Polish Zloty", "symbol": "zł", "decimals": 2, "region": "europe"},
    "CZK": {"name": "Czech Koruna", "symbol": "Kč", "decimals": 2, "region": "europe"},
    "DKK": {"name": "Danish Krone", "symbol": "kr", "decimals": 2, "region": "europe"},
    "HUF": {"name": "Hungarian Forint", "symbol": "Ft", "decimals": 0, "region": "europe"},
    "MYR": {"name": "Malaysian Ringgit", "symbol": "RM", "decimals": 2, "region": "asia"},
    "PHP": {"name": "Philippine Peso", "symbol": "₱", "decimals": 2, "region": "asia"},
    "THB": {"name": "Thai Baht", "symbol": "฿", "decimals": 2, "region": "asia"},
    "IDR": {"name": "Indonesian Rupiah", "symbol": "Rp", "decimals": 0, "region": "asia"},
    "AED": {"name": "UAE Dirham", "symbol": "د.إ", "decimals": 2, "region": "asia"},
    "SAR": {"name": "Saudi Riyal", "symbol": "﷼", "decimals": 2, "region": "asia"},
}

# ─── Payment Method Fee Structures ─────────────────────────────────────────────

# Each method: (base_fixed_fee_usd, variable_fee_pct, cross_border_pct,
#               swift_fee_usd, applies_to_regions, min_fee_usd, max_fee_pct)
#   - variable_fee_pct: percentage of transfer amount
#   - cross_border_pct: additional surcharge for cross-region transfers
#   - swift_fee_usd: flat SWIFT correspondent fee deducted separately
#   - applies_to_regions: None = all, otherwise list of region pairs where fee applies

METHOD_FEES = {
    "SWIFT": {
        "fixed_fee": 2.50,
        "variable_pct": 0.15,
        "cross_border_surcharge_pct": 0.10,
        "swift_fee": 15.00,
        "min_fee": 5.00,
        "max_fee_pct": 0.50,
        "speed": "3-5 business days",
        "description": "Traditional bank wire via SWIFT network",
    },
    "SEPA": {
        "fixed_fee": 0.50,
        "variable_pct": 0.05,
        "cross_border_surcharge_pct": 0.0,
        "swift_fee": 0.0,
        "min_fee": 0.50,
        "max_fee_pct": 0.20,
        "speed": "1-2 business days",
        "description": "SEPA transfer (EUR zone only)",
        "restricted_to": (("EUR",), ("EUR",)),
    },
    "Wise": {
        "fixed_fee": 0.75,
        "variable_pct": 0.41,
        "cross_border_surcharge_pct": 0.0,
        "swift_fee": 0.0,
        "min_fee": 0.75,
        "max_fee_pct": 0.80,
        "speed": "1-2 business days",
        "description": "Wise (mid-market rate + transparent fee)",
    },
    "PayPal": {
        "fixed_fee": 0.99,
        "variable_pct": 3.49,
        "cross_border_surcharge_pct": 0.50,
        "swift_fee": 0.0,
        "min_fee": 0.99,
        "max_fee_pct": 4.50,
        "speed": "instant-minutes",
        "description": "PayPal (convenient, higher fees)",
    },
    "Crypto": {
        "fixed_fee": 0.10,
        "variable_pct": 0.05,
        "cross_border_surcharge_pct": 0.0,
        "swift_fee": 0.0,
        "min_fee": 0.10,
        "max_fee_pct": 0.50,
        "speed": "minutes-1 hour",
        "description": "Crypto/stablecoin transfer (low fee, volatile)",
    },
}

# ─── Region Grouping (for cross-border surcharge logic) ────────────────────────

REGION_CURRENCIES = {
    "europe": {"EUR", "GBP", "CHF", "SEK", "NOK", "DKK", "PLN", "CZK", "HUF", "TRY", "RUB", "ILS"},
    "americas": {"USD", "CAD", "MXN", "BRL"},
    "asia": {"JPY", "AUD", "NZD", "CNY", "INR", "SGD", "HKD", "KRW", "MYR", "PHP", "THB", "IDR", "AED", "SAR"},
    "africa": {"ZAR"},
}

def _get_region(currency: str) -> str:
    """Return region for a currency code."""
    return CURRENCY_INFO.get(currency.upper(), {}).get("region", "other")


def _is_same_region(curr1: str, curr2: str) -> bool:
    """Check if two currencies are in the same region."""
    return _get_region(curr1) == _get_region(curr2)


def _get_region_pair(curr1: str, curr2: str) -> tuple:
    """Get sorted region pair for cross-border logic."""
    r1, r2 = _get_region(curr1), _get_region(curr2)
    return tuple(sorted([r1, r2]))


# ─── Cache ─────────────────────────────────────────────────────────────────────

class DataCache:
    """Simple TTL-based in-memory cache."""

    def __init__(self, ttl: int = 300):
        self._cache: Dict[str, tuple] = {}
        self._ttl = ttl

    def get(self, key: str):
        val, ts = self._cache.get(key, (None, 0))
        if val is not None and (time.time() - ts) < self._ttl:
            return val
        return None

    def set(self, key: str, val) -> None:
        self._cache[key] = (val, time.time())

    def clear(self) -> int:
        count = len(self._cache)
        self._cache.clear()
        return count


cache = DataCache(ttl=300)  # 5-minute TTL


# ─── Rate Generation ───────────────────────────────────────────────────────────

def _compute_rate(from_curr: str, to_curr: str, rates: dict, base: str = "USD") -> float:
    """Calculate the exchange rate from `from_curr` to `to_curr`.

    Uses cross-rate triangulation through the shared `base` currency.

    Args:
        from_curr: Source currency code.
        to_curr: Target currency code.
        rates: Dict of rates relative to `base`.
        base: The base currency for the rates dict (default USD).

    Returns:
        Float exchange rate (1 from_curr = X to_curr).
    """
    from_curr = from_curr.upper()
    to_curr = to_curr.upper()

    if from_curr == to_curr:
        return 1.0

    if from_curr == base:
        return rates.get(to_curr, 1.0)
    if to_curr == base:
        return 1.0 / rates.get(from_curr, 1.0)

    # Cross-rate: (from → base) then (base → to)
    from_to_base = rates.get(from_curr, 1.0)
    base_to_to = rates.get(to_curr, 1.0)
    return base_to_to / from_to_base


def _get_live_rates(base: str = "USD") -> Dict[str, float]:
    """Build a rates dict with `base` as the reference currency.

    Converts from internal EUR-based rates to the requested base.
    """
    base = base.upper()
    if base == "EUR":
        return dict(EUR_BASE_RATES)

    # Convert EUR base → requested base
    eur_to_base = EUR_BASE_RATES.get(base)
    if eur_to_base is None:
        raise ValueError(f"Unsupported base currency: {base}")

    rates = {}
    for code, eur_rate in EUR_BASE_RATES.items():
        rates[code] = eur_rate / eur_to_base
    rates[base] = 1.0
    return rates


def _generate_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _get_date_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


# ─── Public API ────────────────────────────────────────────────────────────────

async def get_rates(
    base: str = "USD",
    symbols: Optional[List[str]] = None,
    force_refresh: bool = False,
) -> Dict:
    """Get exchange rates for all supported currencies relative to `base`.

    Args:
        base: Base currency code (default USD).
        symbols: Optional list of target currency codes to filter.
        force_refresh: Bypass cache.

    Returns:
        Dict with keys: base, date, rates, source.
    """
    base = base.upper()
    cache_key = f"rates:{base}"

    if not force_refresh:
        cached = cache.get(cache_key)
        if cached is not None:
            rates = cached
            if symbols:
                rates = {k: v for k, v in rates.items() if k.upper() in [s.upper() for s in symbols]}
            # Always include the base
            rates[base] = 1.0
            return {
                "base": base,
                "date": _get_date_str(),
                "rates": rates,
                "source": "cache",
            }

    # Simulate a brief network call
    await asyncio.sleep(0.01)

    try:
        rates = _get_live_rates(base)
    except ValueError:
        # Fallback: use EUR as intermediary
        rates = _get_live_rates("EUR")
        eur_to_base = rates.get(base)
        if eur_to_base is None:
            raise ValueError(f"Unsupported currency: {base}")
        rates = {c: r / eur_to_base for c, r in rates.items()}
        rates[base] = 1.0

    cache.set(cache_key, rates)

    if symbols:
        symbol_set = set(s.upper() for s in symbols)
        rates = {k: v for k, v in rates.items() if k in symbol_set}
        # Always include the base
        rates[base] = 1.0

    return {
        "base": base,
        "date": _get_date_str(),
        "rates": rates,
        "source": "live",
    }


async def get_all_currencies() -> List[str]:
    """Return a sorted list of all supported currency codes.

    Returns:
        List of ISO currency codes.
    """
    return sorted(EUR_BASE_RATES.keys())


def convert_amount(
    amount: float,
    from_currency: str,
    to_currency: str,
    rates: dict,
    base_currency: str = "USD",
) -> float:
    """Convert an amount from one currency to another.

    Args:
        amount: Amount to convert.
        from_currency: Source currency code.
        to_currency: Target currency code.
        rates: Dict of rates relative to base_currency.
        base_currency: Base of the rates dict (default USD).

    Returns:
        Converted amount as float.
    """
    from_curr = from_currency.upper()
    to_curr = to_currency.upper()
    base = base_currency.upper()

    rate = _compute_rate(from_curr, to_curr, rates, base)
    return amount * rate


def estimate_fees(
    amount: float,
    from_currency: str,
    to_currency: str,
    method: Optional[str] = None,
) -> Dict:
    """Estimate fees for a cross-border payment.

    Args:
        amount: Transaction amount in source currency.
        from_currency: Source currency code.
        to_currency: Target currency code.
        method: Payment method. If None, returns SWIFT-based estimate.

    Returns:
        Dict with fee breakdown:
            amount, from_currency, to_currency,
            fixed_fee, variable_fee, cross_border_surcharge,
            swift_fee, swift_applies, total_fee, total_fee_pct
    """
    from_curr = from_currency.upper()
    to_curr = to_currency.upper()

    # Determine if cross-region
    cross_region = not _is_same_region(from_curr, to_curr)

    # Default method — case-insensitive lookup
    method_key = (method or "SWIFT")
    # Build case-insensitive mapping
    method_lookup = {k.upper(): k for k in METHOD_FEES}
    canonical_key = method_lookup.get(method_key.upper(), "SWIFT")
    if canonical_key not in METHOD_FEES:
        canonical_key = "SWIFT"

    fee_def = METHOD_FEES[canonical_key]
    method_key = canonical_key

    # Check restrictions (e.g., SEPA only works for EUR)
    restricted = fee_def.get("restricted_to")
    if restricted:
        allowed_src, allowed_dst = restricted
        if from_curr not in allowed_src or to_curr not in allowed_dst:
            # Fall back to SWIFT
            fee_def = METHOD_FEES["SWIFT"]
            method_key = "SWIFT"

    fixed_fee = fee_def["fixed_fee"]
    variable_fee = amount * (fee_def["variable_pct"] / 100.0)

    cross_border_surcharge = 0.0
    if cross_region:
        cross_border_surcharge = amount * (fee_def["cross_border_surcharge_pct"] / 100.0)

    swift_fee = 0.0
    swift_applies = False
    if method_key == "SWIFT" and cross_region:
        swift_fee = fee_def["swift_fee"]
        swift_applies = True

    # Apply min/max bounds
    min_fee = fee_def["min_fee"]
    max_fee_pct = fee_def["max_fee_pct"]

    total_fee = fixed_fee + variable_fee + cross_border_surcharge + swift_fee

    # Apply min fee
    if total_fee < min_fee:
        total_fee = min_fee

    # Apply max fee as percentage cap
    max_fee = amount * (max_fee_pct / 100.0)
    if total_fee > max_fee:
        total_fee = max_fee

    total_fee_pct = (total_fee / amount * 100.0) if amount > 0 else 0.0

    return {
        "amount": amount,
        "from_currency": from_curr,
        "to_currency": to_curr,
        "fixed_fee": round(fixed_fee, 2),
        "variable_fee": round(variable_fee, 2),
        "cross_border_surcharge": round(cross_border_surcharge, 2),
        "swift_fee": round(swift_fee, 2),
        "swift_applies": swift_applies,
        "total_fee": round(total_fee, 2),
        "total_fee_pct": round(total_fee_pct, 4),
    }


def find_routes(
    from_currency: str,
    to_currency: str,
    amount: float,
    rates: dict,
    base_currency: str = "USD",
    max_hops: int = 2,
) -> List[Dict]:
    """Find optimal payment routes between two currencies.

    Explores direct routes and multi-hop routes (via intermediate currencies),
    computes fees for each available payment method, and returns routes
    sorted by best final amount (descending).

    Args:
        from_currency: Source currency code.
        to_currency: Target currency code.
        amount: Amount in source currency.
        rates: Dict of exchange rates relative to base_currency.
        base_currency: Base of the rates dict (default USD).
        max_hops: Maximum intermediate hops (default 2, max 3).

    Returns:
        List of route dicts, each with:
            route, hops, intermediate (optional),
            final_amount, total_fees, effective_rate.
    """
    from_curr = from_currency.upper()
    to_curr = to_currency.upper()
    base = base_currency.upper()
    max_hops = min(max_hops, 3)

    # Get all available currencies (that are in the rates dict)
    currencies = [c for c in EUR_BASE_RATES.keys() if c in rates]

    route_candidates: List[Dict] = []

    # ── 1. Direct route ──
    direct_rate = _compute_rate(from_curr, to_curr, rates, base)
    direct_base_amount = amount * direct_rate
    route_candidates.append({
        "route": [from_curr, to_curr],
        "hops": 0,
        "intermediate": None,
        "final_amount": round(direct_base_amount, 6),
        "total_fees": 0.0,
        "effective_rate": round(direct_rate, 6),
    })

    # ── 2. Single-hop routes (via intermediate currency) ──
    if max_hops >= 1:
        for intermediate in currencies:
            if intermediate in (from_curr, to_curr):
                continue

            rate1 = _compute_rate(from_curr, intermediate, rates, base)
            rate2 = _compute_rate(intermediate, to_curr, rates, base)
            combined_rate = rate1 * rate2

            # Fee for first leg
            leg1_fee = _estimate_route_leg_fee(amount, from_curr, intermediate)
            amount_after_leg1 = amount - leg1_fee
            amount_in_intermediate = amount_after_leg1 * rate1

            # Fee for second leg
            leg2_fee = _estimate_route_leg_fee(amount_in_intermediate, intermediate, to_curr)
            amount_after_leg2 = amount_in_intermediate - leg2_fee
            final_amount = amount_after_leg2 * rate2

            total_fees = leg1_fee + leg2_fee

            route_candidates.append({
                "route": [from_curr, intermediate, to_curr],
                "hops": 1,
                "intermediate": intermediate,
                "final_amount": round(final_amount, 6),
                "total_fees": round(total_fees, 2),
                "effective_rate": round(final_amount / amount, 6) if amount > 0 else 0.0,
            })

    # ── 3. Two-hop routes (via two intermediate currencies) ──
    if max_hops >= 2:
        for i, mid1 in enumerate(currencies):
            if mid1 in (from_curr, to_curr):
                continue
            for mid2 in currencies:
                if mid2 in (from_curr, to_curr, mid1):
                    continue

                r1 = _compute_rate(from_curr, mid1, rates, base)
                r2 = _compute_rate(mid1, mid2, rates, base)
                r3 = _compute_rate(mid2, to_curr, rates, base)

                leg1_fee = _estimate_route_leg_fee(amount, from_curr, mid1)
                a1 = (amount - leg1_fee) * r1

                leg2_fee = _estimate_route_leg_fee(a1, mid1, mid2)
                a2 = (a1 - leg2_fee) * r2

                leg3_fee = _estimate_route_leg_fee(a2, mid2, to_curr)
                a3 = (a2 - leg3_fee) * r3

                total_fees = leg1_fee + leg2_fee + leg3_fee

                route_candidates.append({
                    "route": [from_curr, mid1, mid2, to_curr],
                    "hops": 2,
                    "intermediate": f"{mid1}/{mid2}",
                    "final_amount": round(a3, 6),
                    "total_fees": round(total_fees, 2),
                    "effective_rate": round(a3 / amount, 6) if amount > 0 else 0.0,
                })

    # ── 4. Add method-specific route variants for the direct path ──
    for method_name in METHOD_FEES:
        if method_name == "SWIFT":
            continue  # Already represented by base routing
        fee_est = estimate_fees(amount, from_curr, to_curr, method=method_name)
        total_fees_method = fee_est["total_fee"]
        method_final = direct_base_amount - total_fees_method
        if method_final > 0:
            route_candidates.append({
                "route": [from_curr, to_curr],
                "hops": 0,
                "intermediate": method_name,
                "final_amount": round(method_final, 6),
                "total_fees": round(total_fees_method, 2),
                "effective_rate": round(method_final / amount, 6) if amount > 0 else 0.0,
            })

    # Sort by final_amount descending (best route first)
    route_candidates.sort(key=lambda r: r["final_amount"], reverse=True)

    # Deduplicate: keep only top unique route paths
    seen_routes: set = set()
    unique_routes: List[Dict] = []
    for r in route_candidates:
        route_key = tuple(r["route"]) + (str(r.get("intermediate", "")),)
        if route_key not in seen_routes:
            seen_routes.add(route_key)
            unique_routes.append(r)

    # Return top 10 at most
    return unique_routes[:10]


def _estimate_route_leg_fee(amount: float, from_curr: str, to_curr: str) -> float:
    """Estimate a typical fee for one leg of a multi-hop route.

    Uses SWIFT as default method with slight discount for aggregation.
    """
    if from_curr == to_curr:
        return 0.0
    cross_region = not _is_same_region(from_curr, to_curr)
    fixed = 1.50
    variable = amount * 0.0015  # 0.15%
    surcharge = amount * 0.0010 if cross_region else 0.0
    swift = 7.50 if cross_region else 0.0
    total = fixed + variable + surcharge + swift
    return max(total, 1.50)


def clear_cache() -> int:
    """Clear the entire in-memory cache.

    Returns:
        Number of cache entries cleared.
    """
    return cache.clear()
