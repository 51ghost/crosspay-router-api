# CrossPay Router API

> Cross-border payment routing with live FX rates — $30K MRR target

A FastAPI-based REST API for cross-border payments, currency conversion, and payment route optimization. Supports **170+ currencies** with live rates from the **European Central Bank** (via Frankfurter API), in-memory caching, and offline fallback.

## Features

- **Live FX Rates** — Real-time exchange rates from ECB, updated daily
- **Currency Conversion** — Convert any amount between 170+ currencies
- **Optimal Routing** — Find the best payment route (direct, via USD, via EUR) with full comparison
- **Fee Estimation** — Transparent fee breakdown (fixed, variable, cross-border, SWIFT)
- **Fast** — 5-minute in-memory cache, async I/O, sub-50ms responses
- **Resilient** — Built-in curated rate dataset for offline/fallback scenarios
- **RapidAPI Ready** — Standard auth headers, rate limiting, CORS enabled

## Quick Start

```bash
# Clone and install
git clone https://github.com/51ghost/crosspay-router-api.git
cd crosspay-router-api
pip install -r requirements.txt

# Run
export CROSSPAY_API_KEY="your-secret-key"
uvicorn main:app --reload --port 8000

# Test
curl http://localhost:8000/v1/health
curl http://localhost:8000/v1/rates?base=USD&symbols=EUR,GBP,JPY \
  -H "X-API-Key: your-secret-key"
```

## API Endpoints

### `GET /v1/health` — Health Check

Returns API status, version, uptime, and number of supported currencies.

### `GET /v1/rates` — Live FX Rates

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `base` | string | No | Base currency (default: USD) |
| `symbols` | string | No | Comma-separated target currencies |
| `force_refresh` | boolean | No | Bypass 5-min cache |

### `POST /v1/convert` — Currency Conversion

```json
{
  "from": "USD",
  "to": "EUR",
  "amount": 1000
}
```

### `POST /v1/routes` — Optimal Payment Routes

Finds the best path between two currencies. Returns direct, via-USD, and via-EUR routes sorted by best rate.

```json
{
  "from": "JPY",
  "to": "GBP",
  "amount": 500000
}
```

### `POST /v1/fees` — Fee Estimation

Breaks down fees by component:

| Component | Amount |
|-----------|--------|
| Fixed fee | $0.50 USD equivalent |
| Variable fee | 0.5% of amount |
| Cross-border surcharge | 0.25% |
| SWIFT fee | $15 (inter-region only) |

## Authentication

Pass your API key via one of these headers:

- `X-API-Key`
- `X-RapidAPI-Proxy-Secret`
- `X-RapidAPI-Key`

Set the key via the `CROSSPAY_API_KEY` environment variable.

Default dev key: `dev-key-1234` (change in production)

## Deployment

### Railway

```bash
railway login
railway init
railway up
```

The included `railway.json` handles build and deployment config. Set `CROSSPAY_API_KEY` as a Railway environment variable.

### Docker

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

## Technology Stack

- **Python 3.12** with **FastAPI**
- **httpx** — Async HTTP client for Frankfurter API
- **slowapi** — Rate limiting
- **Pydantic** — Request/response validation
- **Uvicorn** — ASGI server

## Data Sources

Primary: [Frankfurter API](https://api.frankfurter.app) (European Central Bank data)
Fallback: Built-in curated dataset covering 170+ currencies

## License

MIT — See LICENSE file.
