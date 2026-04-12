# VerdeChain 🌿

> **Automated ESG Logistics Intelligence Platform**  
> Eliminates manual carbon reporting. Prevents Carbon Crashes. Alerts your compliance team before legal limits are breached.

[![CI](https://github.com/your-org/verdechain/actions/workflows/ci.yml/badge.svg)](https://github.com/your-org/verdechain/actions)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-green.svg)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688.svg)](https://fastapi.tiangolo.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## What VerdeChain Does

Commercial freight logistics companies must comply with legal CO₂ emission limits per route. Manually tracking this across hundreds of routes — from CSV manifests, JSON telematics streams, and scanned PDF invoices — takes weeks of analyst work per quarter and always lags behind reality.

VerdeChain replaces this entirely with three layers of software-only automation:

| Layer | What it automates |
|---|---|
| **Ingestion Engine** | Reads CSV, JSON, PDF → normalizes to unified schema → deduplicates in &lt;5 seconds |
| **Optimization Engine** | Ranks every modal shift (carbon 70%, cost 30%) → auto-rebalances critical routes |
| **Predictive Alerting** | Linear regression + STL forecasts breach 30 days ahead → dispatches alerts to compliance team |

---

## Quick Start

```bash
git clone https://github.com/your-org/verdechain.git
cd verdechain

# Install dependencies
pip install -r requirements.txt

# Run with Docker (includes TimescaleDB + Redis)
docker-compose up

# Or run API directly
uvicorn src.api.main:app --reload

# Test ingestion with sample data
python src/ingestion/parser.py data/samples/shipments_sample.csv
```

API available at: `http://localhost:8000`  
Swagger docs: `http://localhost:8000/docs`

---

## Dataset Formats

VerdeChain accepts three formats. See [`docs/DATASET_FORMATS.md`](docs/DATASET_FORMATS.md) for full specification.

### CSV — Shipment Manifest

```csv
voyage_id,date,origin,destination,transport_mode,fuel_type,distance_km,weight_tonnes,cost_usd
VYG-2026-001,2026-01-03,Mumbai,Delhi,road,diesel,1400,22.5,3200
VYG-2026-002,2026-01-04,Mumbai,Pune,road,electric,148,8.2,540
VYG-2026-003,2026-01-05,Kolkata,Bhubaneswar,rail,electric,490,65.0,2800
```

**Required:** `voyage_id`, `origin`, `destination`, `transport_mode`, `fuel_type`, `distance_km`, `weight_tonnes`  
**Optional:** `cost_usd`, `carrier`, `vehicle_id`, `load_factor_pct`  
**Column aliases accepted:** `distance`→`distance_km`, `weight`→`weight_tonnes`, `mode`→`transport_mode`, `from`→`origin`

### JSON — Live Telematics Stream

```json
{
  "shipments": [
    {
      "id": "TLM-001",
      "from": "Mumbai", "to": "Delhi",
      "mode": "road", "fuel": "diesel",
      "distance_km": 1400,
      "cargo_weight_t": 22.5,
      "cost": 3200,
      "date": "2026-01-17",
      "co2_emitted_kg": 554.3,
      "alerts": []
    }
  ]
}
```

Also accepts: flat arrays `[{...}]`, or `{"data": [...]}`, `{"records": [...]}`.

### PDF — Freight Invoice (OCR)

The parser looks for these fields in any layout:

```
Voyage ID:      VYG-2026-001
Origin:         Mumbai
Destination:    Delhi
Distance:       1400 km
Weight:         22.5 t
Total Cost:     $3,200.00
Transport Mode: Road  (keyword scan)
Fuel Type:      Diesel (keyword scan)
```

Uses `pdfplumber` for native PDFs; falls back to `pytesseract` for scanned images.

### Emission Factors (GLEC Framework v3)

| Mode | Fuel | gCO₂e / tonne-km |
|---|---|---|
| Road | Diesel | 62.0 |
| Road | CNG | 42.0 |
| Road | Electric | **8.5** |
| Rail | Diesel | 22.0 |
| Rail | Electric | **6.0** |
| Sea | HFO | 11.0 |
| Air | Kerosene | **500.0** |

---

## Project Structure

```
verdechain/
├── src/
│   ├── ingestion/
│   │   └── parser.py          # CSV + JSON + PDF ingestion engine
│   ├── optimization/
│   │   └── engine.py          # Multi-objective modal shift optimizer
│   ├── forecasting/
│   │   └── predictor.py       # STL + linear regression 30-day forecast
│   ├── alerts/
│   │   └── router.py          # 4-level alert classification + dispatch
│   └── api/
│       └── main.py            # FastAPI REST + WebSocket
├── data/
│   └── samples/
│       ├── shipments_sample.csv          # 30-row CSV manifest
│       ├── live_tracking_stream.json     # 7-shipment telematics JSON
│       ├── carbon_budgets.csv            # Route budget configuration
│       └── invoice_sample_BDL2026441.txt # PDF invoice format guide
├── config/
│   └── verdechain.yaml        # Full configuration reference
├── tests/
│   └── test_parser.py         # 20+ unit tests (pytest)
├── docs/
│   └── DATASET_FORMATS.md     # Complete dataset format specification
├── .github/
│   └── workflows/ci.yml       # GitHub Actions CI (Python 3.11 + 3.12)
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/ingest` | Upload CSV/JSON/PDF files, get analysis |
| `GET` | `/api/optimize/{route_id}` | Get ranked modal substitutions |
| `POST` | `/api/forecast/{route_id}` | 30-day carbon breach forecast |
| `POST` | `/api/alert/check` | Classify and dispatch alerts |
| `WS` | `/ws/live` | WebSocket for live dashboard updates |
| `GET` | `/health` | System health check |

---

## Alert Protocol

| Level | Threshold | Action |
|---|---|---|
| Advisory | 60–74% | Log only, weekly digest |
| Warning | 75–89% | Email to Sustainability Manager |
| Critical | 90–99% | Email + SMS + Webhook + Auto-rebalance |
| **EMERGENCY** | **100%+** | **Broadcast all channels + Route suspended + Incident log** |

---

## Running Tests

```bash
pytest tests/ -v --cov=src
```

---

## Supported Standards

- **GLEC Framework v3** — emission factors
- **ISO 14064** — GHG quantification
- **GHG Protocol Scope 3** — supply chain emissions
- **EU ETS** — trading scheme compliance

---

## License

MIT © 2026 VerdeChain
