# VerdeChain Dataset Formats

All three input formats are normalized to the canonical `FreightRecord` schema
upon ingestion. This document defines every field, accepted value, and example
for each format.

---

## 1. CSV — Shipment Manifest

**File:** `data/samples/shipments_sample.csv`  
**Use case:** Bulk export from a Transport Management System (TMS), dispatch log, or fleet operator manifest.

### Required Columns

| Column | Type | Example | Notes |
|---|---|---|---|
| `voyage_id` | string | `VYG-2026-001` | Must be unique. Used for deduplication. |
| `date` | ISO date | `2026-01-03` | YYYY-MM-DD format |
| `origin` | string | `Mumbai` | City name or IATA code |
| `destination` | string | `Delhi` | City name or IATA code |
| `transport_mode` | enum | `road` | `road` / `rail` / `sea` / `air` / `intermodal` |
| `fuel_type` | enum | `diesel` | `diesel` / `cng` / `electric` / `hfo` / `lng` / `kerosene` |
| `distance_km` | float | `1400.0` | Route distance in kilometres |
| `weight_tonnes` | float | `22.5` | Cargo weight in metric tonnes |
| `cost_usd` | float | `3200.00` | Total route cost in USD |

### Optional Columns (enrich analysis)

| Column | Type | Example |
|---|---|---|
| `carrier` | string | `BlueDart Logistics` |
| `vehicle_id` | string | `MH-TRK-441` |
| `load_factor_pct` | int | `87` |
| `return_empty` | boolean | `false` |

### Column Name Tolerance
The parser normalises column names automatically:
- Strips whitespace, lowercases, replaces spaces with underscores
- Aliases accepted: `distance` → `distance_km`, `weight` → `weight_tonnes`, `cost` → `cost_usd`, `mode` → `transport_mode`, `fuel` → `fuel_type`

---

## 2. JSON — Live Telematics / API Stream

**File:** `data/samples/live_tracking_stream.json`  
**Use case:** Real-time GPS feed from IoT telematics devices, API response from fleet management platform, or webhook payload.

### Top-level Structure

```json
{
  "stream_id": "VERDE-LIVE-2026-Q1",
  "generated_at": "2026-01-17T09:42:00Z",
  "schema_version": "2.1",
  "source": "telematics_gateway",
  "shipments": [ ... ]
}
```

The parser also accepts a flat array: `[ { shipment }, { shipment } ]`  
Or wrapped objects: `{ "data": [...] }` or `{ "records": [...] }`

### Shipment Object Fields

| Field | Type | Example | Notes |
|---|---|---|---|
| `id` | string | `TLM-20260117-001` | Unique record ID |
| `voyage_ref` | string | `VYG-2026-001` | Cross-reference to manifest |
| `from` | string | `Mumbai` | Origin (also accepts `origin`) |
| `to` | string | `Delhi` | Destination (also accepts `destination`) |
| `mode` | enum | `road` | Transport mode |
| `fuel` | enum | `diesel` | Fuel type |
| `distance_km` | float | `1400` | Route distance |
| `cargo_weight_t` | float | `22.5` | Weight in tonnes |
| `cost` | float | `3200` | Cost in USD |
| `date` | string | `2026-01-17` | ISO date |
| `co2_emitted_kg` | float | `554.3` | Pre-computed if available |

### Optional Enrichment Fields

```json
{
  "current_position": { "lat": 22.3, "lng": 74.1 },
  "speed_kmh": 72,
  "engine_temp_c": 94,
  "fuel_consumed_litres": 210.4,
  "load_factor_pct": 87,
  "eta_iso": "2026-01-18T14:00:00Z",
  "alerts": [
    {
      "type": "carbon_warning",
      "message": "CO2e at 78% of monthly budget",
      "severity": "warning",
      "triggered_at": "2026-01-17T07:15:00Z"
    }
  ],
  "tags": ["priority", "pharmaceutical"]
}
```

---

## 3. PDF — Freight Invoice (OCR Extraction)

**File:** `data/samples/invoice_sample_BDL2026441.txt` (plaintext representation)  
**Use case:** Scanned freight invoices, carrier receipts, customs declarations.

### How it Works

The PDF parser uses `pdfplumber` for native PDFs and falls back to `pytesseract` OCR for scanned images.
A regex sweep then extracts structured fields from the unstructured text.

### Regex Extraction Rules

```python
EXTRACTION_PATTERNS = {
    "voyage_id":    r"Voyage[:\s]+([A-Z0-9\-]+)",
    "origin":       r"Origin[:\s]+([A-Za-z ,]+)",
    "destination":  r"Destination[:\s]+([A-Za-z ,]+)",
    "distance_km":  r"Distance[:\s]+([\d\.]+)\s*km",
    "weight_t":     r"Weight[:\s]+([\d\.]+)\s*t",
    "cost_usd":     r"(?:Total Cost|Cost)[:\s]+\$?([\d\.,]+)",
    "co2e":         r"CO2e[:\s]+([\d\.]+)\s*tCO2e",
}

# Mode detection: keyword scan across full text
MODE_KEYWORDS = ["road", "rail", "sea", "air", "intermodal"]
FUEL_KEYWORDS = ["diesel", "electric", "cng", "hfo", "lng", "kerosene"]
```

### Supported Invoice Layouts
- Single-page standard freight invoice
- Multi-page consolidated invoices (text from all pages is merged)
- Table-based layouts (pdfplumber table extraction)
- Scanned images at ≥150 DPI (pytesseract fallback)

---

## 4. Carbon Budget CSV

**File:** `data/samples/carbon_budgets.csv`  
**Use case:** Route-level carbon budget configuration, imported from sustainability team or regulatory filing.

| Column | Description |
|---|---|
| `route_id` | Unique route identifier |
| `budget_tco2e_monthly` | Monthly CO₂e limit in tonnes |
| `budget_tco2e_quarterly` | Quarterly limit |
| `legal_limit_tco2e_annual` | Hard legal annual cap |
| `ytd_tco2e` | Year-to-date actual emissions |
| `mtd_tco2e` | Month-to-date actual emissions |
| `threshold_pct` | Current consumption as % of monthly budget |
| `risk_level` | `low` / `warning` / `critical` / `emergency` |
| `regulatory_zone` | Zone code for compliance mapping |

---

## 5. Canonical FreightRecord Schema (Output)

All three formats normalize to this unified Python dataclass:

```python
@dataclass
class FreightRecord:
    voyage_id:        str       # Unique shipment ID
    origin:           str       # Departure city/port/airport
    destination:      str       # Arrival city/port/airport
    distance_km:      float     # Route distance (km)
    weight_tonnes:    float     # Cargo weight (metric tonnes)
    mode:             str       # road | rail | sea | air | intermodal
    fuel_type:        str       # diesel | cng | electric | hfo | lng | kerosene
    emission_factor:  float     # gCO2e per tonne-km (GLEC v3)
    co2e_tonnes:      float     # Computed: (EF × dist × weight) / 1,000,000
    cost_usd:         float     # Total route cost
    date:             str       # ISO 8601 date
    source_format:    str       # csv | json | pdf
    record_hash:      str       # SHA-256 12-char deduplication key
```

### Emission Factors (GLEC Framework v3)

| Mode | Fuel | gCO₂e / tonne-km |
|---|---|---|
| Road | Diesel | 62.0 |
| Road | CNG | 42.0 |
| Road | Electric | 8.5 |
| Rail | Diesel | 22.0 |
| Rail | Electric | 6.0 |
| Sea | HFO | 11.0 |
| Sea | LNG | 8.2 |
| Air | Kerosene | 500.0 |

---

## Quick Test

```bash
# Test CSV ingestion
python -c "
from src.ingestion.parser import FreightDataParser
p = FreightDataParser()
n = p.ingest_csv('data/samples/shipments_sample.csv')
print(f'Ingested {n} records')
print(p.summary())
"

# Test JSON ingestion
python -c "
from src.ingestion.parser import FreightDataParser
p = FreightDataParser()
n = p.ingest_json('data/samples/live_tracking_stream.json')
print(f'Ingested {n} records')
"
```
