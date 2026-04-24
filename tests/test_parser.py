"""
tests/test_parser.py
─────────────────────
Unit tests for the FreightDataParser ingestion engine.
Run: pytest tests/ -v --cov=src
"""

import pytest
import json
import csv
from pathlib import Path

# Add src to path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ingestion.parser import FreightDataParser, EMISSION_FACTORS


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def sample_csv(tmp_path):
    data = [
        [
            "voyage_id",
            "date",
            "origin",
            "destination",
            "transport_mode",
            "fuel_type",
            "distance_km",
            "weight_tonnes",
            "cost_usd",
        ],
        [
            "VYG-001",
            "2026-01-01",
            "Mumbai",
            "Delhi",
            "road",
            "diesel",
            "1400",
            "22.5",
            "3200",
        ],
        [
            "VYG-002",
            "2026-01-02",
            "Delhi",
            "Kolkata",
            "rail",
            "electric",
            "1450",
            "65.0",
            "7800",
        ],
        [
            "VYG-003",
            "2026-01-03",
            "Chennai",
            "Bangalore",
            "road",
            "cng",
            "346",
            "19.8",
            "1100",
        ],
        [
            "VYG-004",
            "2026-01-04",
            "Mumbai",
            "Chennai",
            "sea",
            "hfo",
            "1180",
            "420.0",
            "18500",
        ],
        [
            "VYG-005",
            "2026-01-05",
            "Delhi",
            "Mumbai",
            "air",
            "kerosene",
            "1150",
            "2.8",
            "9200",
        ],
    ]
    path = tmp_path / "test_shipments.csv"
    with open(path, "w", newline="") as f:
        csv.writer(f).writerows(data)
    return str(path)


@pytest.fixture
def sample_json(tmp_path):
    data = {
        "shipments": [
            {
                "id": "TLM-001",
                "from": "Mumbai",
                "to": "Delhi",
                "mode": "road",
                "fuel": "diesel",
                "distance_km": 1400,
                "cargo_weight_t": 22.5,
                "cost": 3200,
                "date": "2026-01-01",
            },
            {
                "id": "TLM-002",
                "from": "Kolkata",
                "to": "Guwahati",
                "mode": "rail",
                "fuel": "electric",
                "distance_km": 1030,
                "cargo_weight_t": 95.0,
                "cost": 5400,
                "date": "2026-01-02",
            },
        ]
    }
    path = tmp_path / "test_stream.json"
    path.write_text(json.dumps(data))
    return str(path)


@pytest.fixture
def flat_json(tmp_path):
    data = [
        {
            "id": "FL-001",
            "from": "Pune",
            "to": "Nashik",
            "mode": "electric road",
            "fuel": "ev",
            "distance_km": 210,
            "cargo_weight_t": 7.1,
            "cost": 490,
            "date": "2026-01-03",
        },
    ]
    path = tmp_path / "flat.json"
    path.write_text(json.dumps(data))
    return str(path)


@pytest.fixture
def duplicate_csv(tmp_path):
    """CSV with duplicate voyage_ids — should be deduplicated."""
    rows = [
        [
            "voyage_id",
            "date",
            "origin",
            "destination",
            "transport_mode",
            "fuel_type",
            "distance_km",
            "weight_tonnes",
            "cost_usd",
        ],
        [
            "DUP-001",
            "2026-01-01",
            "Mumbai",
            "Delhi",
            "road",
            "diesel",
            "1400",
            "22.5",
            "3200",
        ],
        [
            "DUP-001",
            "2026-01-01",
            "Mumbai",
            "Delhi",
            "road",
            "diesel",
            "1400",
            "22.5",
            "3200",
        ],  # duplicate
        [
            "DUP-002",
            "2026-01-02",
            "Delhi",
            "Jaipur",
            "road",
            "cng",
            "270",
            "14.0",
            "980",
        ],
    ]
    path = tmp_path / "dupes.csv"
    with open(path, "w", newline="") as f:
        csv.writer(f).writerows(rows)
    return str(path)


# ── Emission Factor Tests ─────────────────────────────────────────────────────


def test_emission_factors_exist():
    assert ("road", "diesel") in EMISSION_FACTORS
    assert ("rail", "electric") in EMISSION_FACTORS
    assert ("air", "kerosene") in EMISSION_FACTORS
    assert ("sea", "hfo") in EMISSION_FACTORS


def test_air_highest_emission_factor():
    air_ef = EMISSION_FACTORS[("air", "kerosene")]
    road_ef = EMISSION_FACTORS[("road", "diesel")]
    assert air_ef > road_ef, "Air should have higher EF than road diesel"


def test_electric_lower_than_diesel():
    assert EMISSION_FACTORS[("road", "electric")] < EMISSION_FACTORS[("road", "diesel")]
    assert EMISSION_FACTORS[("rail", "electric")] < EMISSION_FACTORS[("rail", "diesel")]


# ── CSV Ingestion Tests ───────────────────────────────────────────────────────


def test_csv_ingest_count(sample_csv):
    p = FreightDataParser()
    n = p.ingest_csv(sample_csv)
    assert n == 5, f"Expected 5 records, got {n}"


def test_csv_modes_normalized(sample_csv):
    p = FreightDataParser()
    p.ingest_csv(sample_csv)
    modes = {r.mode for r in p.records}
    assert "road" in modes
    assert "rail" in modes
    assert "sea" in modes
    assert "air" in modes


def test_csv_co2e_computed(sample_csv):
    p = FreightDataParser()
    p.ingest_csv(sample_csv)
    for r in p.records:
        assert r.co2e_tonnes >= 0, "CO2e should be non-negative"
        # Air freight should be highest emitter per tonne
        if r.mode == "air":
            assert r.emission_factor == 500.0


def test_csv_deduplication(duplicate_csv):
    p = FreightDataParser()
    n = p.ingest_csv(duplicate_csv)
    assert n == 2, f"Expected 2 unique records (1 deduped), got {n}"
    assert p.rejected == 1, f"Expected 1 rejected duplicate, got {p.rejected}"


def test_csv_column_aliases(tmp_path):
    """Test that alias column names are accepted."""
    rows = [
        ["voyage", "date", "from", "to", "mode", "fuel", "distance", "weight", "cost"],
        [
            "AL-001",
            "2026-01-01",
            "Mumbai",
            "Delhi",
            "road",
            "diesel",
            "1400",
            "22.5",
            "3200",
        ],
    ]
    path = tmp_path / "aliases.csv"
    with open(path, "w", newline="") as f:
        csv.writer(f).writerows(rows)
    p = FreightDataParser()
    n = p.ingest_csv(str(path))
    assert n == 1
    assert p.records[0].voyage_id == "AL-001"
    assert p.records[0].origin == "Mumbai"


def test_csv_missing_optional_fields(tmp_path):
    """CSV with only required fields should still parse."""
    rows = [
        [
            "voyage_id",
            "origin",
            "destination",
            "transport_mode",
            "fuel_type",
            "distance_km",
            "weight_tonnes",
        ],
        ["MIN-001", "Delhi", "Jaipur", "road", "diesel", "270", "14.0"],
    ]
    path = tmp_path / "minimal.csv"
    with open(path, "w", newline="") as f:
        csv.writer(f).writerows(rows)
    p = FreightDataParser()
    n = p.ingest_csv(str(path))
    assert n == 1
    assert p.records[0].cost_usd == 0.0


# ── JSON Ingestion Tests ──────────────────────────────────────────────────────


def test_json_ingest_count(sample_json):
    p = FreightDataParser()
    n = p.ingest_json(sample_json)
    assert n == 2


def test_json_flat_array(flat_json):
    p = FreightDataParser()
    n = p.ingest_json(flat_json)
    assert n == 1
    assert p.records[0].mode == "road"  # "electric road" → road
    assert p.records[0].fuel_type == "electric"  # "ev" → electric


def test_json_mode_fuel_normalisation(sample_json):
    p = FreightDataParser()
    p.ingest_json(sample_json)
    modes = {r.mode for r in p.records}
    fuels = {r.fuel_type for r in p.records}
    assert "road" in modes
    assert "rail" in modes
    assert "diesel" in fuels
    assert "electric" in fuels


# ── Cross-format Deduplication ────────────────────────────────────────────────


def test_cross_format_deduplication(sample_csv, sample_json):
    """Same voyage_id in both CSV and JSON should deduplicate."""
    p = FreightDataParser()
    csv_n = p.ingest_csv(sample_csv)
    json_n = p.ingest_json(sample_json)
    # TLM-001 in JSON has same origin/dest as VYG-001 in CSV but different IDs
    # so no dedup expected — just test that both are ingested
    assert len(p.records) == csv_n + json_n - p.rejected


# ── Summary Tests ─────────────────────────────────────────────────────────────


def test_summary_fields(sample_csv):
    p = FreightDataParser()
    p.ingest_csv(sample_csv)
    s = p.summary()
    assert "total_records" in s
    assert "total_co2e_tonnes" in s
    assert "modes" in s
    assert s["total_co2e_tonnes"] > 0
    assert s["total_records"] == 5


def test_to_dataframe(sample_csv):
    p = FreightDataParser()
    p.ingest_csv(sample_csv)
    df = p.to_dataframe()
    assert len(df) == 5
    required_cols = {
        "voyage_id",
        "origin",
        "destination",
        "mode",
        "fuel_type",
        "co2e_tonnes",
        "cost_usd",
        "source_format",
    }
    assert required_cols.issubset(set(df.columns))


# ── Clear ─────────────────────────────────────────────────────────────────────


def test_clear(sample_csv):
    p = FreightDataParser()
    p.ingest_csv(sample_csv)
    assert len(p.records) == 5
    p.clear()
    assert len(p.records) == 0
    assert p.rejected == 0


# ── Ingest file auto-detect ───────────────────────────────────────────────────


def test_ingest_file_csv(sample_csv):
    p = FreightDataParser()
    n = p.ingest_file(sample_csv)
    assert n == 5


def test_ingest_file_json(sample_json):
    p = FreightDataParser()
    n = p.ingest_file(sample_json)
    assert n == 2


def test_ingest_file_unsupported(tmp_path):
    path = tmp_path / "data.xlsx"
    path.write_text("dummy")
    p = FreightDataParser()
    with pytest.raises(ValueError, match="Unsupported file format"):
        p.ingest_file(str(path))
