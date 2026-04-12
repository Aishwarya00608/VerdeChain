"""
verdechain/ingestion/parser.py
──────────────────────────────
Automated multi-format freight data ingestion.
Reads CSV, JSON, and PDF — normalizes all to FreightRecord.
Deduplicates using content-addressed SHA-256 hashing.
"""

import pandas as pd
import pdfplumber
import json
import hashlib
import re
import logging
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Optional, Dict, Any

logger = logging.getLogger(__name__)


# ── Canonical Schema ──────────────────────────────────────────────────────────

@dataclass
class FreightRecord:
    """
    Canonical unified schema for all freight data sources.
    Every CSV row, JSON object, and PDF invoice maps to this.
    """
    voyage_id:       str
    origin:          str
    destination:     str
    distance_km:     float
    weight_tonnes:   float
    mode:            str    # road | rail | sea | air | intermodal
    fuel_type:       str    # diesel | cng | electric | hfo | lng | kerosene
    emission_factor: float  # gCO2e per tonne-km (GLEC v3)
    co2e_tonnes:     float  # computed: (EF × dist × weight) / 1,000,000
    cost_usd:        float
    date:            str
    source_format:   str    # csv | json | pdf
    record_hash:     str    # 12-char SHA-256 dedup key

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ── Emission Factors (GLEC Framework v3) ─────────────────────────────────────

EMISSION_FACTORS: Dict[tuple, float] = {
    ("road",  "diesel"):   62.0,
    ("road",  "cng"):      42.0,
    ("road",  "electric"):  8.5,
    ("rail",  "diesel"):   22.0,
    ("rail",  "electric"):  6.0,
    ("sea",   "hfo"):      11.0,
    ("sea",   "lng"):       8.2,
    ("air",   "kerosene"): 500.0,
}
DEFAULT_EF = 62.0  # fallback: road diesel


# ── Regex Patterns for PDF Extraction ────────────────────────────────────────

PDF_PATTERNS = {
    "voyage_id":   r"Voyage(?:\s*ID)?[:\s]+([A-Z0-9\-]+)",
    "distance_km": r"Distance[:\s]+([\d\.]+)\s*km",
    "weight_t":    r"Weight[:\s]+([\d\.]+)\s*t(?:onnes?)?",
    "cost_usd":    r"(?:Total\s+Cost|Cost|Amount)[:\s]+\$?\s*([\d\.,]+)",
    "co2e":        r"CO2e?[:\s]+([\d\.]+)\s*tCO2e?",
    "origin":      r"Origin[:\s]+([A-Za-z ,\-]+?)(?:\n|,\s*[A-Z]{2})",
    "destination": r"Destination[:\s]+([A-Za-z ,\-]+?)(?:\n|,\s*[A-Z]{2})",
}

MODE_KEYWORDS  = ["intermodal", "rail", "sea", "air", "road"]
FUEL_KEYWORDS  = ["electric", "kerosene", "diesel", "cng", "hfo", "lng"]

# CSV column aliases → canonical names
COLUMN_ALIASES = {
    "distance":         "distance_km",
    "dist":             "distance_km",
    "weight":           "weight_tonnes",
    "wt":               "weight_tonnes",
    "cost":             "cost_usd",
    "amount":           "cost_usd",
    "total":            "cost_usd",
    "mode":             "transport_mode",
    "fuel":             "fuel_type",
    "transport":        "transport_mode",
    "from":             "origin",
    "to":               "destination",
    "voyage":           "voyage_id",
    "shipment_id":      "voyage_id",
    "id":               "voyage_id",
}


# ── Main Parser Class ─────────────────────────────────────────────────────────

class FreightDataParser:
    """
    Automated parser for heterogeneous freight data.

    Usage:
        parser = FreightDataParser()
        parser.ingest_csv("data/manifests/q1_shipments.csv")
        parser.ingest_json("data/streams/live_tracking.json")
        parser.ingest_pdf("data/invoices/invoice_BDL001.pdf")
        df = parser.to_dataframe()
        print(parser.summary())
    """

    def __init__(self, strict_mode: bool = False):
        self.records:       List[FreightRecord] = []
        self._seen_hashes:  set = set()
        self.rejected:      int = 0
        self.parse_errors:  int = 0
        self.strict_mode = strict_mode

    # ── Internal helpers ────────────────────────────────────────────────────

    @staticmethod
    def _compute_hash(voyage_id: str, date: str, origin: str) -> str:
        key = f"{voyage_id}|{date}|{origin}".lower().strip()
        return hashlib.sha256(key.encode()).hexdigest()[:12]

    @staticmethod
    def _compute_co2e(mode: str, fuel: str, dist_km: float, weight_t: float) -> float:
        ef = EMISSION_FACTORS.get((mode.lower(), fuel.lower()), DEFAULT_EF)
        return round((ef * dist_km * weight_t) / 1_000_000, 6)

    @staticmethod
    def _normalise_mode(raw: str) -> str:
        raw = raw.lower().strip()
        if "rail" in raw:     return "rail"
        if "sea" in raw or "ship" in raw or "vessel" in raw or "ocean" in raw: return "sea"
        if "air" in raw or "flight" in raw or "cargo" in raw: return "air"
        if "intermodal" in raw or "multi" in raw: return "intermodal"
        return "road"

    @staticmethod
    def _normalise_fuel(raw: str) -> str:
        raw = raw.lower().strip()
        if "electric" in raw or "ev" in raw or "battery" in raw: return "electric"
        if "cng" in raw or "natural gas" in raw:                  return "cng"
        if "kerosene" in raw or "jet" in raw:                     return "kerosene"
        if "hfo" in raw or "heavy fuel" in raw:                   return "hfo"
        if "lng" in raw:                                           return "lng"
        return "diesel"

    def _add_record(self, record: FreightRecord) -> bool:
        if record.record_hash not in self._seen_hashes:
            self._seen_hashes.add(record.record_hash)
            self.records.append(record)
            return True
        self.rejected += 1
        return False

    # ── CSV Ingestion ───────────────────────────────────────────────────────

    def ingest_csv(self, filepath: str) -> int:
        """
        Parse and normalize a CSV shipment manifest.
        Handles messy column names, missing fields, and type errors gracefully.

        Returns: number of new records added
        """
        before = len(self.records)
        try:
            df = pd.read_csv(filepath, dtype=str)

            # Normalize column names
            df.columns = [c.strip().lower().replace(" ", "_").replace("-", "_")
                          for c in df.columns]
            df.rename(columns=COLUMN_ALIASES, inplace=True)
            df.fillna("", inplace=True)

            for _, row in df.iterrows():
                try:
                    mode  = self._normalise_mode(row.get("transport_mode", "road"))
                    fuel  = self._normalise_fuel(row.get("fuel_type", "diesel"))
                    dist  = float(row.get("distance_km", 0) or 0)
                    wt    = float(row.get("weight_tonnes", 0) or 0)
                    vid   = str(row.get("voyage_id", "")).strip() or "CSV-UNKNOWN"
                    date  = str(row.get("date", "")).strip()
                    orig  = str(row.get("origin", "")).strip()
                    dest  = str(row.get("destination", "")).strip()
                    cost  = float(str(row.get("cost_usd", 0)).replace(",", "") or 0)

                    rec = FreightRecord(
                        voyage_id       = vid,
                        origin          = orig,
                        destination     = dest,
                        distance_km     = dist,
                        weight_tonnes   = wt,
                        mode            = mode,
                        fuel_type       = fuel,
                        emission_factor = EMISSION_FACTORS.get((mode, fuel), DEFAULT_EF),
                        co2e_tonnes     = self._compute_co2e(mode, fuel, dist, wt),
                        cost_usd        = cost,
                        date            = date,
                        source_format   = "csv",
                        record_hash     = self._compute_hash(vid, date, orig),
                    )
                    self._add_record(rec)
                except (ValueError, TypeError) as e:
                    self.parse_errors += 1
                    if self.strict_mode:
                        raise
                    logger.warning(f"CSV row parse error: {e}")

        except Exception as e:
            logger.error(f"CSV ingestion failed for {filepath}: {e}")
            if self.strict_mode:
                raise

        added = len(self.records) - before
        logger.info(f"CSV: +{added} records from {filepath} (rejected: {self.rejected})")
        return added

    # ── JSON Ingestion ──────────────────────────────────────────────────────

    def ingest_json(self, filepath: str) -> int:
        """
        Parse a JSON telematics stream or API response.
        Accepts: flat array, {shipments: []}, {data: []}, {records: []}

        Returns: number of new records added
        """
        before = len(self.records)
        try:
            with open(filepath, encoding="utf-8") as f:
                data = json.load(f)

            # Unwrap container formats
            if isinstance(data, list):
                shipments = data
            elif isinstance(data, dict):
                shipments = (data.get("shipments")
                             or data.get("data")
                             or data.get("records")
                             or [data])
            else:
                shipments = []

            for s in shipments:
                if not isinstance(s, dict):
                    continue
                try:
                    mode = self._normalise_mode(
                        s.get("mode") or s.get("transport_mode") or "road")
                    fuel = self._normalise_fuel(
                        s.get("fuel") or s.get("fuel_type") or "diesel")
                    dist = float(s.get("distance_km") or s.get("distance") or 0)
                    wt   = float(s.get("cargo_weight_t")
                                 or s.get("weight_tonnes")
                                 or s.get("weight") or 0)
                    vid  = (str(s.get("id") or s.get("voyage_id")
                                or s.get("voyage_ref") or "JSON-UNKNOWN"))
                    date = str(s.get("date") or "")
                    orig = str(s.get("from") or s.get("origin") or "")
                    dest = str(s.get("to")   or s.get("destination") or "")
                    cost = float(s.get("cost") or s.get("cost_usd") or 0)

                    rec = FreightRecord(
                        voyage_id       = vid,
                        origin          = orig,
                        destination     = dest,
                        distance_km     = dist,
                        weight_tonnes   = wt,
                        mode            = mode,
                        fuel_type       = fuel,
                        emission_factor = EMISSION_FACTORS.get((mode, fuel), DEFAULT_EF),
                        co2e_tonnes     = self._compute_co2e(mode, fuel, dist, wt),
                        cost_usd        = cost,
                        date            = date,
                        source_format   = "json",
                        record_hash     = self._compute_hash(vid, date, orig),
                    )
                    self._add_record(rec)
                except (ValueError, TypeError) as e:
                    self.parse_errors += 1
                    logger.warning(f"JSON record parse error: {e}")

        except Exception as e:
            logger.error(f"JSON ingestion failed for {filepath}: {e}")
            if self.strict_mode:
                raise

        added = len(self.records) - before
        logger.info(f"JSON: +{added} records from {filepath}")
        return added

    # ── PDF Ingestion ───────────────────────────────────────────────────────

    def ingest_pdf(self, filepath: str) -> int:
        """
        OCR-extract freight invoice data from PDF files.
        Uses pdfplumber for native PDFs; falls back to pytesseract for scans.

        Returns: number of new records added (usually 1 per invoice)
        """
        before = len(self.records)
        try:
            text = ""
            with pdfplumber.open(filepath) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text() or ""
                    text += page_text + "\n"

            # Fallback OCR for scanned PDFs
            if not text.strip():
                try:
                    import pytesseract
                    from pdf2image import convert_from_path
                    images = convert_from_path(filepath, dpi=200)
                    for img in images:
                        text += pytesseract.image_to_string(img) + "\n"
                except ImportError:
                    logger.warning("pytesseract not available; PDF may be unreadable")

            # Extract structured fields via regex
            def extract(pattern: str) -> Optional[str]:
                m = re.search(pattern, text, re.IGNORECASE)
                return m.group(1).strip() if m else None

            vid    = extract(PDF_PATTERNS["voyage_id"])   or f"PDF-{Path(filepath).stem}"
            dist   = float(extract(PDF_PATTERNS["distance_km"]) or 0)
            wt     = float(extract(PDF_PATTERNS["weight_t"])    or 0)
            cost   = float((extract(PDF_PATTERNS["cost_usd"]) or "0").replace(",", ""))
            orig   = (extract(PDF_PATTERNS["origin"])      or "PDF-EXTRACT").strip()
            dest   = (extract(PDF_PATTERNS["destination"]) or "PDF-EXTRACT").strip()

            # Mode and fuel via keyword scan
            text_lower = text.lower()
            mode = next((m for m in MODE_KEYWORDS if m in text_lower), "road")
            fuel = next((f for f in FUEL_KEYWORDS if f in text_lower), "diesel")

            rec = FreightRecord(
                voyage_id       = vid,
                origin          = orig,
                destination     = dest,
                distance_km     = dist,
                weight_tonnes   = wt,
                mode            = mode,
                fuel_type       = fuel,
                emission_factor = EMISSION_FACTORS.get((mode, fuel), DEFAULT_EF),
                co2e_tonnes     = self._compute_co2e(mode, fuel, dist, wt),
                cost_usd        = cost,
                date            = "",
                source_format   = "pdf",
                record_hash     = self._compute_hash(vid, "pdf", filepath),
            )
            self._add_record(rec)

        except Exception as e:
            logger.error(f"PDF ingestion failed for {filepath}: {e}")
            if self.strict_mode:
                raise

        added = len(self.records) - before
        logger.info(f"PDF: +{added} record from {filepath}")
        return added

    # ── Auto-detect and ingest any supported file ───────────────────────────

    def ingest_file(self, filepath: str) -> int:
        """Auto-detect format and ingest. Raises ValueError for unsupported types."""
        ext = Path(filepath).suffix.lower()
        if ext == ".csv":
            return self.ingest_csv(filepath)
        elif ext == ".json":
            return self.ingest_json(filepath)
        elif ext == ".pdf":
            return self.ingest_pdf(filepath)
        else:
            raise ValueError(f"Unsupported file format: {ext}. Use .csv, .json, or .pdf")

    def ingest_directory(self, directory: str) -> int:
        """Ingest all supported files in a directory."""
        total = 0
        for path in sorted(Path(directory).iterdir()):
            if path.suffix.lower() in {".csv", ".json", ".pdf"}:
                try:
                    total += self.ingest_file(str(path))
                except Exception as e:
                    logger.error(f"Failed to ingest {path}: {e}")
        return total

    # ── Output ─────────────────────────────────────────────────────────────

    def to_dataframe(self) -> pd.DataFrame:
        """Return all ingested records as a pandas DataFrame."""
        if not self.records:
            return pd.DataFrame()
        return pd.DataFrame([r.to_dict() for r in self.records])

    def summary(self) -> dict:
        """Return a summary of the ingestion session."""
        df = self.to_dataframe()
        if df.empty:
            return {"total_records": 0, "duplicates_rejected": self.rejected}
        return {
            "total_records":       len(self.records),
            "duplicates_rejected": self.rejected,
            "parse_errors":        self.parse_errors,
            "total_co2e_tonnes":   round(df["co2e_tonnes"].sum(), 4),
            "total_cost_usd":      round(df["cost_usd"].sum(), 2),
            "modes":               df["mode"].value_counts().to_dict(),
            "sources":             df["source_format"].value_counts().to_dict(),
            "date_range": {
                "min": df["date"].min(),
                "max": df["date"].max(),
            }
        }

    def clear(self):
        """Reset parser state."""
        self.records.clear()
        self._seen_hashes.clear()
        self.rejected = 0
        self.parse_errors = 0


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO,
                        format="%(levelname)s | %(name)s | %(message)s")

    parser = FreightDataParser()

    if len(sys.argv) > 1:
        for fp in sys.argv[1:]:
            parser.ingest_file(fp)
    else:
        # Demo with sample files
        parser.ingest_csv("data/samples/shipments_sample.csv")
        parser.ingest_json("data/samples/live_tracking_stream.json")

    print("\n── VerdeChain Ingestion Summary ──")
    for k, v in parser.summary().items():
        print(f"  {k}: {v}")

    df = parser.to_dataframe()
    if not df.empty:
        print(f"\nTop 5 emitting routes:")
        top = (df.groupby(["origin", "destination"])["co2e_tonnes"]
                 .sum()
                 .sort_values(ascending=False)
                 .head(5))
        print(top.to_string())
