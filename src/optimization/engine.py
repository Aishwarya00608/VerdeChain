"""
verdechain/optimization/engine.py
──────────────────────────────────
Multi-objective route optimization engine.
Scores modal shifts: carbon savings (70%) vs cost impact (30%).
"""

from dataclasses import dataclass
from typing import List
import logging

logger = logging.getLogger(__name__)

CARBON_WEIGHT = 0.70
COST_WEIGHT = 0.30

# Modal shift substitution library
# Each entry: (from_mode, from_fuel) → list of alternatives
SUBSTITUTIONS = {
    ("road", "diesel"): [
        {
            "to_mode": "rail",
            "to_fuel": "electric",
            "carbon_saving_pct": 90.3,
            "cost_delta_pct": 12.0,
            "label": "Rail + EV Last Mile",
            "feasibility": "high",
        },
        {
            "to_mode": "sea",
            "to_fuel": "hfo",
            "carbon_saving_pct": 82.3,
            "cost_delta_pct": 7.5,
            "label": "Coastal Sea + EV Hub",
            "feasibility": "medium",
        },
        {
            "to_mode": "road",
            "to_fuel": "cng",
            "carbon_saving_pct": 32.3,
            "cost_delta_pct": 2.0,
            "label": "CNG Fleet Conversion",
            "feasibility": "high",
        },
        {
            "to_mode": "road",
            "to_fuel": "electric",
            "carbon_saving_pct": 86.3,
            "cost_delta_pct": 18.0,
            "label": "Full EV Last-Mile Fleet",
            "feasibility": "medium",
        },
    ],
    ("road", "cng"): [
        {
            "to_mode": "rail",
            "to_fuel": "electric",
            "carbon_saving_pct": 85.7,
            "cost_delta_pct": 10.0,
            "label": "Rail + EV Last Mile",
            "feasibility": "high",
        },
        {
            "to_mode": "road",
            "to_fuel": "electric",
            "carbon_saving_pct": 79.8,
            "cost_delta_pct": 15.0,
            "label": "Full EV Upgrade",
            "feasibility": "medium",
        },
    ],
    ("air", "kerosene"): [
        {
            "to_mode": "rail",
            "to_fuel": "electric",
            "carbon_saving_pct": 98.8,
            "cost_delta_pct": -20.0,
            "label": "Rail Express (saves cost!)",
            "feasibility": "high",
        },
        {
            "to_mode": "road",
            "to_fuel": "diesel",
            "carbon_saving_pct": 87.6,
            "cost_delta_pct": -30.0,
            "label": "Road HGV (much cheaper)",
            "feasibility": "high",
        },
        {
            "to_mode": "sea",
            "to_fuel": "hfo",
            "carbon_saving_pct": 97.8,
            "cost_delta_pct": -40.0,
            "label": "Coastal Sea (bulk cargo)",
            "feasibility": "medium",
        },
    ],
    ("sea", "hfo"): [
        {
            "to_mode": "sea",
            "to_fuel": "lng",
            "carbon_saving_pct": 25.5,
            "cost_delta_pct": 5.0,
            "label": "LNG Vessel Conversion",
            "feasibility": "medium",
        },
        {
            "to_mode": "rail",
            "to_fuel": "electric",
            "carbon_saving_pct": 45.5,
            "cost_delta_pct": 20.0,
            "label": "Intermodal Rail Route",
            "feasibility": "low",
        },
    ],
}


@dataclass
class SubstitutionResult:
    route_id: str
    from_mode: str
    from_fuel: str
    to_mode: str
    to_fuel: str
    label: str
    carbon_saving_pct: float
    cost_delta_pct: float
    composite_score: float
    feasibility: str
    recommendation: str


def score(carbon_saving_pct: float, cost_delta_pct: float) -> float:
    """
    Composite score = (carbon_saving% × 0.70) + (cost_saving% × 0.30)
    Negative cost_delta means cost reduction (bonus).
    """
    cost_contribution = 100 - cost_delta_pct  # invert: lower cost = higher score
    return round(
        (carbon_saving_pct * CARBON_WEIGHT) + (cost_contribution * COST_WEIGHT), 2
    )


def rank_substitutions(
    route_id: str, mode: str, fuel: str, threshold_pct: float = 0.0
) -> List[SubstitutionResult]:
    """
    Generate and rank all feasible modal substitutions for a given route.
    Returns list sorted by composite score (highest first).
    """
    key = (mode.lower(), fuel.lower())
    candidates = SUBSTITUTIONS.get(key, [])

    results = []
    for c in candidates:
        s = score(c["carbon_saving_pct"], c["cost_delta_pct"])
        rec = (
            f"URGENT: Apply immediately — threshold at {threshold_pct:.0f}%"
            if threshold_pct >= 90
            else (
                f"Recommended — will reduce carbon by {c['carbon_saving_pct']:.1f}%"
                if threshold_pct >= 75
                else "Advisory — consider for next route planning cycle"
            )
        )
        results.append(
            SubstitutionResult(
                route_id=route_id,
                from_mode=mode,
                from_fuel=fuel,
                to_mode=c["to_mode"],
                to_fuel=c["to_fuel"],
                label=c["label"],
                carbon_saving_pct=c["carbon_saving_pct"],
                cost_delta_pct=c["cost_delta_pct"],
                composite_score=s,
                feasibility=c["feasibility"],
                recommendation=rec,
            )
        )

    results.sort(key=lambda x: x.composite_score, reverse=True)
    logger.info(f"Ranked {len(results)} substitutions for {route_id} ({mode}/{fuel})")
    return results
