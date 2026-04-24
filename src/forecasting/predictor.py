"""
verdechain/forecasting/predictor.py
────────────────────────────────────
30-day carbon budget breach prediction.
Uses STL seasonal decomposition + linear regression.
"""

import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)


@dataclass
class ForecastResult:
    route_id: str
    current_pct: float  # current % of budget consumed
    forecast_30d: List[float]  # daily projected % values
    days_to_breach: Optional[int]
    breach_probability: float  # 0.0–1.0
    trend_slope: float  # emissions rate of change per day
    alert_level: str  # advisory | warning | critical | emergency
    summary: str


def _alert_level(pct: float) -> str:
    if pct >= 100:
        return "emergency"
    if pct >= 90:
        return "critical"
    if pct >= 75:
        return "warning"
    if pct >= 60:
        return "advisory"
    return "normal"


def forecast_budget(
    route_id: str,
    daily_emissions: List[float],
    monthly_budget_tco2e: float,
    horizon_days: int = 30,
) -> ForecastResult:
    """
    Forecast carbon budget consumption for the next `horizon_days`.

    Args:
        route_id:             Route identifier
        daily_emissions:      List of daily CO2e values (tonnes), recent-first or chronological
        monthly_budget_tco2e: Monthly carbon budget in tCO2e
        horizon_days:         Forecast window in days (default: 30)

    Returns:
        ForecastResult with projected daily budget % and breach detection
    """
    if len(daily_emissions) < 7:
        logger.warning(
            f"Route {route_id}: fewer than 7 data points — forecast less reliable"
        )

    series = pd.Series(daily_emissions, dtype=float)

    # ── STL Decomposition (remove seasonality) ─────────────────────────────
    try:
        from statsmodels.tsa.seasonal import STL

        period = min(7, len(series) // 2) if len(series) >= 4 else 2
        stl = STL(series, period=period, robust=True).fit()
        detrended = stl.trend.values
    except Exception:
        # Fallback: simple rolling mean as trend
        detrended = series.rolling(min(7, len(series)), min_periods=1).mean().values

    # ── Linear Regression on trend ─────────────────────────────────────────
    X = np.arange(len(detrended)).reshape(-1, 1)
    y = detrended

    # Use last 14 points for recency-weighted regression
    window = min(14, len(y))
    X_w = X[-window:]
    y_w = y[-window:]

    try:
        from sklearn.linear_model import LinearRegression

        model = LinearRegression()
        model.fit(X_w, y_w)
        slope = float(model.coef_[0])
        intercept = float(model.intercept_)
    except Exception:
        # Fallback: manual slope calculation
        if len(y_w) > 1:
            slope = float((y_w[-1] - y_w[0]) / (len(y_w) - 1))
        else:
            slope = 0.0
        intercept = float(y_w[-1]) if len(y_w) > 0 else 0.0

    # ── Project forward ─────────────────────────────────────────────────────
    last_idx = len(detrended) - 1
    future_X = np.arange(last_idx + 1, last_idx + horizon_days + 1)
    future_vals = slope * future_X + intercept
    future_vals = np.maximum(future_vals, 0)  # emissions can't be negative

    # Convert to cumulative budget % per day
    cumulative = np.cumsum(future_vals)
    budget_pct = (cumulative / monthly_budget_tco2e) * 100

    # Current consumption %
    current_consumed = float(series.sum())
    current_pct = round((current_consumed / monthly_budget_tco2e) * 100, 1)

    # Days until breach
    breach_day = next((i for i, v in enumerate(budget_pct) if v >= 100), None)

    # Breach probability based on slope and current level
    if current_pct >= 100:
        breach_prob = 1.0
    elif breach_day is not None:
        breach_prob = round(
            min(1.0, 0.5 + (100 - current_pct) / 200 * (slope / 0.01 + 1)), 2
        )
    else:
        breach_prob = round(max(0.0, current_pct / 100 * slope * 0.1), 2)

    projected_end = (
        current_pct + float(budget_pct[-1]) if len(budget_pct) > 0 else current_pct
    )
    level = _alert_level(projected_end if breach_day else current_pct)

    summary = (
        f"BREACH IN {breach_day} DAYS — "
        f"projected to hit {projected_end:.1f}% of monthly budget."
        if breach_day
        else f"On track — projected {projected_end:.1f}% of monthly budget at 30 days."
    )

    return ForecastResult(
        route_id=route_id,
        current_pct=current_pct,
        forecast_30d=[round(float(v), 2) for v in budget_pct],
        days_to_breach=breach_day,
        breach_probability=breach_prob,
        trend_slope=round(slope, 6),
        alert_level=level,
        summary=summary,
    )
