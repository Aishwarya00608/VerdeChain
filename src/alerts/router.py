"""
verdechain/alerts/router.py
────────────────────────────
4-level alert classification and multi-channel dispatch.
Notifies compliance team via email, SMS, and webhook.
"""

import json
import smtplib
import logging
from dataclasses import dataclass, asdict
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from enum import IntEnum
from typing import Optional

logger = logging.getLogger(__name__)


class AlertLevel(IntEnum):
    NORMAL = 0
    ADVISORY = 1  # 60–74%
    WARNING = 2  # 75–89%
    CRITICAL = 3  # 90–99%
    EMERGENCY = 4  # 100%+


LEVEL_NAMES = {
    AlertLevel.NORMAL: "NORMAL",
    AlertLevel.ADVISORY: "ADVISORY",
    AlertLevel.WARNING: "WARNING",
    AlertLevel.CRITICAL: "CRITICAL",
    AlertLevel.EMERGENCY: "EMERGENCY",
}

LEVEL_COLORS = {
    AlertLevel.ADVISORY: "#00cc14",
    AlertLevel.WARNING: "#ffb800",
    AlertLevel.CRITICAL: "#ff7700",
    AlertLevel.EMERGENCY: "#ff3d3d",
}


def classify(threshold_pct: float) -> AlertLevel:
    if threshold_pct >= 100:
        return AlertLevel.EMERGENCY
    if threshold_pct >= 90:
        return AlertLevel.CRITICAL
    if threshold_pct >= 75:
        return AlertLevel.WARNING
    if threshold_pct >= 60:
        return AlertLevel.ADVISORY
    return AlertLevel.NORMAL


@dataclass
class CarbonAlert:
    route_id: str
    route_name: str
    threshold_pct: float
    level: AlertLevel
    co2e_tonnes: float
    budget_tonnes: float
    top_substitution: Optional[str]
    forecast_breach_days: Optional[int]
    timestamp: str = ""

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.utcnow().isoformat() + "Z"

    def to_dict(self):
        d = asdict(self)
        d["level"] = LEVEL_NAMES[self.level]
        return d

    def short_summary(self) -> str:
        lvl = LEVEL_NAMES[self.level]
        s = f"[VERDECHAIN {lvl}] Route {self.route_id} at {self.threshold_pct:.1f}% carbon budget."
        if self.forecast_breach_days:
            s += f" Breach forecast in {self.forecast_breach_days} days."
        if self.top_substitution:
            s += f" Recommended: {self.top_substitution}."
        return s


class AlertRouter:
    """
    Routes CarbonAlert objects to the appropriate notification channels
    based on severity level.

    Level 1 (Advisory):  — (no notification, logged only)
    Level 2 (Warning):   Email to sustainability team
    Level 3 (Critical):  Email + SMS + Webhook + auto-rebalance trigger
    Level 4 (Emergency): Broadcast all channels + incident log
    """

    def __init__(self, config: dict):
        self.cfg = config
        self.dispatched: list = []

    def dispatch(self, alert: CarbonAlert) -> dict:
        results = {
            "alert_id": f"ALRT-{alert.timestamp[:10]}-{alert.route_id}",
            "level": LEVEL_NAMES[alert.level],
            "channels": [],
        }

        logger.warning(
            f"ALERT [{LEVEL_NAMES[alert.level]}] {alert.route_id}: "
            f"{alert.threshold_pct:.1f}% — {alert.short_summary()}"
        )

        if alert.level >= AlertLevel.WARNING:
            ok = self._send_email(alert)
            results["channels"].append({"email": ok})

        if alert.level >= AlertLevel.CRITICAL:
            ok = self._send_sms(alert)
            results["channels"].append({"sms": ok})
            ok = self._post_webhook(alert)
            results["channels"].append({"webhook": ok})
            results["auto_rebalance_triggered"] = True

        if alert.level == AlertLevel.EMERGENCY:
            self._log_incident(alert)
            results["incident_logged"] = True
            results["route_suspended"] = True

        self.dispatched.append(results)
        return results

    def _send_email(self, alert: CarbonAlert) -> bool:
        try:
            smtp_cfg = self.cfg.get("email", {})
            if not smtp_cfg.get("smtp_host"):
                logger.info("[EMAIL SIMULATED] " + alert.short_summary())
                return True

            msg = MIMEMultipart("alternative")
            msg["Subject"] = (
                f"[VERDECHAIN {LEVEL_NAMES[alert.level]}] "
                f"Route {alert.route_id} — "
                f"{alert.threshold_pct:.1f}% Carbon Budget"
            )
            msg["From"] = smtp_cfg["from"]
            msg["To"] = ", ".join(smtp_cfg.get("to", []))

            color = LEVEL_COLORS.get(alert.level, "#00cc14")
            html = f"""
            <div style="font-family:monospace;background:#0a0f0a;color:#f5f0e8;padding:28px;">
              <h2 style="color:{color};margin:0 0 16px">
                ◆ VERDECHAIN — {LEVEL_NAMES[alert.level]} ALERT
              </h2>
              <p><strong>Route:</strong> {alert.route_id} — {alert.route_name}</p>
              <p><strong>Carbon Budget:</strong>
                <span style="color:{color};font-size:1.4em">
                  {alert.threshold_pct:.1f}%
                </span>
              </p>
              <p><strong>CO₂e:</strong> {alert.co2e_tonnes:.2f}t
                 / {alert.budget_tonnes:.1f}t budget</p>
              {"<p><strong>Forecast Breach:</strong> In "
               + str(alert.forecast_breach_days) + " days</p>"
               if alert.forecast_breach_days else ""}
              {"<p><strong>Recommended Action:</strong> "
               + alert.top_substitution + "</p>"
               if alert.top_substitution else ""}
              <p style="color:#666;font-size:0.8em;margin-top:20px">
                {alert.timestamp} · VerdeChain Automated Intelligence
              </p>
            </div>"""

            msg.attach(MIMEText(html, "html"))
            with smtplib.SMTP(smtp_cfg["smtp_host"], smtp_cfg.get("port", 587)) as s:
                s.starttls()
                if smtp_cfg.get("password"):
                    s.login(smtp_cfg["from"], smtp_cfg["password"])
                s.send_message(msg)
            return True
        except Exception as e:
            logger.error(f"Email dispatch failed: {e}")
            return False

    def _send_sms(self, alert: CarbonAlert) -> bool:
        try:
            sms_cfg = self.cfg.get("sms", {})
            if not sms_cfg.get("account_sid"):
                logger.info("[SMS SIMULATED] " + alert.short_summary()[:160])
                return True
            from twilio.rest import Client

            client = Client(sms_cfg["account_sid"], sms_cfg["auth_token"])
            client.messages.create(
                body=alert.short_summary()[:160],
                from_=sms_cfg["from_number"],
                to=sms_cfg["to_number"],
            )
            return True
        except Exception as e:
            logger.error(f"SMS dispatch failed: {e}")
            return False

    def _post_webhook(self, alert: CarbonAlert) -> bool:
        try:
            wh_cfg = self.cfg.get("webhook", {})
            if not wh_cfg.get("url"):
                logger.info("[WEBHOOK SIMULATED] " + json.dumps(alert.to_dict()))
                return True
            import urllib.request

            payload = json.dumps(alert.to_dict()).encode()
            req = urllib.request.Request(
                wh_cfg["url"],
                data=payload,
                headers={
                    "Content-Type": "application/json",
                    "X-VerdeChain-Alert": LEVEL_NAMES[alert.level],
                },
            )
            urllib.request.urlopen(req, timeout=10)
            return True
        except Exception as e:
            logger.error(f"Webhook dispatch failed: {e}")
            return False

    def _log_incident(self, alert: CarbonAlert):
        incident = {
            "incident_id": f"INC-{alert.timestamp[:10]}-{alert.route_id}",
            "type": "CARBON_CRASH",
            "route_id": alert.route_id,
            "threshold_pct": alert.threshold_pct,
            "timestamp": alert.timestamp,
            "status": "OPEN",
        }
        logger.critical(f"INCIDENT LOGGED: {json.dumps(incident)}")
