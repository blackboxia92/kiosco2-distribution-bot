"""Dynamic, SQLite-backed financial and operational metrics."""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator

ART_TIMEZONE = ZoneInfo("America/Argentina/Buenos_Aires")
PROJECT_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$"
HIDDEN_REPORT_PROJECT_IDS = frozenset({"deployment-smoke-test"})


@dataclass(frozen=True)
class ProjectDefinition:
    """Canonical portfolio entry used even when a service emitted no events."""

    project_id: str
    name: str
    kind: Literal["kiosk", "addon"]
    display_order: int
    parent_id: str = ""
    telemetry_connected: bool = False


PORTFOLIO_PROJECTS = (
    ProjectDefinition(
        "kiosco1-b2b-lead-extractor",
        "Kiosco 1 · B2B Lead Extractor",
        "kiosk",
        10,
    ),
    ProjectDefinition(
        "kiosco2-directory-submitter",
        "Kiosco 2 · LaunchScale",
        "kiosk",
        20,
    ),
    ProjectDefinition(
        "kiosco3-b2b-alert-monitor",
        "Kiosco 3 · B2B Alert Monitor",
        "kiosk",
        30,
    ),
    ProjectDefinition(
        "stacksignal-tech",
        "Kiosco 4 · StackSignal",
        "kiosk",
        40,
        telemetry_connected=True,
    ),
    ProjectDefinition(
        "kiosco2-distribution-bot",
        "Distribuidor de Kiosco 2",
        "addon",
        50,
        parent_id="kiosco2-directory-submitter",
        telemetry_connected=True,
    ),
    ProjectDefinition(
        "telegram-analytics-ops",
        "Telegram Analytics & Operations",
        "addon",
        60,
    ),
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def normalized_timestamp(value: datetime | None) -> datetime:
    if value is None:
        return utc_now()
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must include a timezone offset")
    return value.astimezone(timezone.utc)


class FinancialEventCreate(BaseModel):
    """Validated input accepted by the public event endpoint."""

    model_config = ConfigDict(str_strip_whitespace=True)

    project_id: str = Field(min_length=1, max_length=100, pattern=PROJECT_ID_PATTERN)
    project_name: str = Field(min_length=1, max_length=200)
    event_type: Literal["REVENUE", "COST"]
    amount: float = Field(gt=0, allow_inf_nan=False)
    source: str = Field(min_length=1, max_length=200)
    affiliate_tag: str = Field(default="", max_length=100)
    timestamp: datetime | None = None

    @field_validator("timestamp")
    @classmethod
    def timestamp_must_be_aware(cls, value: datetime | None) -> datetime | None:
        return None if value is None else normalized_timestamp(value)


class FinancialEventResponse(BaseModel):
    id: int
    project_id: str
    project_name: str
    event_type: Literal["REVENUE", "COST"]
    amount: float
    source: str
    affiliate_tag: str = ""
    timestamp: datetime


class TrafficEventCreate(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    project_id: str = Field(min_length=1, max_length=100, pattern=PROJECT_ID_PATTERN)
    project_name: str = Field(min_length=1, max_length=200)
    event_type: Literal["CLICK"] = "CLICK"
    source: str = Field(min_length=1, max_length=200)
    affiliate_tag: str = Field(default="", max_length=100)
    target_url: str = Field(default="", max_length=2048)
    timestamp: datetime | None = None

    @field_validator("timestamp")
    @classmethod
    def timestamp_must_be_aware(cls, value: datetime | None) -> datetime | None:
        return None if value is None else normalized_timestamp(value)


class TrafficEventResponse(TrafficEventCreate):
    id: int
    timestamp: datetime


@dataclass(frozen=True)
class ServiceHealth:
    name: str
    url: str
    status_code: int
    latency_ms: int

    @property
    def healthy(self) -> bool:
        return self.status_code == 200


@dataclass(frozen=True)
class ProjectDailyStats:
    project_id: str
    name: str
    revenue: float
    cost: float
    amazon_revenue: float
    clicks: int
    operational_events: int
    kind: str = "kiosk"
    parent_id: str = ""
    display_order: int = 999
    telemetry_connected: bool = False
    meaningful_events: int = 0
    monitor_scans: int = 0
    opportunities_notified: int = 0
    opportunities_approved: int = 0
    opportunities_discarded: int = 0

    @property
    def net(self) -> float:
        return self.revenue - self.cost


class MetricsStore:
    """Small repository that owns the dynamic metrics schema and queries."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    @contextmanager
    def _db(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def _init_schema(self) -> None:
        with self._db() as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS projects (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    active BOOLEAN NOT NULL DEFAULT 1,
                    kind TEXT NOT NULL DEFAULT 'external',
                    parent_id TEXT NOT NULL DEFAULT '',
                    display_order INTEGER NOT NULL DEFAULT 999,
                    telemetry_connected BOOLEAN NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS financial_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id TEXT NOT NULL,
                    event_type TEXT NOT NULL CHECK (event_type IN ('REVENUE', 'COST')),
                    amount REAL NOT NULL CHECK (amount > 0),
                    source TEXT NOT NULL,
                    affiliate_tag TEXT NOT NULL DEFAULT '',
                    timestamp TIMESTAMP NOT NULL,
                    FOREIGN KEY (project_id) REFERENCES projects(id)
                );

                CREATE TABLE IF NOT EXISTS operational_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    details TEXT NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    FOREIGN KEY (project_id) REFERENCES projects(id)
                );

                CREATE TABLE IF NOT EXISTS traffic_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id TEXT NOT NULL,
                    event_type TEXT NOT NULL CHECK (event_type = 'CLICK'),
                    source TEXT NOT NULL,
                    affiliate_tag TEXT NOT NULL DEFAULT '',
                    target_url TEXT NOT NULL DEFAULT '',
                    timestamp TIMESTAMP NOT NULL,
                    FOREIGN KEY (project_id) REFERENCES projects(id)
                );

                CREATE INDEX IF NOT EXISTS idx_financial_events_project_timestamp
                    ON financial_events(project_id, timestamp);
                CREATE INDEX IF NOT EXISTS idx_operational_events_project_timestamp
                    ON operational_events(project_id, timestamp);
                CREATE INDEX IF NOT EXISTS idx_traffic_events_project_timestamp
                    ON traffic_events(project_id, timestamp);
                """
            )
            columns = {
                row[1] for row in db.execute("PRAGMA table_info(financial_events)")
            }
            if "affiliate_tag" not in columns:
                db.execute(
                    "ALTER TABLE financial_events ADD COLUMN affiliate_tag TEXT NOT NULL DEFAULT ''"
                )

            project_columns = {
                row[1] for row in db.execute("PRAGMA table_info(projects)")
            }
            project_migrations = {
                "kind": "TEXT NOT NULL DEFAULT 'external'",
                "parent_id": "TEXT NOT NULL DEFAULT ''",
                "display_order": "INTEGER NOT NULL DEFAULT 999",
                "telemetry_connected": "BOOLEAN NOT NULL DEFAULT 0",
            }
            for name, definition in project_migrations.items():
                if name not in project_columns:
                    db.execute(f"ALTER TABLE projects ADD COLUMN {name} {definition}")

            created_at = utc_now().isoformat()
            for project in PORTFOLIO_PROJECTS:
                db.execute(
                    """
                    INSERT INTO projects (
                        id, name, created_at, active, kind, parent_id,
                        display_order, telemetry_connected
                    ) VALUES (?, ?, ?, 1, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        name = excluded.name,
                        active = 1,
                        kind = excluded.kind,
                        parent_id = excluded.parent_id,
                        display_order = excluded.display_order,
                        telemetry_connected = excluded.telemetry_connected
                    """,
                    (
                        project.project_id,
                        project.name,
                        created_at,
                        project.kind,
                        project.parent_id,
                        project.display_order,
                        int(project.telemetry_connected),
                    ),
                )

    def log_financial_event(
        self, event: FinancialEventCreate
    ) -> FinancialEventResponse:
        timestamp = normalized_timestamp(event.timestamp)
        timestamp_text = timestamp.isoformat()
        created_at = utc_now().isoformat()

        with self._db() as db:
            db.execute(
                """
                INSERT INTO projects (id, name, created_at, active)
                VALUES (?, ?, ?, 1)
                ON CONFLICT(id) DO UPDATE SET
                    name = CASE
                        WHEN projects.kind IN ('kiosk', 'addon') THEN projects.name
                        ELSE excluded.name
                    END,
                    active = 1
                """,
                (event.project_id, event.project_name, created_at),
            )
            cursor = db.execute(
                """
                INSERT INTO financial_events
                    (project_id, event_type, amount, source, affiliate_tag, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    event.project_id,
                    event.event_type,
                    event.amount,
                    event.source,
                    event.affiliate_tag,
                    timestamp_text,
                ),
            )
            event_id = int(cursor.lastrowid)

        return FinancialEventResponse(
            id=event_id,
            project_id=event.project_id,
            project_name=event.project_name,
            event_type=event.event_type,
            amount=event.amount,
            source=event.source,
            affiliate_tag=event.affiliate_tag,
            timestamp=timestamp,
        )

    def log_traffic_event(self, event: TrafficEventCreate) -> TrafficEventResponse:
        timestamp = normalized_timestamp(event.timestamp)
        with self._db() as db:
            db.execute(
                """
                INSERT INTO projects (id, name, created_at, active)
                VALUES (?, ?, ?, 1)
                ON CONFLICT(id) DO UPDATE SET
                    name = CASE
                        WHEN projects.kind IN ('kiosk', 'addon') THEN projects.name
                        ELSE excluded.name
                    END,
                    active = 1
                """,
                (event.project_id, event.project_name, utc_now().isoformat()),
            )
            cursor = db.execute(
                """
                INSERT INTO traffic_events
                    (project_id, event_type, source, affiliate_tag, target_url, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    event.project_id,
                    event.event_type,
                    event.source,
                    event.affiliate_tag,
                    event.target_url,
                    timestamp.isoformat(),
                ),
            )
        return TrafficEventResponse(id=int(cursor.lastrowid), **event.model_dump(exclude={"timestamp"}), timestamp=timestamp)

    def log_operational_event(
        self,
        *,
        project_id: str,
        project_name: str,
        action: str,
        details: str,
        timestamp: datetime | None = None,
    ) -> int:
        """Record internal activity; available to monitors and future integrations."""
        occurred_at = normalized_timestamp(timestamp).isoformat()
        with self._db() as db:
            db.execute(
                """
                INSERT INTO projects (id, name, created_at, active)
                VALUES (?, ?, ?, 1)
                ON CONFLICT(id) DO UPDATE SET
                    name = CASE
                        WHEN projects.kind IN ('kiosk', 'addon') THEN projects.name
                        ELSE excluded.name
                    END,
                    active = 1
                """,
                (project_id, project_name, utc_now().isoformat()),
            )
            cursor = db.execute(
                """
                INSERT INTO operational_events
                    (project_id, action, details, timestamp)
                VALUES (?, ?, ?, ?)
                """,
                (project_id, action, details, occurred_at),
            )
            return int(cursor.lastrowid)

    def daily_stats(
        self, report_date: date, tz: ZoneInfo = ART_TIMEZONE
    ) -> list[ProjectDailyStats]:
        day_start = datetime.combine(report_date, time.min, tzinfo=tz)
        next_day = datetime.combine(
            date.fromordinal(report_date.toordinal() + 1), time.min, tzinfo=tz
        )
        start_utc = day_start.astimezone(timezone.utc).isoformat()
        end_utc = next_day.astimezone(timezone.utc).isoformat()

        with self._db() as db:
            rows = db.execute(
                """
                WITH financial AS (
                    SELECT
                        project_id,
                        SUM(CASE WHEN event_type = 'REVENUE' THEN amount ELSE 0 END)
                            AS revenue,
                        SUM(CASE WHEN event_type = 'COST' THEN amount ELSE 0 END) AS cost,
                        SUM(CASE WHEN event_type = 'REVENUE' AND source = 'amazon'
                            THEN amount ELSE 0 END) AS amazon_revenue
                    FROM financial_events
                    WHERE timestamp >= ? AND timestamp < ?
                    GROUP BY project_id
                ),
                operational AS (
                    SELECT
                        project_id,
                        COUNT(*) AS event_count,
                        SUM(CASE WHEN action != 'monitor_scan' THEN 1 ELSE 0 END)
                            AS meaningful_events,
                        SUM(CASE WHEN action = 'monitor_scan' THEN 1 ELSE 0 END)
                            AS monitor_scans,
                        SUM(CASE WHEN action = 'opportunity_notified' THEN 1 ELSE 0 END)
                            AS opportunities_notified,
                        SUM(CASE WHEN action = 'opportunity_approved' THEN 1 ELSE 0 END)
                            AS opportunities_approved,
                        SUM(CASE WHEN action = 'opportunity_discarded' THEN 1 ELSE 0 END)
                            AS opportunities_discarded
                    FROM operational_events
                    WHERE timestamp >= ? AND timestamp < ?
                    GROUP BY project_id
                ),
                traffic AS (
                    SELECT project_id, COUNT(*) AS click_count
                    FROM traffic_events
                    WHERE event_type = 'CLICK' AND timestamp >= ? AND timestamp < ?
                    GROUP BY project_id
                )
                SELECT
                    p.id AS project_id,
                    p.name,
                    p.kind,
                    p.parent_id,
                    p.display_order,
                    p.telemetry_connected,
                    COALESCE(f.revenue, 0) AS revenue,
                    COALESCE(f.cost, 0) AS cost,
                    COALESCE(f.amazon_revenue, 0) AS amazon_revenue,
                    COALESCE(t.click_count, 0) AS clicks,
                    COALESCE(o.event_count, 0) AS operational_events,
                    COALESCE(o.meaningful_events, 0) AS meaningful_events,
                    COALESCE(o.monitor_scans, 0) AS monitor_scans,
                    COALESCE(o.opportunities_notified, 0) AS opportunities_notified,
                    COALESCE(o.opportunities_approved, 0) AS opportunities_approved,
                    COALESCE(o.opportunities_discarded, 0) AS opportunities_discarded
                FROM projects p
                LEFT JOIN financial f ON f.project_id = p.id
                LEFT JOIN operational o ON o.project_id = p.id
                LEFT JOIN traffic t ON t.project_id = p.id
                WHERE p.active = 1
                ORDER BY p.display_order, p.name COLLATE NOCASE, p.id
                """,
                (
                    start_utc,
                    end_utc,
                    start_utc,
                    end_utc,
                    start_utc,
                    end_utc,
                ),
            ).fetchall()

        return [
            ProjectDailyStats(
                project_id=str(row["project_id"]),
                name=str(row["name"]),
                revenue=float(row["revenue"]),
                cost=float(row["cost"]),
                amazon_revenue=float(row["amazon_revenue"]),
                clicks=int(row["clicks"]),
                operational_events=int(row["operational_events"]),
                kind=str(row["kind"]),
                parent_id=str(row["parent_id"]),
                display_order=int(row["display_order"]),
                telemetry_connected=bool(row["telemetry_connected"]),
                meaningful_events=int(row["meaningful_events"]),
                monitor_scans=int(row["monitor_scans"]),
                opportunities_notified=int(row["opportunities_notified"]),
                opportunities_approved=int(row["opportunities_approved"]),
                opportunities_discarded=int(row["opportunities_discarded"]),
            )
            for row in rows
        ]


def _escape_markdown(value: object) -> str:
    return re.sub(r"([_\*\[\]()~`>#+\-=|{}.!\\])", r"\\\1", str(value))


def _format_amount(value: float) -> str:
    return _escape_markdown(f"{value:,.2f}")


def _is_internal_test_project(item: ProjectDailyStats) -> bool:
    identity = f"{item.project_id} {item.name}".lower()
    return (
        item.project_id in HIDDEN_REPORT_PROJECT_IDS
        or "smoke" in identity
        or ("deployment" in identity and "test" in identity)
    )


def format_daily_report(
    report_date: date,
    stats: list[ProjectDailyStats],
    health: list[ServiceHealth] | None = None,
    affiliate_tag: str = "blackboxia92-21",
) -> str:
    """Create a concise executive report instead of exposing internal counters."""
    visible_stats = [item for item in stats if not _is_internal_test_project(item)]
    by_id = {item.project_id: item for item in visible_stats}

    lines = [
        f"📊 *Resumen de hoy · {_escape_markdown(report_date.strftime('%d/%m/%Y'))}*",
        "_Zona horaria: ART_",
        "",
        "*ESTADO GENERAL*",
    ]

    if health:
        healthy_count = sum(service.healthy for service in health)
        icon = "✅" if healthy_count == len(health) else "⚠️"
        lines.append(
            f"{icon} *{_escape_markdown(healthy_count)} de "
            f"{_escape_markdown(len(health))} servicios en línea*"
        )
        for service in (item for item in health if not item.healthy):
            status = (
                "sin respuesta"
                if service.status_code == 0
                else f"HTTP {service.status_code}"
            )
            lines.append(
                f"❌ {_escape_markdown(service.name)}: {_escape_markdown(status)}"
            )
    else:
        lines.append("⚪ Estado técnico no disponible")

    pending_kiosks = [
        item
        for item in visible_stats
        if item.kind == "kiosk" and not item.telemetry_connected
    ]
    stack_signal = by_id.get("stacksignal-tech")
    lines.extend(["", "*ACTIVIDAD DEL DÍA*"])
    if pending_kiosks:
        lines.append("⚠️ Todavía faltan métricas de Kioscos 1, 2 y 3")
    if stack_signal:
        lines.append(
            f"• Kiosco 4: *{_escape_markdown(stack_signal.clicks)} clics afiliados*"
        )

    distribution = by_id.get("kiosco2-distribution-bot")
    lines.extend(["", "*DISTRIBUCIÓN DE KIOSCO 2*"])
    if distribution:
        notified = distribution.opportunities_notified
        approved = distribution.opportunities_approved
        discarded = distribution.opportunities_discarded
        approval_rate = round((approved / notified) * 100) if notified else 0
        lines.extend(
            [
                f"• *{_escape_markdown(notified)}* propuestas enviadas al panel",
                (
                    f"• *{_escape_markdown(approved)}* aprobadas · "
                    f"*{_escape_markdown(discarded)}* omitidas"
                ),
                f"• Tasa de aprobación: *{_escape_markdown(approval_rate)}%*",
            ]
        )
    else:
        lines.append("⚪ Sin datos del distribuidor")

    total_revenue = sum(item.revenue for item in visible_stats)
    total_cost = sum(item.cost for item in visible_stats)
    amazon_revenue = sum(item.amazon_revenue for item in visible_stats)
    lines.extend(["", "*DINERO REGISTRADO*"])
    if total_revenue == 0 and total_cost == 0:
        lines.append("• Aún no hay ingresos ni costos registrados")
    else:
        lines.extend(
            [
                f"• Ingresos: *{_format_amount(total_revenue)}*",
                f"• Costos: *{_format_amount(total_cost)}*",
                f"• Neto: *{_format_amount(total_revenue - total_cost)}*",
            ]
        )
    if amazon_revenue:
        lines.append(
            f"• Amazon {_escape_markdown(affiliate_tag)}: "
            f"*{_format_amount(amazon_revenue)}*"
        )
    lines.append("_Solo incluye fuentes conectadas; no es la facturación total_")
    return "\n".join(lines)
