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


def format_daily_report(
    report_date: date,
    stats: list[ProjectDailyStats],
    health: list[ServiceHealth] | None = None,
    affiliate_tag: str = "blackboxia92-21",
) -> str:
    """Create an ecosystem report: four kiosks first, then the two addons."""
    lines = [
        f"📊 *Reporte diario · {_escape_markdown(report_date.strftime('%d/%m/%Y'))}*",
        "_Zona horaria: ART_",
        "_Ecosistema: 4 kioscos \\+ 2 addons_",
        "",
    ]

    visible_stats = [item for item in stats if item.project_id not in HIDDEN_REPORT_PROJECT_IDS]
    for kind, heading in (
        ("kiosk", "KIOSCOS"),
        ("addon", "ADDONS"),
        ("external", "OTROS PROYECTOS"),
    ):
        items = [item for item in visible_stats if item.kind == kind]
        if not items:
            continue
        lines.append(f"*{heading}*")
        for item in items:
            prefix = "↳ " if item.parent_id else ""
            lines.append(f"{prefix}*{_escape_markdown(item.name)}*")
            if item.project_id == "telegram-analytics-ops":
                lines.append("  Panel y consolidación: _operativo_")
            elif not item.telemetry_connected:
                lines.append("  Telemetría de negocio: _pendiente de integrar_")
            else:
                lines.append(
                    f"  Ingresos {_format_amount(item.revenue)} · "
                    f"Costos {_format_amount(item.cost)} · "
                    f"Clics {_escape_markdown(item.clicks)}"
                )
            if item.project_id == "kiosco2-distribution-bot":
                lines.append(
                    "  Oportunidades: "
                    f"{_escape_markdown(item.opportunities_notified)} notificadas · "
                    f"{_escape_markdown(item.opportunities_approved)} aprobadas · "
                    f"{_escape_markdown(item.opportunities_discarded)} omitidas"
                )
            lines.append("")

    total_revenue = sum(item.revenue for item in stats)
    total_cost = sum(item.cost for item in stats)
    amazon_revenue = sum(item.amazon_revenue for item in stats)
    total_clicks = sum(item.clicks for item in stats)
    lines.extend(
        [
            "*TOTAL REGISTRADO \\(FUENTES CONECTADAS\\)*",
            f"Ingresos registrados: *{_format_amount(total_revenue)}*",
            f"Amazon {_escape_markdown(affiliate_tag)}: *{_format_amount(amazon_revenue)}*",
            f"Clics afiliados: *{_escape_markdown(total_clicks)}*",
            f"Costos registrados: *{_format_amount(total_cost)}*",
            f"Neto registrado: *{_format_amount(total_revenue - total_cost)}*",
        ]
    )
    if not visible_stats:
        lines.insert(3, "Sin actividad financiera o de clics registrada durante el día\\.")
    if health:
        lines.extend(["", "*SALUD DE SERVICIOS*"])
        for service in health:
            icon = "✅" if service.healthy else "❌"
            lines.append(
                f"{icon} {_escape_markdown(service.name)}: HTTP {_escape_markdown(service.status_code)} · {_escape_markdown(service.latency_ms)} ms"
            )
    return "\n".join(lines)
