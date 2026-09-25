"""Dynamic, SQLite-backed financial and operational metrics."""

from __future__ import annotations

import re
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any, Iterator, Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator


ART_TIMEZONE = ZoneInfo("America/Argentina/Buenos_Aires")
PROJECT_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$"
HIDDEN_REPORT_PROJECT_IDS = frozenset({"deployment-smoke-test"})


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
                    active BOOLEAN NOT NULL DEFAULT 1
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
                    name = excluded.name,
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
                ON CONFLICT(id) DO UPDATE SET name = excluded.name, active = 1
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
                    name = excluded.name,
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
                WITH active_projects AS (
                    SELECT project_id FROM financial_events
                    WHERE timestamp >= ? AND timestamp < ?
                    UNION
                    SELECT project_id FROM operational_events
                    WHERE timestamp >= ? AND timestamp < ?
                    UNION
                    SELECT project_id FROM traffic_events
                    WHERE timestamp >= ? AND timestamp < ?
                ),
                financial AS (
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
                    SELECT project_id, COUNT(*) AS event_count
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
                    COALESCE(f.revenue, 0) AS revenue,
                    COALESCE(f.cost, 0) AS cost,
                    COALESCE(f.amazon_revenue, 0) AS amazon_revenue,
                    COALESCE(t.click_count, 0) AS clicks,
                    COALESCE(o.event_count, 0) AS operational_events
                FROM active_projects a
                JOIN projects p ON p.id = a.project_id
                LEFT JOIN financial f ON f.project_id = p.id
                LEFT JOIN operational o ON o.project_id = p.id
                LEFT JOIN traffic t ON t.project_id = p.id
                ORDER BY p.name COLLATE NOCASE, p.id
                """,
                (
                    start_utc,
                    end_utc,
                    start_utc,
                    end_utc,
                    start_utc,
                    end_utc,
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
    """Create a Telegram MarkdownV2 report from a fully dynamic project list."""
    lines = [
        f"📊 *Reporte diario · {_escape_markdown(report_date.strftime('%d/%m/%Y'))}*",
        "_Zona horaria: ART_",
        "",
    ]

    visible_stats = [item for item in stats if item.project_id not in HIDDEN_REPORT_PROJECT_IDS]
    for item in visible_stats:
        lines.extend(
            [
                f"*{_escape_markdown(item.name)}* · {_escape_markdown(item.project_id)}",
                f"  Ingresos: *{_format_amount(item.revenue)}*",
                f"  Costos: *{_format_amount(item.cost)}*",
                f"  Ganancia neta: *{_format_amount(item.net)}*",
                f"  Clics: *{_escape_markdown(item.clicks)}*",
                f"  Eventos operativos: {_escape_markdown(item.operational_events)}",
                "",
            ]
        )

    total_revenue = sum(item.revenue for item in stats)
    total_cost = sum(item.cost for item in stats)
    amazon_revenue = sum(item.amazon_revenue for item in stats)
    total_clicks = sum(item.clicks for item in stats)
    lines.extend(
        [
            "*TOTAL CONSOLIDADO*",
            f"Ingresos Totales: *{_format_amount(total_revenue)}*",
            f"Amazon {_escape_markdown(affiliate_tag)}: *{_format_amount(amazon_revenue)}*",
            f"Clics afiliados: *{_escape_markdown(total_clicks)}*",
            f"Costos Totales: *{_format_amount(total_cost)}*",
            f"Ganancia Neta: *{_format_amount(total_revenue - total_cost)}*",
        ]
    )
    if not stats:
        lines.insert(3, "Sin actividad financiera o de clics registrada durante el día\\.")
    if health:
        lines.extend(["", "*ESTADO DE SERVICIOS*"])
        for service in health:
            icon = "✅" if service.healthy else "❌"
            lines.append(
                f"{icon} {_escape_markdown(service.name)}: HTTP {_escape_markdown(service.status_code)} · {_escape_markdown(service.latency_ms)} ms"
            )
    return "\n".join(lines)
