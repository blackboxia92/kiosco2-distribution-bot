from __future__ import annotations

import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import date, datetime, timezone
from pathlib import Path

from metrics import FinancialEventCreate, MetricsStore, TrafficEventCreate, format_daily_report


class MetricsStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.database = Path(self.tempdir.name) / "bot.db"
        self.store = MetricsStore(self.database)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_schema_contains_required_tables(self) -> None:
        with closing(sqlite3.connect(self.database)) as db:
            tables = {
                row[0]
                for row in db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        self.assertTrue(
            {"projects", "financial_events", "operational_events", "traffic_events"}.issubset(
                tables
            )
        )

    def test_registers_projects_and_aggregates_art_day(self) -> None:
        event_time = datetime(2026, 9, 25, 15, 0, tzinfo=timezone.utc)
        self.store.log_financial_event(
            FinancialEventCreate(
                project_id="alpha",
                project_name="Alpha SaaS",
                event_type="REVENUE",
                amount=100,
                source="stripe",
                timestamp=event_time,
            )
        )
        self.store.log_financial_event(
            FinancialEventCreate(
                project_id="alpha",
                project_name="Alpha SaaS",
                event_type="COST",
                amount=35.5,
                source="railway",
                timestamp=event_time,
            )
        )
        self.store.log_financial_event(
            FinancialEventCreate(
                project_id="beta",
                project_name="Beta Kiosk",
                event_type="REVENUE",
                amount=40,
                source="mercadopago",
                timestamp=event_time,
            )
        )
        self.store.log_operational_event(
            project_id="beta",
            project_name="Beta Kiosk",
            action="signup",
            details='{"plan":"pro"}',
            timestamp=event_time,
        )
        self.store.log_traffic_event(
            TrafficEventCreate(
                project_id="alpha",
                project_name="Alpha SaaS",
                source="stacksignal-tech",
                affiliate_tag="blackboxia92-21",
                target_url="https://www.amazon.es/dp/example?tag=blackboxia92-21",
                timestamp=event_time,
            )
        )
        self.store.log_financial_event(
            FinancialEventCreate(
                project_id="deployment-smoke-test",
                project_name="Deployment Smoke Test",
                event_type="REVENUE",
                amount=5,
                source="test",
                timestamp=event_time,
            )
        )

        stats = self.store.daily_stats(date(2026, 9, 25))
        by_id = {item.project_id: item for item in stats}
        self.assertEqual(set(by_id), {"alpha", "beta", "deployment-smoke-test"})
        self.assertEqual(by_id["alpha"].revenue, 100)
        self.assertEqual(by_id["alpha"].cost, 35.5)
        self.assertEqual(by_id["alpha"].net, 64.5)
        self.assertEqual(by_id["alpha"].clicks, 1)
        self.assertEqual(by_id["beta"].operational_events, 1)

        report = format_daily_report(date(2026, 9, 25), stats)
        self.assertIn("Alpha SaaS", report)
        self.assertNotIn("Deployment Smoke Test", report)
        self.assertNotIn("deployment-smoke-test", report)
        self.assertIn("Ingresos Totales: *145\\.00*", report)
        self.assertIn("Costos Totales: *35\\.50*", report)
        self.assertIn("Ganancia Neta: *109\\.50*", report)
        self.assertIn("Clics afiliados: *1*", report)

    def test_timestamp_without_timezone_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            FinancialEventCreate(
                project_id="alpha",
                project_name="Alpha",
                event_type="REVENUE",
                amount=1,
                source="test",
                timestamp=datetime(2026, 9, 25, 12, 0),
            )


if __name__ == "__main__":
    unittest.main()
