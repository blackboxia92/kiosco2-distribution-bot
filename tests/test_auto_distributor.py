from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import Mock, patch

import requests

from auto_distributor import (
    IntentAnalyzer,
    OpportunityStore,
    Settings,
    TelegramClient,
    create_app,
)
from metrics import MetricsStore
from source_item import SourceItem


class FailingSession:
    def post(self, *args, **kwargs):
        raise requests.HTTPError("simulated provider failure")


def settings(database: Path, **overrides) -> Settings:
    values = {
        "database_path": database,
        "telegram_bot_token": "test-token",
        "telegram_chat_id": "123",
        "checkout_url": "https://example.test/jobs",
        "metrics_api_key": "metrics-secret",
        "llm_provider": "rules",
        "llm_model": "deterministic-v1",
        "llm_api_key": "",
        "allow_llm_fallback": True,
        "min_score": 8,
        "max_approvals_per_day": 5,
        "hn_poll_seconds": 120,
        "ph_poll_seconds": 300,
        "port": 8080,
    }
    values.update(overrides)
    return Settings(**values)


class UnifiedRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.database = Path(self.tempdir.name) / "bot.db"

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_shared_database_contains_monitor_and_metrics_tables(self) -> None:
        OpportunityStore(self.database)
        MetricsStore(self.database)
        with closing(sqlite3.connect(self.database)) as db:
            tables = {
                row[0]
                for row in db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        self.assertTrue(
            {
                "opportunities",
                "source_state",
                "projects",
                "financial_events",
                "operational_events",
                "traffic_events",
            }.issubset(tables)
        )

    def test_source_seed_and_item_deduplication(self) -> None:
        store = OpportunityStore(self.database)
        item = SourceItem(
            source="Hacker News",
            item_id="42",
            title="Launching a SaaS directory",
            body="Looking for the first 100 users",
            url="https://news.ycombinator.com/item?id=42",
        )
        self.assertEqual(store.seed_source("Hacker News", [item]), 1)
        self.assertTrue(store.source_initialized("Hacker News"))
        self.assertTrue(store.exists(item))
        self.assertEqual(store.seed_source("Hacker News", [item]), 0)

    def test_auto_mode_prefers_groq_when_key_exists(self) -> None:
        environment = {
            "DATABASE_PATH": str(self.database),
            "LLM_PROVIDER": "auto",
            "GROQ_API_KEY": "groq-test-key",
            "GROQ_MODEL": "model-test",
            "OPENAI_API_KEY": "openai-test-key",
        }
        with patch.dict(os.environ, environment, clear=True):
            loaded = Settings.from_env()
        self.assertEqual(loaded.llm_provider, "groq")
        self.assertEqual(loaded.llm_model, "model-test")
        self.assertTrue(loaded.allow_llm_fallback)

    def test_auto_mode_falls_back_to_rules_on_groq_failure(self) -> None:
        configured = settings(
            self.database,
            llm_provider="groq",
            llm_model="model-test",
            llm_api_key="groq-test-key",
            allow_llm_fallback=True,
        )
        analyzer = IntentAnalyzer(configured, session=FailingSession())
        item = SourceItem(
            source="Product Hunt",
            item_id="ph-1",
            title="New AI SaaS",
            body="A new launch",
            url="https://example.test/product",
        )
        evaluation, evaluator = analyzer.analyze(item)
        self.assertGreaterEqual(evaluation["score"], 0)
        self.assertEqual(evaluator, "rules/deterministic-v1 (fallback)")

    def test_app_exposes_health_and_log_event_routes(self) -> None:
        app = create_app(settings(self.database))
        paths = {route.path for route in app.routes}
        self.assertIn("/", paths)
        self.assertIn("/health", paths)
        self.assertIn("/api/v1/log-event", paths)
        self.assertIn("/api/v1/log-click", paths)

        root_route = next(route for route in app.routes if route.path == "/")
        self.assertEqual(
            root_route.endpoint(),
            {
                "status": "online",
                "service": "kiosco2-distribution-bot",
                "version": "2.0.0",
            },
        )

    def test_stats_command_only_runs_for_authorized_chat(self) -> None:
        configured = settings(self.database)
        client = TelegramClient(
            configured,
            OpportunityStore(self.database),
            MetricsStore(self.database),
        )
        client.send_stats = Mock()
        client._handle_update(
            {"message": {"chat": {"id": 123}, "text": "/stats@MyBot"}}
        )
        client._handle_update(
            {"message": {"chat": {"id": 999}, "text": "/stats"}}
        )
        client.send_stats.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
