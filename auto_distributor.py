"""24/7 HN + Product Hunt opportunity monitor with Telegram HITL and metrics."""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import logging
import os
import re
import secrets
import signal
import sqlite3
import threading
import time
from collections.abc import Iterator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import requests
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Request, status

from hn_monitor import HackerNewsMonitor
from metrics import (
    ART_TIMEZONE,
    FinancialEventCreate,
    FinancialEventResponse,
    MetricsStore,
    ServiceHealth,
    TrafficEventCreate,
    TrafficEventResponse,
    format_daily_report,
)
from ph_monitor import ProductHuntMonitor
from source_item import SourceItem

LOGGER = logging.getLogger("kiosco2")
BOT_PROJECT_ID = "kiosco2-distribution-bot"
BOT_PROJECT_NAME = "Kiosco 2 Distribution Bot"
DEFAULT_HEALTHCHECK_URLS = (
    "https://blackboxia92.app.n8n.cloud/healthz",
    "https://kiosco2-directory-submitter-production.up.railway.app/health",
    "https://kiosco2-distribution-bot-production.up.railway.app/health",
    "https://kiosco3-b2b-alert-monitor.blackboxia92.workers.dev/health",
    "https://stacksignal-tech.netlify.app/",
)
HEALTHCHECK_NAMES = {
    "blackboxia92.app.n8n.cloud": "Kiosco 1 · n8n",
    "kiosco2-directory-submitter-production.up.railway.app": "Kiosco 2 · LaunchScale",
    "kiosco2-distribution-bot-production.up.railway.app": "↳ Distribuidor de Kiosco 2",
    "kiosco3-b2b-alert-monitor.blackboxia92.workers.dev": "Kiosco 3 · Alert Monitor",
    "stacksignal-tech.netlify.app": "Kiosco 4 · StackSignal",
}


def utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat()


def env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        return max(minimum, int(raw))
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc


def parse_healthcheck_urls(raw: str) -> tuple[str, ...]:
    """Accept comma- or whitespace-separated URLs from deployment variables."""
    urls = tuple(value for value in re.split(r"[,\s]+", raw.strip()) if value)
    if not urls:
        return DEFAULT_HEALTHCHECK_URLS
    invalid = [url for url in urls if not url.startswith(("https://", "http://"))]
    if invalid:
        raise ValueError("HEALTHCHECK_URLS must contain absolute HTTP(S) URLs")
    return urls


@dataclass(frozen=True)
class Settings:
    database_path: Path
    telegram_bot_token: str
    telegram_chat_id: str
    checkout_url: str
    metrics_api_key: str
    llm_provider: str
    llm_model: str
    llm_api_key: str
    allow_llm_fallback: bool
    min_score: int
    max_approvals_per_day: int
    hn_poll_seconds: int
    ph_poll_seconds: int
    port: int
    affiliate_tag: str = "blackboxia92-21"
    healthcheck_urls: tuple[str, ...] = DEFAULT_HEALTHCHECK_URLS
    daily_report_hour: int = 20

    @classmethod
    def from_env(cls) -> Settings:
        requested = os.getenv("LLM_PROVIDER", "auto").strip().lower()
        groq_key = os.getenv("GROQ_API_KEY", "").strip()
        openai_key = os.getenv("OPENAI_API_KEY", "").strip()
        allow_fallback = requested == "auto"

        if requested == "auto":
            if groq_key:
                provider = "groq"
                key = groq_key
                model = os.getenv("GROQ_MODEL", "openai/gpt-oss-20b").strip()
            elif openai_key:
                provider = "openai"
                key = openai_key
                model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini").strip()
            else:
                provider, key, model = "rules", "", "deterministic-v1"
        elif requested == "groq":
            provider, key = "groq", groq_key
            model = os.getenv("GROQ_MODEL", "openai/gpt-oss-20b").strip()
        elif requested == "openai":
            provider, key = "openai", openai_key
            model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini").strip()
        elif requested == "rules":
            provider, key, model = "rules", "", "deterministic-v1"
        else:
            raise ValueError("LLM_PROVIDER must be auto, groq, openai, or rules")

        if provider != "rules" and not key:
            raise ValueError(f"{provider.upper()} API key is required")

        return cls(
            database_path=Path(os.getenv("DATABASE_PATH", "/app/data/bot.db")),
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip(),
            checkout_url=os.getenv("CHECKOUT_URL", "").strip(),
            metrics_api_key=os.getenv("METRICS_API_KEY", "").strip(),
            llm_provider=provider,
            llm_model=model,
            llm_api_key=key,
            allow_llm_fallback=allow_fallback,
            min_score=env_int("MIN_SCORE", 8, 0),
            max_approvals_per_day=env_int("MAX_APPROVALS_PER_DAY", 5),
            hn_poll_seconds=env_int("HN_POLL_SECONDS", 120, 15),
            ph_poll_seconds=env_int("PH_POLL_SECONDS", 300, 30),
            port=env_int("PORT", 8080),
            affiliate_tag=os.getenv("AMAZON_ASSOCIATE_TAG", "blackboxia92-21").strip(),
            healthcheck_urls=parse_healthcheck_urls(
                os.getenv("HEALTHCHECK_URLS", ",".join(DEFAULT_HEALTHCHECK_URLS))
            ),
            daily_report_hour=env_int("DAILY_REPORT_HOUR", 20, 0),
        )

    @property
    def evaluator_label(self) -> str:
        return f"{self.llm_provider}/{self.llm_model}"


class OpportunityStore:
    """SQLite repository shared safely by both monitor threads."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA busy_timeout=30000")
        return db

    @contextmanager
    def _db(self) -> Iterator[sqlite3.Connection]:
        db = self._connect()
        try:
            yield db
            db.commit()
        finally:
            db.close()

    def _init_schema(self) -> None:
        with self._db() as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS opportunities (
                    lead_id TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    source_item_id TEXT NOT NULL,
                    author TEXT NOT NULL DEFAULT '',
                    title TEXT NOT NULL,
                    body TEXT NOT NULL DEFAULT '',
                    url TEXT NOT NULL,
                    published_at TEXT NOT NULL DEFAULT '',
                    score INTEGER NOT NULL DEFAULT 0,
                    reason TEXT NOT NULL DEFAULT '',
                    suggested_reply TEXT NOT NULL DEFAULT '',
                    evaluator TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'seen',
                    detected_at TEXT NOT NULL,
                    notified_at TEXT,
                    reviewed_at TEXT
                );

                CREATE TABLE IF NOT EXISTS source_state (
                    source TEXT PRIMARY KEY,
                    initialized_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS service_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE UNIQUE INDEX IF NOT EXISTS idx_opportunity_source_item
                    ON opportunities(source, source_item_id);
                CREATE INDEX IF NOT EXISTS idx_opportunity_status
                    ON opportunities(status, detected_at);
                """
            )
            columns = {
                str(row[1])
                for row in db.execute("PRAGMA table_info(opportunities)").fetchall()
            }
            migrations = {
                "evaluator": "TEXT NOT NULL DEFAULT ''",
                "notified_at": "TEXT",
                "reviewed_at": "TEXT",
            }
            for name, definition in migrations.items():
                if name not in columns:
                    db.execute(f"ALTER TABLE opportunities ADD COLUMN {name} {definition}")
        LOGGER.info("SQLite database initialized at %s", self.path)

    @staticmethod
    def lead_id(item: SourceItem) -> str:
        value = f"{item.source}:{item.item_id}".encode()
        return hashlib.sha256(value).hexdigest()[:24]

    def source_initialized(self, source: str) -> bool:
        with self._db() as db:
            row = db.execute(
                "SELECT 1 FROM source_state WHERE source = ?", (source,)
            ).fetchone()
        return row is not None

    def seed_source(self, source: str, items: list[SourceItem]) -> int:
        now = utc_now_text()
        inserted = 0
        with self._db() as db:
            for item in items:
                cursor = db.execute(
                    """
                    INSERT OR IGNORE INTO opportunities (
                        lead_id, source, source_item_id, author, title, body, url,
                        published_at, status, detected_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'seeded', ?)
                    """,
                    (
                        self.lead_id(item), item.source, item.item_id, item.author,
                        item.title, item.body, item.url, item.published_at, now,
                    ),
                )
                inserted += cursor.rowcount
            db.execute(
                "INSERT OR REPLACE INTO source_state(source, initialized_at) VALUES (?, ?)",
                (source, now),
            )
        return inserted

    def exists(self, item: SourceItem) -> bool:
        with self._db() as db:
            row = db.execute(
                "SELECT 1 FROM opportunities WHERE source = ? AND source_item_id = ?",
                (item.source, item.item_id),
            ).fetchone()
        return row is not None

    def save_evaluation(
        self, item: SourceItem, evaluation: dict[str, Any], evaluator: str, min_score: int
    ) -> bool:
        score = max(0, min(10, int(evaluation["score"])))
        state = "qualified" if score >= min_score else "ignored"
        with self._db() as db:
            cursor = db.execute(
                """
                INSERT OR IGNORE INTO opportunities (
                    lead_id, source, source_item_id, author, title, body, url,
                    published_at, score, reason, suggested_reply, evaluator,
                    status, detected_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self.lead_id(item), item.source, item.item_id, item.author,
                    item.title, item.body, item.url, item.published_at, score,
                    str(evaluation["reason"]), str(evaluation["suggested_reply"]),
                    evaluator, state, utc_now_text(),
                ),
            )
        return cursor.rowcount == 1

    def qualified(self, limit: int) -> list[sqlite3.Row]:
        with self._db() as db:
            return db.execute(
                """
                SELECT * FROM opportunities
                WHERE status = 'qualified'
                ORDER BY detected_at ASC LIMIT ?
                """,
                (limit,),
            ).fetchall()

    def notified_today(self) -> int:
        today = datetime.now(ART_TIMEZONE).date()
        start = datetime.combine(today, datetime.min.time(), tzinfo=ART_TIMEZONE)
        end = datetime.combine(
            date.fromordinal(today.toordinal() + 1),
            datetime.min.time(),
            tzinfo=ART_TIMEZONE,
        )
        with self._db() as db:
            row = db.execute(
                """
                SELECT COUNT(*) FROM opportunities
                WHERE notified_at >= ? AND notified_at < ?
                """,
                (
                    start.astimezone(timezone.utc).isoformat(),
                    end.astimezone(timezone.utc).isoformat(),
                ),
            ).fetchone()
        return int(row[0])

    def mark_notified(self, lead_id: str) -> None:
        with self._db() as db:
            db.execute(
                """
                UPDATE opportunities SET status = 'pending', notified_at = ?
                WHERE lead_id = ? AND status = 'qualified'
                """,
                (utc_now_text(), lead_id),
            )

    def review(self, lead_id: str, new_status: str) -> sqlite3.Row | None:
        if new_status not in {"approved", "discarded"}:
            raise ValueError("Unsupported review status")
        with self._db() as db:
            row = db.execute(
                "SELECT * FROM opportunities WHERE lead_id = ?", (lead_id,)
            ).fetchone()
            if row is None or row["status"] != "pending":
                return None
            db.execute(
                "UPDATE opportunities SET status = ?, reviewed_at = ? WHERE lead_id = ?",
                (new_status, utc_now_text(), lead_id),
            )
            return row

    def get_state(self, key: str, default: str = "") -> str:
        with self._db() as db:
            row = db.execute(
                "SELECT value FROM service_state WHERE key = ?", (key,)
            ).fetchone()
        return default if row is None else str(row[0])

    def set_state(self, key: str, value: str) -> None:
        with self._db() as db:
            db.execute(
                "INSERT OR REPLACE INTO service_state(key, value) VALUES (?, ?)",
                (key, value),
            )


class IntentAnalyzer:
    """Groq/OpenAI evaluator with deterministic fail-safe for auto mode."""

    def __init__(self, settings: Settings, session: requests.Session | None = None) -> None:
        self.settings = settings
        self.session = session or requests.Session()

    @property
    def label(self) -> str:
        return self.settings.evaluator_label

    def _rules(self, item: SourceItem) -> dict[str, Any]:
        text = f"{item.title} {item.body}".lower()
        terms = {
            "launching": 2,
            "launched": 2,
            "first users": 2,
            "first 100 users": 3,
            "backlinks": 2,
            "directory": 3,
            "show hn": 2,
            "saas": 1,
            "saas marketing": 3,
            "product hunt": 1,
        }
        score = 8 if item.source == "Product Hunt" else 6
        matches = [term for term, weight in terms.items() if term in text]
        score += sum(terms[term] for term in matches)
        score = min(10, max(0, score))
        reason = (
            "Lanzamiento reciente con necesidad probable de distribución."
            if item.source == "Product Hunt"
            else "Señales de lanzamiento/crecimiento: " + ", ".join(matches or ["SaaS"])
        )
        reply = (
            f"Congrats on launching {item.title}! If directory distribution and "
            "backlinks are part of your growth plan, I offer a $15 submission "
            f"service that can save you the manual work: {self.settings.checkout_url}"
        )
        return {"score": score, "reason": reason, "suggested_reply": reply}

    def _llm(self, item: SourceItem) -> dict[str, Any]:
        endpoint = (
            "https://api.groq.com/openai/v1/chat/completions"
            if self.settings.llm_provider == "groq"
            else "https://api.openai.com/v1/chat/completions"
        )
        prompt = {
            "source": item.source,
            "title": item.title,
            "description": item.body[:4000],
            "url": item.url,
        }
        response = self.session.post(
            endpoint,
            headers={
                "Authorization": f"Bearer {self.settings.llm_api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": self.settings.llm_model,
                "temperature": 0.1,
                "response_format": {"type": "json_object"},
                "messages": [
                    {
                        "role": "system",
                        "content": (
                            "Evaluate purchase intent for a $15 B2B/SaaS directory "
                            "submission service. Return strict JSON with integer score "
                            "0-10, a brief reason, and suggested_reply in natural English "
                            "of no more than four sentences. Personalize it only with "
                            "facts from the supplied item, avoid spam language and promises, "
                            f"and include this service URL: {self.settings.checkout_url}"
                        ),
                    },
                    {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)},
                ],
            },
            timeout=30,
        )
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"].strip()
        if content.startswith("```"):
            content = content.strip("`")
            if content.lower().startswith("json"):
                content = content[4:].lstrip()
        parsed = json.loads(content)
        return {
            "score": int(parsed["score"]),
            "reason": str(parsed["reason"]).strip(),
            "suggested_reply": str(parsed["suggested_reply"]).strip(),
        }

    def analyze(self, item: SourceItem) -> tuple[dict[str, Any], str]:
        if self.settings.llm_provider == "rules":
            return self._rules(item), "rules/deterministic-v1"
        try:
            return self._llm(item), self.label
        except (requests.RequestException, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            if not self.settings.allow_llm_fallback:
                raise
            LOGGER.warning(
                "%s evaluation failed (%s); using deterministic fallback",
                self.settings.llm_provider,
                exc,
            )
            return self._rules(item), "rules/deterministic-v1 (fallback)"


class TelegramClient:
    def __init__(
        self,
        settings: Settings,
        store: OpportunityStore,
        metrics: MetricsStore,
        session: requests.Session | None = None,
    ) -> None:
        self.settings = settings
        self.store = store
        self.metrics = metrics
        self.session = session or requests.Session()
        self.base_url = f"https://api.telegram.org/bot{settings.telegram_bot_token}"

    def _call(self, method: str, payload: dict[str, Any], timeout: int = 35) -> Any:
        response = self.session.post(
            f"{self.base_url}/{method}", json=payload, timeout=timeout
        )
        response.raise_for_status()
        body = response.json()
        if not body.get("ok"):
            raise RuntimeError(f"Telegram {method} failed: {body}")
        return body.get("result")

    def verify_and_notify_startup(self) -> None:
        if not self.settings.telegram_bot_token or not self.settings.telegram_chat_id:
            raise ValueError("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are required")
        identity = self._call("getMe", {})
        self.send_text(
            "✅ <b>Kiosco 2 sincronizado</b>\n"
            "Fuentes activas: Hacker News + Product Hunt\n"
            f"Evaluador: <code>{html.escape(self.settings.evaluator_label)}</code>\n"
            "Métricas dinámicas y comando /stats activos."
        )
        LOGGER.info(
            "Telegram API verified as @%s; startup notification sent",
            identity.get("username", "unknown"),
        )

    def send_text(
        self,
        text: str,
        *,
        reply_markup: dict[str, Any] | None = None,
        parse_mode: str = "HTML",
    ) -> Any:
        payload: dict[str, Any] = {
            "chat_id": self.settings.telegram_chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        return self._call("sendMessage", payload)

    def send_opportunity(self, row: sqlite3.Row) -> None:
        source = html.escape(str(row["source"]))
        title = html.escape(str(row["title"]))
        reason = html.escape(str(row["reason"]))
        evaluator = html.escape(str(row["evaluator"]))
        url = html.escape(str(row["url"]), quote=True)
        text = (
            f"🎯 <b>Nueva oportunidad · {source}</b>\n"
            f"<b>{title}</b>\n"
            f"Score: <b>{int(row['score'])}/10</b>\n"
            f"Motivo: {reason}\n"
            f"Evaluador: <code>{evaluator}</code>\n"
            f"<a href=\"{url}\">Abrir oportunidad</a>"
        )
        keyboard = {
            "inline_keyboard": [
                [
                    {
                        "text": "🟢 Aprobar y Publicar",
                        "callback_data": f"approve:{row['lead_id']}",
                    },
                    {
                        "text": "🔴 Descartar",
                        "callback_data": f"discard:{row['lead_id']}",
                    },
                ]
            ]
        }
        self.send_text(text, reply_markup=keyboard)

    def send_stats(self) -> None:
        report_date = datetime.now(ART_TIMEZONE).date()
        health = probe_services(self.settings.healthcheck_urls, self.session)
        report = format_daily_report(
            report_date,
            self.metrics.daily_stats(report_date),
            health,
            self.settings.affiliate_tag,
        )
        self.send_text(report, parse_mode="MarkdownV2")

    def poll_once(self, timeout: int = 25) -> None:
        offset = int(self.store.get_state("telegram_update_offset", "0"))
        updates = self._call(
            "getUpdates",
            {
                "offset": offset,
                "timeout": timeout,
                "allowed_updates": ["message", "callback_query"],
            },
            timeout=timeout + 10,
        )
        for update in updates:
            next_offset = int(update["update_id"]) + 1
            try:
                self._handle_update(update)
            except Exception:
                LOGGER.exception("Telegram update %s failed", update.get("update_id"))
            finally:
                self.store.set_state("telegram_update_offset", str(next_offset))

    def _handle_update(self, update: dict[str, Any]) -> None:
        message = update.get("message") or {}
        if message:
            chat_id = str((message.get("chat") or {}).get("id", ""))
            command = str(message.get("text", "")).split(maxsplit=1)[0].lower()
            if chat_id == self.settings.telegram_chat_id and command.split("@", 1)[0] == "/stats":
                self.send_stats()
            return

        callback = update.get("callback_query") or {}
        if not callback:
            return
        callback_id = str(callback.get("id", ""))
        chat_id = str(((callback.get("message") or {}).get("chat") or {}).get("id", ""))
        if chat_id != self.settings.telegram_chat_id:
            self._call(
                "answerCallbackQuery",
                {"callback_query_id": callback_id, "text": "Chat no autorizado"},
            )
            return

        action, separator, lead_id = str(callback.get("data", "")).partition(":")
        if not separator or action not in {"approve", "discard"}:
            return
        new_status = "approved" if action == "approve" else "discarded"
        row = self.store.review(lead_id, new_status)
        if row is None:
            self._call(
                "answerCallbackQuery",
                {"callback_query_id": callback_id, "text": "Ya fue procesado"},
            )
            return

        message_id = (callback.get("message") or {}).get("message_id")
        if message_id:
            self._call(
                "editMessageReplyMarkup",
                {
                    "chat_id": self.settings.telegram_chat_id,
                    "message_id": message_id,
                    "reply_markup": {"inline_keyboard": []},
                },
            )
        if action == "approve":
            reply = html.escape(str(row["suggested_reply"]))
            link = html.escape(str(row["url"]), quote=True)
            checkout = html.escape(self.settings.checkout_url, quote=True)
            suffix = f'\n\n<a href="{checkout}">Abrir servicio de distribución</a>' if checkout else ""
            self.send_text(
                "🟢 <b>Aprobado · borrador listo</b>\n"
                f"{reply}\n\n<a href=\"{link}\">Abrir publicación de origen</a>{suffix}"
            )
            answer = "Aprobado"
        else:
            answer = "Descartado"
        self._call(
            "answerCallbackQuery",
            {"callback_query_id": callback_id, "text": answer},
        )
        self.metrics.log_operational_event(
            project_id=BOT_PROJECT_ID,
            project_name=BOT_PROJECT_NAME,
            action=f"opportunity_{new_status}",
            details=json.dumps(
                {"lead_id": lead_id, "source": row["source"]}, ensure_ascii=False
            ),
        )


class Runtime:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.store = OpportunityStore(settings.database_path)
        self.metrics = MetricsStore(settings.database_path)
        self.analyzer = IntentAnalyzer(settings)
        self.telegram = TelegramClient(settings, self.store, self.metrics)
        self.monitors = {
            "Hacker News": (HackerNewsMonitor(), settings.hn_poll_seconds),
            "Product Hunt": (ProductHuntMonitor(), settings.ph_poll_seconds),
        }
        self.stop_event = threading.Event()
        self.threads: list[threading.Thread] = []
        self.dispatch_lock = threading.Lock()
        self.last_daily_report = ""

    def start(self) -> None:
        self.telegram.verify_and_notify_startup()
        for source, (monitor, interval) in self.monitors.items():
            thread = threading.Thread(
                target=self._monitor_loop,
                args=(source, monitor, interval),
                name=f"monitor-{source.lower().replace(' ', '-')}",
                daemon=True,
            )
            thread.start()
            self.threads.append(thread)
        telegram_thread = threading.Thread(
            target=self._telegram_loop, name="telegram-updates", daemon=True
        )
        telegram_thread.start()
        self.threads.append(telegram_thread)
        report_thread = threading.Thread(
            target=self._daily_report_loop, name="daily-report", daemon=True
        )
        report_thread.start()
        self.threads.append(report_thread)
        LOGGER.info(
            "Dual monitor started; evaluator=%s fallback=%s",
            self.settings.evaluator_label,
            self.settings.allow_llm_fallback,
        )

    def stop(self) -> None:
        self.stop_event.set()
        for thread in self.threads:
            thread.join(timeout=5)
        LOGGER.info("Runtime stopped")

    def run_once(self) -> None:
        for source, (monitor, _) in self.monitors.items():
            self.scan(source, monitor)

    def _monitor_loop(self, source: str, monitor: Any, interval: int) -> None:
        while not self.stop_event.is_set():
            try:
                self.scan(source, monitor)
            except Exception:
                LOGGER.exception("%s scan failed; next source remains active", source)
            self.stop_event.wait(interval)

    def scan(self, source: str, monitor: Any) -> None:
        items: list[SourceItem] = monitor.fetch_items()
        if not self.store.source_initialized(source):
            seeded = self.store.seed_source(source, items)
            LOGGER.info("%s initialized: seeded=%s (no historical outreach)", source, seeded)
            self._log_scan(source, len(items), 0, seeded=seeded)
            return

        new_count = 0
        for item in reversed(items):
            if self.store.exists(item):
                continue
            evaluation, evaluator = self.analyzer.analyze(item)
            if self.store.save_evaluation(
                item, evaluation, evaluator, self.settings.min_score
            ):
                new_count += 1
                LOGGER.info(
                    "Stored %s opportunity %s score=%s evaluator=%s",
                    source,
                    item.item_id,
                    evaluation["score"],
                    evaluator,
                )
        self._dispatch_qualified()
        self._log_scan(source, len(items), new_count)
        LOGGER.info("%s scan complete: fetched=%s new=%s", source, len(items), new_count)

    def _dispatch_qualified(self) -> None:
        with self.dispatch_lock:
            remaining = max(
                0,
                self.settings.max_approvals_per_day - self.store.notified_today(),
            )
            for row in self.store.qualified(remaining):
                self.telegram.send_opportunity(row)
                self.store.mark_notified(str(row["lead_id"]))
                self.metrics.log_operational_event(
                    project_id=BOT_PROJECT_ID,
                    project_name=BOT_PROJECT_NAME,
                    action="opportunity_notified",
                    details=json.dumps(
                        {"lead_id": row["lead_id"], "source": row["source"]},
                        ensure_ascii=False,
                    ),
                )

    def _log_scan(self, source: str, fetched: int, new: int, seeded: int = 0) -> None:
        self.metrics.log_operational_event(
            project_id=BOT_PROJECT_ID,
            project_name=BOT_PROJECT_NAME,
            action="monitor_scan",
            details=json.dumps(
                {"source": source, "fetched": fetched, "new": new, "seeded": seeded},
                ensure_ascii=False,
            ),
        )

    def _telegram_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.telegram.poll_once()
            except requests.RequestException as exc:
                LOGGER.warning("Telegram polling failed: %s", exc)
                self.stop_event.wait(5)
            except Exception:
                LOGGER.exception("Telegram polling loop failed")
                self.stop_event.wait(5)

    def _daily_report_loop(self) -> None:
        while not self.stop_event.is_set():
            now = datetime.now(ART_TIMEZONE)
            report_key = now.date().isoformat()
            if now.hour == self.settings.daily_report_hour and self.last_daily_report != report_key:
                try:
                    self.telegram.send_stats()
                    self.last_daily_report = report_key
                    LOGGER.info("Daily metrics report sent for %s", report_key)
                except Exception:
                    LOGGER.exception("Daily metrics report failed")
            self.stop_event.wait(30)


def probe_services(
    urls: tuple[str, ...], session: requests.Session | None = None
) -> list[ServiceHealth]:
    client = session or requests.Session()
    results: list[ServiceHealth] = []
    for url in urls:
        started = time.monotonic()
        try:
            response = client.get(url, timeout=12, allow_redirects=True)
            status_code = int(response.status_code)
        except requests.RequestException:
            status_code = 0
        latency_ms = int((time.monotonic() - started) * 1000)
        host = url.split("//", 1)[-1].split("/", 1)[0]
        name = HEALTHCHECK_NAMES.get(host, host)
        results.append(ServiceHealth(name, url, status_code, latency_ms))
    return results


def authorized_metrics_request(settings: Settings, x_api_key: str, authorization: str) -> None:
    if not settings.metrics_api_key:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="METRICS_API_KEY is not configured",
        )
    bearer = authorization.removeprefix("Bearer ") if authorization.startswith("Bearer ") else ""
    supplied = x_api_key or bearer
    if not supplied or not secrets.compare_digest(supplied, settings.metrics_api_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid metrics API key",
        )


def create_app(settings: Settings | None = None) -> FastAPI:
    active_settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        runtime = Runtime(active_settings)
        app.state.runtime = runtime
        runtime.start()
        try:
            yield
        finally:
            runtime.stop()

    app = FastAPI(title="Kiosco 2 Distribution Bot", version="2.0.0", lifespan=lifespan)

    @app.get("/")
    def root() -> dict[str, str]:
        return {
            "status": "online",
            "service": "kiosco2-distribution-bot",
            "version": app.version,
        }

    @app.get("/health")
    def health(request: Request) -> dict[str, Any]:
        runtime: Runtime = request.app.state.runtime
        return {
            "status": "ok",
            "sources": list(runtime.monitors),
            "evaluator": runtime.settings.evaluator_label,
            "database": str(runtime.settings.database_path),
        }

    @app.post(
        "/api/v1/log-event",
        response_model=FinancialEventResponse,
        status_code=status.HTTP_201_CREATED,
    )
    def log_event(
        event: FinancialEventCreate,
        request: Request,
        x_api_key: str = Header(default="", alias="X-API-Key"),
        authorization: str = Header(default="", alias="Authorization"),
    ) -> FinancialEventResponse:
        runtime: Runtime = request.app.state.runtime
        authorized_metrics_request(runtime.settings, x_api_key, authorization)
        return runtime.metrics.log_financial_event(event)

    @app.post(
        "/api/v1/log-click",
        response_model=TrafficEventResponse,
        status_code=status.HTTP_201_CREATED,
    )
    def log_click(
        event: TrafficEventCreate,
        request: Request,
        x_api_key: str = Header(default="", alias="X-API-Key"),
        authorization: str = Header(default="", alias="Authorization"),
    ) -> TrafficEventResponse:
        runtime: Runtime = request.app.state.runtime
        authorized_metrics_request(runtime.settings, x_api_key, authorization)
        return runtime.metrics.log_traffic_event(event)

    return app


def configure_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(threadName)s %(message)s",
    )


def main() -> None:
    load_dotenv()
    configure_logging()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="Run one scan per source")
    args = parser.parse_args()
    settings = Settings.from_env()

    if args.once:
        runtime = Runtime(settings)
        runtime.telegram.verify_and_notify_startup()
        runtime.run_once()
        return

    app = create_app(settings)
    signal.signal(signal.SIGTERM, lambda *_: LOGGER.info("SIGTERM received"))
    uvicorn.run(app, host="0.0.0.0", port=settings.port, log_level="info")


if __name__ == "__main__":
    main()
