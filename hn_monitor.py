"""Hacker News launch monitor backed by the public Algolia API."""

from __future__ import annotations

import html
import re
from typing import Any

import requests

from source_item import SourceItem

ALGOLIA_ENDPOINT = "https://hn.algolia.com/api/v1/search_by_date"
KEYWORDS = (
    "launching",
    "first users",
    "first 100 users",
    "directory",
    "backlinks",
    "show hn",
    "saas",
    "saas marketing",
)
QUERIES = ("launch SaaS", "launching", "first users", "directory", "backlinks", "Show HN", "SaaS")


def _plain_text(value: str) -> str:
    value = re.sub(r"<[^>]+>", " ", value or "")
    return re.sub(r"\s+", " ", html.unescape(value)).strip()


class HackerNewsMonitor:
    source_name = "Hacker News"

    def __init__(self, session: requests.Session | None = None) -> None:
        self.session = session or requests.Session()

    @staticmethod
    def matches_keywords(title: str, body: str) -> bool:
        haystack = f"{title}\n{body}".casefold()
        return any(keyword.casefold() in haystack for keyword in KEYWORDS)

    def fetch_items(self) -> list[SourceItem]:
        items: dict[str, SourceItem] = {}
        for query in QUERIES:
            response = self.session.get(
                ALGOLIA_ENDPOINT,
                params={
                    "tags": "(story,poll)",
                    "query": query,
                    "hitsPerPage": 30,
                },
                headers={"User-Agent": "kiosco2-distribution-bot/2.0"},
                timeout=30,
            )
            response.raise_for_status()
            payload: dict[str, Any] = response.json()
            for hit in payload.get("hits", []):
                item_id = str(hit.get("objectID") or "").strip()
                title = _plain_text(
                    str(hit.get("title") or hit.get("story_title") or "")
                )
                body = _plain_text(
                    str(hit.get("story_text") or hit.get("comment_text") or "")
                )
                if not item_id or not title or not self.matches_keywords(title, body):
                    continue
                items[item_id] = SourceItem(
                    source=self.source_name,
                    item_id=item_id,
                    title=title,
                    body=body,
                    url=f"https://news.ycombinator.com/item?id={item_id}",
                    author=str(hit.get("author") or ""),
                    published_at=str(hit.get("created_at") or ""),
                )
        return sorted(
            items.values(),
            key=lambda item: item.published_at,
            reverse=True,
        )
