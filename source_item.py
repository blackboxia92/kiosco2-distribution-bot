"""Shared normalized item model for public launch sources."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceItem:
    source: str
    item_id: str
    title: str
    body: str
    url: str
    author: str = ""
    published_at: str = ""
