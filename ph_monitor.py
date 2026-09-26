"""Product Hunt daily launch monitor backed by its public RSS feed."""

from __future__ import annotations

import html
import re
import xml.etree.ElementTree as ET

import requests

from source_item import SourceItem

PRODUCT_HUNT_FEED = "https://www.producthunt.com/feed"


def _plain_text(value: str) -> str:
    value = re.sub(r"<[^>]+>", " ", value or "")
    return re.sub(r"\s+", " ", html.unescape(value)).strip()


def _first_text(node: ET.Element, paths: tuple[str, ...]) -> str:
    for path in paths:
        found = node.find(path)
        if found is not None and found.text:
            return found.text.strip()
    return ""


class ProductHuntMonitor:
    source_name = "Product Hunt"

    def __init__(self, session: requests.Session | None = None) -> None:
        self.session = session or requests.Session()

    @staticmethod
    def parse_feed(content: bytes) -> list[SourceItem]:
        root = ET.fromstring(content)
        entries = root.findall("./channel/item")
        atom = False
        if not entries:
            atom = True
            entries = root.findall("{http://www.w3.org/2005/Atom}entry")

        items: list[SourceItem] = []
        for entry in entries:
            if atom:
                ns = "{http://www.w3.org/2005/Atom}"
                title = _first_text(entry, (f"{ns}title",))
                body = _first_text(entry, (f"{ns}content", f"{ns}summary"))
                item_id = _first_text(entry, (f"{ns}id",))
                author = _first_text(entry, (f"{ns}author/{ns}name",))
                published = _first_text(entry, (f"{ns}published", f"{ns}updated"))
                link_node = entry.find(f"{ns}link")
                url = str(link_node.get("href") if link_node is not None else "")
            else:
                title = _first_text(entry, ("title",))
                body = _first_text(entry, ("description",))
                item_id = _first_text(entry, ("guid", "id"))
                author = _first_text(entry, ("author",))
                published = _first_text(entry, ("pubDate", "published"))
                url = _first_text(entry, ("link",))

            title = _plain_text(title)
            body = _plain_text(body)
            url = url.strip()
            item_id = (item_id or url or title).strip()
            if not item_id or not title or not url:
                continue
            items.append(
                SourceItem(
                    source="Product Hunt",
                    item_id=item_id,
                    title=title,
                    body=body,
                    url=url,
                    author=_plain_text(author),
                    published_at=published.strip(),
                )
            )
        return items

    def fetch_items(self) -> list[SourceItem]:
        response = self.session.get(
            PRODUCT_HUNT_FEED,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (compatible; kiosco2-distribution-bot/2.0; "
                    "+https://kiosco2-directory-submitter-production.up.railway.app/jobs)"
                ),
                "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml",
            },
            timeout=30,
        )
        response.raise_for_status()
        return self.parse_feed(response.content)
