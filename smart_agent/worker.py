"""Standalone cloud worker for Smart News Agent.

The legacy Topic Watch application remains untouched. This worker is the first
deployable slice: RSS ingestion, deterministic URL deduplication, Gemini-first
classification/scoring, and Grok fallback on Gemini quota/rate-limit failures.
"""
from __future__ import annotations

import json
import logging
import os
import re
import sys
from datetime import UTC, datetime
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen

import feedparser

LOG = logging.getLogger("smart_news_worker")
SCHEMA = """
CREATE TABLE IF NOT EXISTS news_sources (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    feed_url TEXT NOT NULL UNIQUE,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS news_reports (
    id BIGSERIAL PRIMARY KEY,
    source_id BIGINT REFERENCES news_sources(id) ON DELETE SET NULL,
    source_name TEXT NOT NULL,
    title TEXT NOT NULL,
    url TEXT NOT NULL UNIQUE,
    canonical_url TEXT NOT NULL UNIQUE,
    published_at TIMESTAMPTZ,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    content TEXT NOT NULL DEFAULT '',
    category TEXT,
    summary TEXT,
    importance INTEGER,
    mission_relevance INTEGER,
    freshness INTEGER,
    source_quality INTEGER,
    total_score INTEGER,
    is_rumor BOOLEAN NOT NULL DEFAULT FALSE,
    ai_provider TEXT,
    ai_model TEXT,
    ai_processed_at TIMESTAMPTZ,
    raw_ai_json JSONB
);
CREATE INDEX IF NOT EXISTS idx_news_reports_score_time
    ON news_reports(total_score DESC, fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_news_reports_published
    ON news_reports(published_at DESC);
"""

CATEGORIES = [
    "شایعات", "نقاط قوت و ضعف", "تهدیدات و فرصت‌ها", "مشکلات و صدمات",
    "وضعیت نیروها و رقیب", "سازمان‌های موازی", "غیبت و فرار",
    "خدمات شهری", "وضعیت روحیه", "سایر و پیشنهادات",
]
SYSTEM_PROMPT = (
    """تو دستیار غربالگری اخبار هستی. فقط بر اساس متن ورودی کار کن.
خبر یا واقعیت جدید نساز و ادعای منبع را تأیید قطعی نکن. اگر متن شایعه/ادعاست
is_rumor را true بگذار. خروجی فقط JSON معتبر با کلیدهای category, summary,
importance, mission_relevance, freshness, source_quality, is_rumor باشد.
category یکی از این دسته‌ها باشد: """
    + "، ".join(CATEGORIES)
    + """
summary خلاصه وفادار و کوتاه فارسی باشد و انتساب ادعا به منبع را حفظ کند.
importance، mission_relevance، freshness و source_quality عدد صحیح 0 تا 100 باشند.
امتیاز کل را محاسبه نکن؛ برنامه آن را با وزن‌های 40/30/20/10 محاسبه می‌کند.
"""
)


def canonicalize(url: str) -> str:
    parts = urlsplit(url.strip())
    host = (parts.hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    path = re.sub(r"/+$", "", parts.path) or "/"
    query = [
        (key, value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if not key.lower().startswith("utm_") and key.lower() not in {"fbclid", "gclid", "mc_cid", "mc_eid"}
    ]
    return urlunsplit((parts.scheme.lower() or "https", host, path, urlencode(query, doseq=True), ""))


def gemini_analyze(title: str, content: str, source: str) -> tuple[dict, str, str]:
    key = os.environ["GEMINI_API_KEY"]
    model = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
    prompt = f"{SYSTEM_PROMPT}\nمنبع: {source}\nعنوان: {title}\nمتن: {content[:12000]}"
    endpoint = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        + quote(model, safe="")
        + ":generateContent?key="
        + quote(key, safe="")
    )
    body = json.dumps(
        {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"responseMimeType": "application/json", "temperature": 0.1},
        }
    ).encode()
    req = Request(endpoint, data=body, headers={"Content-Type": "application/json"}, method="POST")
    with urlopen(req, timeout=90) as response:  # noqa: S310
        payload = json.loads(response.read())
    text = payload["candidates"][0]["content"]["parts"][0]["text"]
    return json.loads(text), "gemini", model


def grok_analyze(title: str, content: str, source: str) -> tuple[dict, str, str]:
    key = os.environ["GROK_API_KEY"]
    model = os.getenv("GROK_MODEL", "grok-3-mini")
    prompt = f"{SYSTEM_PROMPT}\nمنبع: {source}\nعنوان: {title}\nمتن: {content[:12000]}"
    body = json.dumps(
        {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "response_format": {"type": "json_object"},
            "temperature": 0.1,
        }
    ).encode()
    req = Request(
        "https://api.x.ai/v1/chat/completions",
        data=body,
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(req, timeout=90) as response:  # noqa: S310
        payload = json.loads(response.read())
    return json.loads(payload["choices"][0]["message"]["content"]), "grok", model


def _should_use_grok(exc: Exception) -> bool:
    """Fallback only for quota exhaustion or transient Gemini availability errors."""
    if isinstance(exc, (URLError, TimeoutError)):
        return True
    if not isinstance(exc, HTTPError):
        return False
    if exc.code in (429, 500, 502, 503, 504):
        return True
    if exc.code == 403:
        # A 403 is a quota issue only when Google's response says so.
        try:
            detail = exc.read(4000).decode("utf-8", errors="replace").lower()
        except Exception:
            return False
        return any(token in detail for token in ("quota", "resource_exhausted", "rate limit"))
    return False


def analyze(title: str, content: str, source: str) -> tuple[dict, str, str]:
    try:
        return gemini_analyze(title, content, source)
    except (HTTPError, URLError, TimeoutError, KeyError, ValueError) as exc:
        if not _should_use_grok(exc):
            raise
        LOG.warning("Gemini unavailable (%s); switching to Grok fallback", type(exc).__name__)
        return grok_analyze(title, content, source)

def score(result: dict) -> int:
    weights = (("importance", 40), ("mission_relevance", 30), ("freshness", 20), ("source_quality", 10))
    return round(sum(max(0, min(100, int(result.get(key, 0)))) * weight / 100 for key, weight in weights))


def process_feed(conn, source: dict) -> int:
    parsed = feedparser.parse(source["feed_url"])
    if parsed.bozo and not parsed.entries:
        LOG.warning("RSS parse failed for %s: %s", source["name"], parsed.bozo_exception)
        return 0
    inserted = 0
    for entry in parsed.entries[:100]:
        title = (entry.get("title") or "").strip()
        url = (entry.get("link") or "").strip()
        if not title or not url:
            continue
        canonical = canonicalize(url)
        content = (entry.get("summary") or entry.get("description") or "").strip()
        content = re.sub(r"<[^>]+>", " ", content)
        content = re.sub(r"\s+", " ", content)[:12000]
        try:
            result, provider, model = analyze(title, content, source["name"])
            required = ("category", "summary", "importance", "mission_relevance", "freshness", "source_quality")
            if any(key not in result for key in required):
                raise ValueError("AI response is missing required scoring fields")
            total = score(result)
            published = None
            if entry.get("published_parsed"):
                published = datetime(*entry.published_parsed[:6], tzinfo=UTC)
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO news_reports
                    (source_id, source_name, title, url, canonical_url, published_at, content,
                     category, summary, importance, mission_relevance, freshness, source_quality,
                     total_score, is_rumor, ai_provider, ai_model, ai_processed_at, raw_ai_json)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),%s)
                    ON CONFLICT DO NOTHING RETURNING id""",
                    (
                        source["id"],
                        source["name"],
                        title,
                        url,
                        canonical,
                        published,
                        content,
                        result["category"],
                        result["summary"],
                        result["importance"],
                        result["mission_relevance"],
                        result["freshness"],
                        result["source_quality"],
                        total,
                        bool(result.get("is_rumor", False)),
                        provider,
                        model,
                        json.dumps(result, ensure_ascii=False),
                    ),
                )
                if cur.fetchone():
                    inserted += 1
        except Exception:
            LOG.exception("Could not process report from %s: %s", source["name"], title[:100])
    conn.commit()
    return inserted


def configure_sources(conn) -> None:
    """Upsert public RSS sources supplied as a JSON GitHub Actions variable."""
    raw = os.getenv("NEWS_SOURCES_JSON", "[]")
    try:
        sources = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("NEWS_SOURCES_JSON must be a JSON array") from exc
    if not isinstance(sources, list):
        raise ValueError("NEWS_SOURCES_JSON must be a JSON array")
    for item in sources:
        if not isinstance(item, dict) or not item.get("name") or not item.get("feed_url"):
            raise ValueError("Each source needs name and feed_url")
        conn.execute(
            """INSERT INTO news_sources (name, feed_url, enabled)
               VALUES (%s, %s, TRUE)
               ON CONFLICT (feed_url) DO UPDATE
               SET name = EXCLUDED.name, enabled = TRUE""",
            (str(item["name"]).strip(), str(item["feed_url"]).strip()),
        )
    conn.commit()


def main() -> int:
    import psycopg
    from psycopg.rows import dict_row

    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
    database_url = os.environ["NEON_DATABASE_URL"]
    with psycopg.connect(database_url, row_factory=dict_row, connect_timeout=15) as conn:
        conn.execute(SCHEMA)
        conn.commit()
        configure_sources(conn)
        sources = conn.execute(
            "SELECT id, name, feed_url FROM news_sources WHERE enabled = TRUE ORDER BY id"
        ).fetchall()
        total = 0
        for source in sources:
            try:
                count = process_feed(conn, source)
                LOG.info("%s: %d new reports", source["name"], count)
                total += count
            except Exception:
                LOG.exception("Feed failed: %s", source["name"])
        LOG.info("Run complete: %d new reports from %d sources", total, len(sources))
    return 0


if __name__ == "__main__":
    sys.exit(main())
