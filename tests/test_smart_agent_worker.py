"""Unit tests for the standalone cloud worker."""

import json
from datetime import UTC, datetime, timedelta
from io import BytesIO
from time import struct_time
from urllib.error import HTTPError, URLError

from smart_agent.worker import (
    CATEGORIES,
    _analysis_input,
    _should_try_next_gemini,
    canonicalize,
    collection_window,
    configure_sources,
    entry_published_at,
    gemini_analyze,
    process_feed,
    report_exists,
    score,
    validate_analysis,
)



def test_parse_telegram_channel_public_preview(monkeypatch) -> None:
    from smart_agent.worker import parse_telegram_channel

    html = """
    <div class="tgme_widget_message_wrap js-widget_message_wrap">
      <div class="tgme_widget_message" data-post="iribnews/123">
        <div class="tgme_widget_message_text js-message_text" dir="auto">خبر نخست<br>توضيحات خبر</div>
        <a class="tgme_widget_message_date" href="https://t.me/iribnews/123"><time datetime="2026-11-05T10:00:00+00:00"></time></a>
      </div>
    </div>
    """
    class Response:
        def __enter__(self): return self
        def __exit__(self, *_args): return None
        def read(self, _limit): return html.encode("utf-8")
    monkeypatch.setattr("smart_agent.worker.urlopen", lambda *_args, **_kwargs: Response())
    entries = parse_telegram_channel("https://t.me/s/iribnews")
    assert len(entries) == 1
    assert entries[0]["title"].startswith("خبر نخست")
    assert entries[0]["link"] == "https://t.me/iribnews/123"
    assert entries[0]["published_parsed"] is not None


def test_canonicalize_drops_tracking_query_and_www() -> None:
    assert canonicalize("https://www.Example.com/news/story/?utm_source=x&id=2#top") == (
        "https://example.com/news/story?id=2"
    )


def test_canonicalize_keeps_root_path() -> None:
    assert canonicalize("https://example.com/?ref=feed") == "https://example.com/?ref=feed"


def test_score_uses_configured_weighting() -> None:
    result = {
        "importance": 100,
        "mission_relevance": 100,
        "freshness": 100,
        "source_quality": 100,
    }
    assert score(result) == 100


def test_score_clamps_out_of_range_values() -> None:
    result = {
        "importance": 150,
        "mission_relevance": -20,
        "freshness": 50,
        "source_quality": 100,
    }
    assert score(result) == 50


def test_gemini_fallback_on_rate_limit() -> None:
    error = HTTPError("https://example.com", 429, "rate limited", {}, BytesIO(b""))
    assert _should_try_next_gemini(error)


def test_gemini_fallback_on_google_quota_forbidden_response() -> None:
    error = HTTPError("https://example.com", 403, "quota", {}, BytesIO(b'{"error":"RESOURCE_EXHAUSTED: quota"}'))
    assert _should_try_next_gemini(error)


def test_no_model_fallback_on_invalid_credentials() -> None:
    error = HTTPError("https://example.com", 403, "forbidden", {}, BytesIO(b"invalid API key"))
    assert not _should_try_next_gemini(error)


def test_gemini_fallback_on_network_error() -> None:
    assert _should_try_next_gemini(URLError("temporary network failure"))


def test_gemini_fallback_on_unusable_model_output() -> None:
    assert _should_try_next_gemini(ValueError("invalid JSON"))


def test_gemini_model_chain_is_ordered_and_deduplicated(monkeypatch) -> None:
    from smart_agent.worker import _gemini_model_chain

    monkeypatch.delenv("GEMINI_FALLBACK_MODELS", raising=False)
    monkeypatch.delenv("GEMINI_MODEL", raising=False)
    assert _gemini_model_chain() == ["gemini-3.1-flash-lite", "gemini-2.5-flash-lite", "gemini-2.5-flash", "gemini-3-flash", "gemini-3.5-flash-lite"]



def test_dw_persian_retries_alternate_feed_when_primary_is_malformed(monkeypatch) -> None:
    from smart_agent.worker import DW_PERSIAN_FALLBACK_FEED_URL

    calls = []

    class Parsed:
        def __init__(self, bozo, entries):
            self.bozo = bozo
            self.entries = entries
            self.bozo_exception = "malformed XML"

    def fake_parse(url):
        calls.append(url)
        return Parsed(True, []) if len(calls) == 1 else Parsed(False, [])

    monkeypatch.setattr("smart_agent.worker.feedparser.parse", fake_parse)

    class Connection:
        def commit(self):
            return None

    count = process_feed(
        Connection(),
        {"name": "DW Persian", "feed_url": "https://old.example/feed", "source_quality": 85, "id": 1},
        datetime(2026, 11, 5, tzinfo=UTC),
        datetime(2026, 11, 5, 1, tzinfo=UTC),
    )

    assert count == 0
    assert calls == ["https://old.example/feed", DW_PERSIAN_FALLBACK_FEED_URL]


def test_report_exists_checks_original_and_canonical_urls() -> None:
    class Cursor:
        def __init__(self) -> None:
            self.query = None
            self.params = None

        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            return None

        def execute(self, query, params) -> None:
            self.query = query
            self.params = params

        def fetchone(self):
            return (1,)

    class Connection:
        def __init__(self) -> None:
            self.cursor_instance = Cursor()

        def cursor(self):
            return self.cursor_instance

    conn = Connection()
    assert report_exists(conn, "https://example.com/a?utm_source=x", "https://example.com/a")
    assert conn.cursor_instance.params == (
        "https://example.com/a?utm_source=x",
        "https://example.com/a",
    )


def test_validate_analysis_accepts_valid_model_output() -> None:
    result = {
        "news_track": "main",
        "category": CATEGORIES[0],
        "summary": "طبق گزارش منبع، این رویداد رخ داده است.",
        "detailed_report": "گزارش تفصیلی بر پایه متن خبر.",
        "importance": 70,
        "mission_relevance": 80,
        "freshness": 90,
        "is_rumor": True,
    }
    assert validate_analysis(result) == result


def test_validate_analysis_rejects_unknown_category() -> None:
    result = {
        "news_track": "secondary",
        "category": "دسته ساختگی",
        "summary": "خلاصه",
        "detailed_report": "گزارش تفصیلی",
        "importance": 70,
        "mission_relevance": 80,
        "freshness": 90,
        "is_rumor": False,
    }
    try:
        validate_analysis(result)
    except ValueError as exc:
        assert "unknown category" in str(exc)
    else:
        raise AssertionError("Unknown categories must not be stored")


def test_validate_analysis_rejects_invalid_score_and_rumor_type() -> None:
    result = {
        "news_track": "secondary",
        "category": CATEGORIES[0],
        "summary": "خلاصه",
        "detailed_report": "گزارش تفصیلی",
        "importance": 101,
        "mission_relevance": 80,
        "freshness": 90,
        "is_rumor": "false",
    }
    try:
        validate_analysis(result)
    except ValueError as exc:
        assert "importance" in str(exc)
    else:
        raise AssertionError("Out-of-range scores must not be stored")


def test_analysis_input_labels_article_as_untrusted_data() -> None:
    prompt = _analysis_input("عنوان", "متن خبر", "منبع")
    assert "داده غیرقابل اعتماد" in prompt
    from smart_agent.worker import SYSTEM_PROMPT
    assert "آمار تجمیعی" in SYSTEM_PROMPT
    assert "عبری" in SYSTEM_PROMPT
    assert "موقعیت" in SYSTEM_PROMPT
    assert "<article>" in prompt
    assert "\n" in prompt
    assert "\\n" not in prompt


def test_default_gemini_fallback_chain_includes_flash_lite(monkeypatch) -> None:
    from smart_agent.worker import _gemini_model_chain

    monkeypatch.delenv("GEMINI_MODEL", raising=False)
    monkeypatch.delenv("GEMINI_FALLBACK_MODELS", raising=False)
    chain = _gemini_model_chain()
    assert chain[0] == "gemini-3.1-flash-lite"
    assert chain[1] == "gemini-2.5-flash-lite"
    assert "gemma-4-26b-a4b-it" not in chain


def test_analyze_falls_back_when_model_output_fails_validation(monkeypatch) -> None:
    import smart_agent.worker as worker

    valid = {
        "news_track": "secondary",
        "category": CATEGORIES[-1],
        "summary": "خلاصه معتبر",
        "detailed_report": "گزارش تفصیلی معتبر",
        "importance": 70,
        "mission_relevance": 80,
        "freshness": 90,
        "is_rumor": False,
    }
    calls = []

    monkeypatch.setattr(worker, "_gemini_model_chain", lambda: ["first-model", "second-model"])
    monkeypatch.setattr(worker, "_ACTIVE_GEMINI_MODEL", None)

    def fake_gemini(_title, _content, _source, model=None):
        calls.append(model)
        if model == "first-model":
            invalid = dict(valid, category="دسته نامعتبر")
            return invalid, "gemini", model
        return valid, "gemini", model

    monkeypatch.setattr(worker, "gemini_analyze", fake_gemini)
    result, provider, model = worker.analyze("title", "content", "source")
    assert result["category"] == CATEGORIES[-1]
    assert provider == "gemini"
    assert model == "second-model"
    assert calls == ["first-model", "second-model"]


def test_missing_gemini_key_fails_without_model_fallback(monkeypatch) -> None:
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    try:
        gemini_analyze("title", "content", "source")
    except RuntimeError as exc:
        assert "GEMINI_API_KEY" in str(exc)
    else:
        raise AssertionError("Missing Gemini credentials must fail clearly")


def test_empty_news_sources_configuration_is_rejected(monkeypatch) -> None:
    class Connection:
        def execute(self, *_args):
            raise AssertionError("No database write should happen without sources")

    monkeypatch.setenv("NEWS_SOURCES_JSON", "[]")
    try:
        configure_sources(Connection())
    except ValueError as exc:
        assert "no feeds configured" in str(exc)
    else:
        raise AssertionError("An empty feed list must not look like a successful run")


def test_entry_published_at_uses_published_or_updated_timestamp() -> None:
    parsed = struct_time((2026, 11, 5, 10, 15, 0, 3, 309, 0))
    assert entry_published_at({"published_parsed": parsed}) == datetime(2026, 11, 5, 10, 15, tzinfo=UTC)
    assert entry_published_at({"updated_parsed": parsed}) == datetime(2026, 11, 5, 10, 15, tzinfo=UTC)
    assert entry_published_at({"title": "No timestamp"}) is None


def test_initial_collection_window_defaults_to_last_30_minutes(monkeypatch) -> None:
    class Connection:
        def execute(self, *_args):
            class Result:
                def fetchone(self):
                    return None

            return Result()

    monkeypatch.delenv("INITIAL_LOOKBACK_MINUTES", raising=False)
    now = datetime(2026, 11, 5, 10, 30, tzinfo=UTC)
    start, end = collection_window(Connection(), now)
    assert start == now - timedelta(minutes=30)
    assert end == now


def test_collection_window_uses_previous_success_with_overlap(monkeypatch) -> None:
    previous = datetime(2026, 11, 5, 10, 0, tzinfo=UTC)

    class Connection:
        def execute(self, *_args):
            class Result:
                def fetchone(self):
                    return (previous,)

            return Result()

    monkeypatch.setenv("WINDOW_OVERLAP_MINUTES", "5")
    now = datetime(2026, 11, 5, 10, 30, tzinfo=UTC)
    start, end = collection_window(Connection(), now)
    assert start == previous - timedelta(minutes=5)
    assert end == now


def test_extract_feed_text_prefers_longer_content_encoded_body() -> None:
    from smart_agent.worker import extract_feed_text

    text = extract_feed_text({
        "summary": "<p>خلاصه کوتاه</p>",
        "content": [{"value": "<p>بند اول خبر</p><p>بند دوم خبر</p>"}],
    })
    assert "بند اول خبر" in text
    assert "بند دوم خبر" in text


def test_validate_analysis_requires_detailed_report() -> None:
    result = {
        "news_track": "secondary",
        "category": CATEGORIES[0],
        "summary": "خلاصه معتبر",
        "importance": 70,
        "mission_relevance": 80,
        "freshness": 90,
        "is_rumor": False,
    }
    try:
        validate_analysis(result)
    except ValueError as exc:
        assert "detailed report" in str(exc)
    else:
        raise AssertionError("A missing detailed report must be rejected")


def test_batch_builder_respects_item_limit(monkeypatch) -> None:
    from smart_agent.worker import build_news_batches

    monkeypatch.setenv("GEMINI_BATCH_MAX_ITEMS", "2")
    monkeypatch.setenv("GEMINI_BATCH_MAX_CHARS", "18000")
    items = [
        {"id": str(i), "source_name": "منبع", "title": f"عنوان {i}", "content": "متن خبر"}
        for i in range(5)
    ]
    batches = build_news_batches(items)
    assert [len(batch) for batch in batches] == [2, 2, 1]


def test_default_model_chain_avoids_unverified_gemma_variants(monkeypatch) -> None:
    from smart_agent.worker import _gemini_model_chain

    monkeypatch.delenv("GEMINI_MODEL", raising=False)
    monkeypatch.delenv("GEMINI_FALLBACK_MODELS", raising=False)
    chain = _gemini_model_chain()
    assert chain[:2] == ["gemini-3.1-flash-lite", "gemini-2.5-flash-lite"]


def test_batch_response_validation_keeps_only_expected_valid_ids(monkeypatch) -> None:
    import smart_agent.worker as worker

    class Response:
        def __enter__(self): return self
        def __exit__(self, *_args): return None
        def read(self):
            return json.dumps({
                "candidates": [{
                    "content": {"parts": [{
                        "text": json.dumps({"results": [
                            {"id": "a", "news_track": "main", "category": worker.CATEGORIES[-1],
                             "summary": "خلاصه", "detailed_report": "گزارش", "importance": 70,
                             "mission_relevance": 80, "freshness": 90, "is_rumor": False},
                            {"id": "invented", "news_track": "main", "category": worker.CATEGORIES[-1],
                             "summary": "خلاصه", "detailed_report": "گزارش", "importance": 70,
                             "mission_relevance": 80, "freshness": 90, "is_rumor": False}
                        ]})
                    }]}
                }]
            }).encode()

    monkeypatch.setenv("GEMINI_API_KEY", "test")
    monkeypatch.setattr(worker, "urlopen", lambda *_args, **_kwargs: Response())
    result, provider, model = worker.gemini_analyze_batch([
        {"id": "a", "source_name": "منبع", "title": "عنوان", "content": "متن"}
    ], model="gemma-4-26b-a4b-it")
    assert list(result) == ["a"]
    assert provider == "gemini"
    assert model == "gemma-4-26b-a4b-it"


def test_ntfy_run_message_is_persian_compact_and_under_mobile_limit() -> None:
    from smart_agent.worker import build_ntfy_run_message

    reports = [
        {"id": i, "title": f"English title {i}", "summary": f"تیتر فارسی خبر شماره {i}",
         "detailed_report": "متن فارسی گزارش درباره تحولات مهم و تازه در منطقه.",
         "source_name": "خبرگزاری نمونه", "total_score": 91}
        for i in range(1, 11)
    ]
    message = build_ntfy_run_message(reports)
    assert "۱۰ خبر مهم تازه" in message
    assert "تیتر فارسی خبر شماره 1" in message
    assert "English title" not in message
    assert len(message.encode("utf-8")) <= 3800


def test_ntfy_run_message_reports_omitted_items_when_oversized() -> None:
    from smart_agent.worker import build_ntfy_run_message

    reports = [
        {"title": "English", "summary": f"تیتر فارسی {i}", "detailed_report": "شرح فارسی " * 1000,
         "source_name": "منبع فارسی", "total_score": 90}
        for i in range(100)
    ]
    message = build_ntfy_run_message(reports)
    assert len(message.encode("utf-8")) <= 3800
    assert "خبر دیگر" in message


def test_ntfy_title_header_is_rfc2047_encoded_for_urllib() -> None:
    import base64
    import smart_agent.worker as worker

    encoded = worker.ntfy_title_header(10)
    assert encoded.startswith("=?UTF-8?B?")
    assert encoded.endswith("?=")
    assert encoded.isascii()
    decoded = base64.b64decode(encoded.removeprefix("=?UTF-8?B?").removesuffix("?=")).decode("utf-8")
    assert decoded == "۱۰ خبر مهم تازه | عامل خبری"

def test_batch_analysis_retries_transient_503_with_exponential_backoff(monkeypatch) -> None:
    import smart_agent.worker as worker

    calls = []
    delays = []
    valid = {"news-1": {"summary": "خلاصه"}}

    monkeypatch.setattr(worker, "_gemini_model_chain", lambda: ["test-model"])
    monkeypatch.setattr(worker, "_ACTIVE_GEMINI_MODEL", None)
    monkeypatch.setattr(worker.time, "sleep", delays.append)

    def fake_batch(_items, model=None):
        calls.append(model)
        if len(calls) < 3:
            raise HTTPError("https://example.com", 503, "temporarily unavailable", {}, BytesIO(b""))
        return valid, "gemini", model

    monkeypatch.setattr(worker, "gemini_analyze_batch", fake_batch)
    results, provider, model = worker.analyze_batch([{"id": "news-1"}])

    assert results == valid
    assert provider == "gemini"
    assert model == "test-model"
    assert calls == ["test-model", "test-model", "test-model"]
    assert delays == [2.0, 4.0]


def test_pending_news_holds_all_associated_source_checkpoints() -> None:
    from smart_agent.worker import pending_source_ids

    candidates = [
        {"id": "pending-1", "source": {"id": 1}, "source_ids": [1, 2]},
        {"id": "saved-1", "source": {"id": 3}, "source_ids": [3]},
        {"id": "pending-2", "source": {"id": 4}, "source_ids": [4]},
    ]

    assert pending_source_ids(candidates, {"pending-1", "pending-2"}) == {1, 2, 4}

