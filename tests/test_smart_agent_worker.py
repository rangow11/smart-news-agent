"""Unit tests for the standalone cloud worker."""

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
    assert score(result) == 60


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
    assert _gemini_model_chain() == ["gemini-3.1-flash-lite", "gemini-3.5-flash-lite", "gemini-3.5-flash"]



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
    assert "gemini-3.5-flash-lite" in chain
    assert "gemini-3.5-flash" in chain


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
