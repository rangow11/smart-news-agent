"""Unit tests for the standalone cloud worker."""

from io import BytesIO
from urllib.error import HTTPError, URLError

from smart_agent.worker import (
    CATEGORIES,
    _analysis_input,
    _should_try_next_gemini,
    canonicalize,
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
    assert _gemini_model_chain() == [
        "gemini-3.8-flash",
        "gemini-3.7-flash",
        "gemini-3.6-flash",
        "gemini-3.5-flash",
        "gemini-3.5-flash-lite",
        "gemini-3.1-flash-lite",
    ]


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
        "category": CATEGORIES[0],
        "summary": "طبق گزارش منبع، این رویداد رخ داده است.",
        "importance": 70,
        "mission_relevance": 80,
        "freshness": 90,
        "is_rumor": True,
    }
    assert validate_analysis(result) == result


def test_validate_analysis_rejects_unknown_category() -> None:
    result = {
        "category": "دسته ساختگی",
        "summary": "خلاصه",
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
        "category": CATEGORIES[0],
        "summary": "خلاصه",
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
    assert "<article>" in prompt
    assert "\\n" in prompt
    assert "\\\\n" not in prompt
