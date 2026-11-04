"""Unit tests for the standalone cloud worker."""
from io import BytesIO
from urllib.error import HTTPError, URLError

from smart_agent.worker import _should_use_grok, canonicalize, score


def test_canonicalize_drops_tracking_query_and_www() -> None:
    assert canonicalize("https://www.Example.com/news/story/?utm_source=x&id=2#top") == (
        "https://example.com/news/story?id=2"
    )


def test_canonicalize_keeps_root_path() -> None:
    assert canonicalize("https://example.com/?ref=feed") == "https://example.com/"


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



def test_grok_fallback_on_rate_limit() -> None:
    error = HTTPError("https://example.com", 429, "rate limited", {}, BytesIO(b""))
    assert _should_use_grok(error)


def test_grok_fallback_on_google_quota_forbidden_response() -> None:
    error = HTTPError(
        "https://example.com", 403, "quota", {}, BytesIO(b'{"error":"RESOURCE_EXHAUSTED: quota"}')
    )
    assert _should_use_grok(error)


def test_no_grok_fallback_on_invalid_credentials() -> None:
    error = HTTPError("https://example.com", 403, "forbidden", {}, BytesIO(b"invalid API key"))
    assert not _should_use_grok(error)


def test_grok_fallback_on_network_error() -> None:
    assert _should_use_grok(URLError("temporary network failure"))
