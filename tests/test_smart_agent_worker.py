"""Unit tests for the standalone cloud worker."""
from smart_agent.worker import canonicalize, score


def test_canonicalize_drops_tracking_query_and_www() -> None:
    assert canonicalize("https://www.Example.com/news/story/?utm_source=x&id=2#top") == (
        "https://example.com/news/story"
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
    assert score(result) == 70
