"""Tests for utility functions."""

from vibe_profiler.utils import (
    jaccard_similarity,
    levenshtein_ratio,
    name_tokens,
    normalize_name,
    strip_common_prefixes,
)


def test_normalize_name():
    assert normalize_name("CustomerID") == "customerid"
    assert normalize_name("order-date") == "order_date"
    assert normalize_name("  Total Amount  ") == "total_amount"


def test_levenshtein_ratio():
    assert levenshtein_ratio("customer_id", "customer_id") == 1.0
    assert levenshtein_ratio("customer_id", "cust_id") > 0.5
    assert levenshtein_ratio("abc", "xyz") < 0.5
    assert levenshtein_ratio("", "") == 1.0


def test_jaccard_similarity():
    assert jaccard_similarity({"a", "b"}, {"a", "b"}) == 1.0
    assert jaccard_similarity({"a", "b"}, {"c", "d"}) == 0.0
    assert jaccard_similarity({"a", "b", "c"}, {"b", "c", "d"}) == 0.5


def test_name_tokens():
    assert name_tokens("customer_id") == {"customer", "id"}
    assert name_tokens("order_date") == {"order", "date"}


def test_strip_common_prefixes():
    assert strip_common_prefixes("cust_id") == "id"
    assert strip_common_prefixes("id") == "id"
