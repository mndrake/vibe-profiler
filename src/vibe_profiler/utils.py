"""Shared utility helpers."""

from __future__ import annotations

import re


def normalize_name(name: str) -> str:
    """Normalize a column/table name to lowercase snake_case for comparison."""
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    return name.strip("_")


def levenshtein_ratio(a: str, b: str) -> float:
    """Compute normalized Levenshtein similarity between two strings (0-1)."""
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    n, m = len(a), len(b)
    if n > m:
        a, b = b, a
        n, m = m, n
    prev = list(range(n + 1))
    for j in range(1, m + 1):
        curr = [j] + [0] * n
        for i in range(1, n + 1):
            cost = 0 if a[i - 1] == b[j - 1] else 1
            curr[i] = min(curr[i - 1] + 1, prev[i] + 1, prev[i - 1] + cost)
        prev = curr
    distance = prev[n]
    return 1.0 - distance / max(n, m)


def jaccard_similarity(set_a: set, set_b: set) -> float:
    """Compute Jaccard similarity between two sets."""
    if not set_a and not set_b:
        return 1.0
    if not set_a or not set_b:
        return 0.0
    intersection = len(set_a & set_b)
    union = len(set_a | set_b)
    return intersection / union if union else 0.0


def name_tokens(name: str) -> set[str]:
    """Split a column name into constituent tokens for comparison."""
    normalized = normalize_name(name)
    return set(normalized.split("_"))


def strip_common_prefixes(name: str) -> str:
    """Remove common table-prefix patterns from column names (e.g., 'cust_id' -> 'id')."""
    parts = normalize_name(name).split("_")
    if len(parts) > 1:
        return "_".join(parts[1:])
    return parts[0]
