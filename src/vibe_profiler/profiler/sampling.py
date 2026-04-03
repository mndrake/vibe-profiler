"""Configurable sampling strategies for large Spark tables."""

from __future__ import annotations

from pyspark.sql import DataFrame


def auto_sample(
    df: DataFrame,
    threshold_rows: int = 10_000_000,
    target_rows: int = 10_000_000,
    forced_fraction: float | None = None,
) -> tuple[DataFrame, bool, float | None]:
    """Return (possibly-sampled df, was_sampled, sample_fraction).

    If *forced_fraction* is set, always sample at that rate.
    Otherwise sample only when estimated row count exceeds *threshold_rows*.
    """
    if forced_fraction is not None:
        frac = max(0.0001, min(forced_fraction, 1.0))
        return df.sample(fraction=frac, seed=42), True, frac

    # Fast approximate count via Spark plan statistics when available
    try:
        count = df.count()
    except Exception:
        return df, False, None

    if count <= threshold_rows:
        return df, False, None

    frac = max(0.0001, min(target_rows / count, 1.0))
    return df.sample(fraction=frac, seed=42), True, frac
