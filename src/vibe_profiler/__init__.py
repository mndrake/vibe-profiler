"""vibe-profiler: Data Vault 2.0 profiling and dbt code generation for Databricks."""

from vibe_profiler._version import __version__
from vibe_profiler.pipeline import VibeProfilerPipeline

__all__ = ["__version__", "VibeProfilerPipeline"]
