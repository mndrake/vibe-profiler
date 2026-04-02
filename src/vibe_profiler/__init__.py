"""vibe-profiler: Data Vault 2.0 profiling and dbt code generation for Databricks."""

from vibe_profiler._version import __version__
from vibe_profiler.pipeline import VibeProfilerPipeline
from vibe_profiler.progress import ProgressEvent, default_progress_callback

__all__ = [
    "__version__",
    "ProgressEvent",
    "VibeProfilerPipeline",
    "default_progress_callback",
]
