"""vibe-profiler: Data Vault 2.0 profiling and dbt code generation for Databricks."""

from vibe_profiler._version import __version__
from vibe_profiler.pipeline import VibeProfilerPipeline
from vibe_profiler.progress import ProgressEvent, default_progress_callback
from vibe_profiler.report import ReportGenerator

__all__ = [
    "__version__",
    "ProgressEvent",
    "ReportGenerator",
    "VibeProfilerPipeline",
    "default_progress_callback",
]
