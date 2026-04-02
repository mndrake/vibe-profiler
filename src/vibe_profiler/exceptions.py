"""Custom exception hierarchy for vibe-profiler."""


class VibeProfilerError(Exception):
    """Base exception for all vibe-profiler errors."""


class ProfilingError(VibeProfilerError):
    """Raised when data profiling fails."""


class AnalysisError(VibeProfilerError):
    """Raised when analysis stage fails."""


class SuggestionError(VibeProfilerError):
    """Raised when vault suggestion generation fails."""


class CodeGenError(VibeProfilerError):
    """Raised when dbt code generation fails."""


class ConfigError(VibeProfilerError):
    """Raised for invalid configuration."""
