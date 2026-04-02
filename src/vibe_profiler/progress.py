"""Progress reporting for pipeline stages."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass
class ProgressEvent:
    """Describes current progress of a pipeline operation."""

    stage: str  # "profiling", "analysis", "suggestion", "codegen"
    step: str  # e.g. "table:customers", "column:email", "business_keys"
    current: int  # current item index (1-based)
    total: int  # total items in this stage
    message: str  # human-readable description
    elapsed_seconds: float  # time since stage started
    estimated_remaining_seconds: Optional[float]  # None if not enough data


# Callback type: users provide a function that receives ProgressEvent
ProgressCallback = Callable[[ProgressEvent], None]


class ProgressTracker:
    """Track progress within a pipeline stage and emit callbacks."""

    def __init__(
        self,
        stage: str,
        total: int,
        callback: ProgressCallback | None = None,
    ) -> None:
        self.stage = stage
        self.total = total
        self.callback = callback
        self._start_time = time.monotonic()
        self._step_times: list[float] = []

    def update(self, current: int, step: str, message: str) -> None:
        """Report progress on item *current* (1-based) of *total*."""
        if self.callback is None:
            return

        now = time.monotonic()
        elapsed = now - self._start_time
        self._step_times.append(now)

        # Estimate remaining time after at least 1 completed step
        eta: float | None = None
        if current > 0 and current < self.total:
            avg_per_step = elapsed / current
            remaining_steps = self.total - current
            eta = round(avg_per_step * remaining_steps, 1)

        self.callback(
            ProgressEvent(
                stage=self.stage,
                step=step,
                current=current,
                total=self.total,
                message=message,
                elapsed_seconds=round(elapsed, 1),
                estimated_remaining_seconds=eta,
            )
        )

    def complete(self, message: str = "Done") -> None:
        """Emit a final completion event."""
        if self.callback is None:
            return
        elapsed = round(time.monotonic() - self._start_time, 1)
        self.callback(
            ProgressEvent(
                stage=self.stage,
                step="complete",
                current=self.total,
                total=self.total,
                message=message,
                elapsed_seconds=elapsed,
                estimated_remaining_seconds=0.0,
            )
        )


def default_progress_callback(event: ProgressEvent) -> None:
    """Simple print-based progress callback, suitable for notebooks."""
    pct = (event.current / event.total * 100) if event.total else 0
    eta_str = ""
    if event.estimated_remaining_seconds is not None and event.estimated_remaining_seconds > 0:
        eta = event.estimated_remaining_seconds
        if eta >= 60:
            eta_str = f" | ETA: {eta / 60:.1f}m"
        else:
            eta_str = f" | ETA: {eta:.0f}s"

    print(
        f"[{event.stage}] {event.current}/{event.total} ({pct:.0f}%) "
        f"{event.message} [{event.elapsed_seconds:.1f}s elapsed{eta_str}]"
    )
