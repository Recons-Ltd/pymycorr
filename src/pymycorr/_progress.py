"""Progress display utilities for streaming operations."""

from __future__ import annotations

import sys
import time
from typing import Any, Literal


def detect_environment() -> Literal["notebook", "terminal", "non_interactive"]:
    """Detect execution environment for appropriate progress display.

    Returns:
        'notebook' if running in Jupyter/IPython notebook,
        'terminal' if running in interactive terminal,
        'non_interactive' if output is piped or in CI.
    """
    try:
        from IPython.core.getipython import get_ipython

        ipython = get_ipython()  # type: ignore[no-untyped-call]
        if ipython is not None and "IPKernelApp" in ipython.config:
            return "notebook"
    except (ImportError, AttributeError):
        pass

    if hasattr(sys.stdout, "isatty") and sys.stdout.isatty():
        return "terminal"

    return "non_interactive"


def format_bytes(num_bytes: int | float) -> str:
    """Format bytes as human-readable string.

    Args:
        num_bytes: Number of bytes.

    Returns:
        Formatted string like '12.5 MB'.
    """
    value = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB"):
        if abs(value) < 1024:
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} TB"


def format_elapsed(seconds: float) -> str:
    """Format elapsed time as MM:SS.

    Args:
        seconds: Elapsed time in seconds.

    Returns:
        Formatted string like '01:23'.
    """
    mins, secs = divmod(int(seconds), 60)
    return f"{mins:02d}:{secs:02d}"


def format_transfer_summary(
    total_bytes: int,
    elapsed_seconds: float,
    uncompressed_bytes: int | None = None,
) -> str:
    """Format completion summary message.

    Args:
        total_bytes: Total compressed bytes transferred over the wire.
        elapsed_seconds: Time taken in seconds.
        uncompressed_bytes: Optional uncompressed (in-memory) size of the data.

    Returns:
        Formatted summary like 'Downloaded 4.2 MB → 14.7 MB in 2.9s (1.4 MB/s)'.
    """
    speed = total_bytes / elapsed_seconds if elapsed_seconds > 0 else 0
    speed_str = format_bytes(speed)
    size_str = format_bytes(total_bytes)
    if uncompressed_bytes is not None:
        size_str = f"{size_str} → {format_bytes(uncompressed_bytes)}"
    return f"Downloaded {size_str} in {elapsed_seconds:.1f}s ({speed_str}/s)"


class ProgressTracker:
    """Context manager for tracking streaming progress with text output.

    Prints live progress updates that overwrite the same line,
    then prints a final summary on completion.
    """

    def __init__(
        self,
        enabled: bool | Literal["auto"],
        desc: str = "Downloading",
    ) -> None:
        """Initialize the progress tracker.

        Args:
            enabled: True to always show, False to never show, 'auto' to detect.
            desc: Description label for the progress output.
        """
        self._enabled = enabled
        self._desc = desc
        self._start_time: float = 0
        self._elapsed: float = 0
        self._total_bytes: int = 0
        self._show_progress: bool = False
        self._last_line_len: int = 0

    def __enter__(self) -> ProgressTracker:
        """Start tracking progress."""
        self._start_time = time.monotonic()
        self._show_progress = self._should_show()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Stop tracking and clear progress line. Does not print summary."""
        self._elapsed = time.monotonic() - self._start_time
        if self._show_progress:
            self._clear_line()

    @property
    def total_bytes(self) -> int:
        """Total compressed bytes received so far."""
        return self._total_bytes

    def print_summary(self, uncompressed_bytes: int | None = None) -> None:
        """Print the completion summary line.

        Args:
            uncompressed_bytes: Optional uncompressed (in-memory) size of the data.
        """
        if self._show_progress and self._total_bytes > 0:
            summary = format_transfer_summary(self._total_bytes, self._elapsed, uncompressed_bytes)
            sys.stdout.write(summary + "\n")
            sys.stdout.flush()

    def _should_show(self) -> bool:
        """Determine if progress should be shown."""
        if self._enabled is False:
            return False
        if self._enabled == "auto":
            return detect_environment() != "non_interactive"
        return True

    def _clear_line(self) -> None:
        """Clear the current line by overwriting with spaces."""
        if self._last_line_len > 0:
            sys.stdout.write("\r" + " " * self._last_line_len + "\r")

    def _write_line(self, line: str) -> None:
        """Write a line, tracking length for later clearing."""
        self._clear_line()
        sys.stdout.write(line)
        sys.stdout.flush()
        self._last_line_len = len(line)

    def update(self, chunk_size: int) -> None:
        """Update progress with received chunk.

        Args:
            chunk_size: Number of bytes received.
        """
        self._total_bytes += chunk_size
        if self._show_progress:
            elapsed = time.monotonic() - self._start_time
            speed = self._total_bytes / elapsed if elapsed > 0 else 0
            line = (
                f"\r{self._desc}: {format_bytes(self._total_bytes)} "
                f"[{format_elapsed(elapsed)}, {format_bytes(speed)}/s]"
            )
            self._write_line(line)
