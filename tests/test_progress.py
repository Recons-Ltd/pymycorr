"""Tests for progress display utilities."""

from __future__ import annotations

import sys
from io import StringIO
from unittest.mock import patch

from pymycorr._progress import (
    ProgressTracker,
    format_bytes,
    format_elapsed,
    format_transfer_summary,
)


class TestFormatBytes:
    def test_bytes(self) -> None:
        assert format_bytes(500) == "500.0 B"

    def test_kilobytes(self) -> None:
        assert format_bytes(1536) == "1.5 KB"

    def test_megabytes(self) -> None:
        assert format_bytes(4_400_000) == "4.2 MB"

    def test_gigabytes(self) -> None:
        assert format_bytes(2_147_483_648) == "2.0 GB"

    def test_zero(self) -> None:
        assert format_bytes(0) == "0.0 B"


class TestFormatElapsed:
    def test_seconds(self) -> None:
        assert format_elapsed(5.3) == "00:05"

    def test_minutes_and_seconds(self) -> None:
        assert format_elapsed(83.7) == "01:23"

    def test_zero(self) -> None:
        assert format_elapsed(0) == "00:00"


class TestFormatTransferSummary:
    def test_compressed_only(self) -> None:
        result = format_transfer_summary(4_200_000, 2.9)
        assert "Downloaded 4.0 MB in 2.9s" in result
        assert "→" not in result
        assert "/s)" in result

    def test_with_uncompressed(self) -> None:
        result = format_transfer_summary(4_200_000, 2.9, uncompressed_bytes=14_700_000)
        assert "→" in result
        assert "4.0 MB" in result
        assert "14.0 MB" in result
        assert "2.9s" in result
        assert "/s)" in result

    def test_no_uncompressed(self) -> None:
        result = format_transfer_summary(1_000_000, 1.0, uncompressed_bytes=None)
        assert "→" not in result
        assert "Downloaded" in result

    def test_zero_elapsed(self) -> None:
        result = format_transfer_summary(1000, 0.0)
        assert "0.0s" in result
        assert "0.0 B/s" in result

    def test_speed_is_compressed(self) -> None:
        # Speed should be based on compressed bytes, not uncompressed
        result = format_transfer_summary(1_000_000, 1.0, uncompressed_bytes=10_000_000)
        # 1MB in 1s = ~1MB/s (compressed speed)
        assert "1000.0 KB/s" in result or "976.6 KB/s" in result


class TestProgressTracker:
    def test_update_accumulates(self) -> None:
        tracker = ProgressTracker(enabled=False)
        with tracker:
            tracker.update(100)
            tracker.update(200)
            tracker.update(300)
        assert tracker.total_bytes == 600

    def test_total_bytes_property(self) -> None:
        tracker = ProgressTracker(enabled=False)
        assert tracker.total_bytes == 0
        with tracker:
            tracker.update(42)
        assert tracker.total_bytes == 42

    def test_exit_does_not_print_summary(self) -> None:
        buf = StringIO()
        with patch.object(sys, "stdout", buf):
            tracker = ProgressTracker(enabled=True)
            with tracker:
                tracker.update(1000)
        output = buf.getvalue()
        assert "Downloaded" not in output

    def test_print_summary_with_uncompressed(self) -> None:
        buf = StringIO()
        tracker = ProgressTracker(enabled=True)
        with tracker:
            tracker.update(4_200_000)
        with patch.object(sys, "stdout", buf):
            tracker.print_summary(uncompressed_bytes=14_700_000)
        output = buf.getvalue()
        assert "Downloaded" in output
        assert "→" in output

    def test_print_summary_without_uncompressed(self) -> None:
        buf = StringIO()
        tracker = ProgressTracker(enabled=True)
        with tracker:
            tracker.update(4_200_000)
        with patch.object(sys, "stdout", buf):
            tracker.print_summary()
        output = buf.getvalue()
        assert "Downloaded" in output
        assert "→" not in output

    def test_disabled_no_output(self) -> None:
        buf = StringIO()
        with patch.object(sys, "stdout", buf):
            tracker = ProgressTracker(enabled=False)
            with tracker:
                tracker.update(1000)
            tracker.print_summary(uncompressed_bytes=2000)
        assert buf.getvalue() == ""

    def test_zero_bytes_no_summary(self) -> None:
        buf = StringIO()
        tracker = ProgressTracker(enabled=True)
        with tracker:
            pass  # no updates
        with patch.object(sys, "stdout", buf):
            tracker.print_summary(uncompressed_bytes=0)
        assert "Downloaded" not in buf.getvalue()
