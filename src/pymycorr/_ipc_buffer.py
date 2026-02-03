"""IPC stream buffer for parsing Arrow data from HTTP chunks."""

from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa
import pyarrow.ipc as ipc

from pymycorr.exceptions import StreamingError


class _IPCStreamBuffer:
    """Accumulates HTTP chunks and yields complete IPC streams.

    The server sends multiple complete Arrow IPC streams (one per batch),
    each terminated by an EOS marker. This class buffers incoming HTTP chunks
    and yields complete IPC streams as they are detected.

    Note: The EOS marker bytes could appear within Arrow data, so we validate
    each candidate stream can actually be parsed before yielding it.
    """

    EOS_MARKER = b"\xff\xff\xff\xff\x00\x00\x00\x00"
    EOS_LEN = 8

    def __init__(self, max_buffer_size: int | None = None) -> None:
        """Initialize the buffer.

        Args:
            max_buffer_size: Maximum buffer size in bytes before raising error.
                None for unlimited.
        """
        self._buffer = bytearray()
        self._max_buffer_size = max_buffer_size
        self.total_bytes = 0
        self.streams_parsed = 0

    def _is_valid_ipc_stream(self, data: bytes) -> bool:
        """Check if data is a valid, complete IPC stream."""
        try:
            reader = ipc.open_stream(data)
            reader.read_all()  # Validates the entire stream
            return True
        except (pa.ArrowInvalid, pa.ArrowIOError):
            return False

    def add_chunk(self, chunk: bytes) -> Iterator[bytes]:
        """Add HTTP chunk and yield any complete IPC streams found.

        Args:
            chunk: Raw bytes from HTTP response.

        Yields:
            Complete IPC stream bytes (including EOS marker).

        Raises:
            StreamingError: If buffer exceeds max_buffer_size.
        """
        self._buffer.extend(chunk)
        self.total_bytes += len(chunk)

        if self._max_buffer_size and len(self._buffer) > self._max_buffer_size:
            raise StreamingError(
                f"Buffer exceeded {self._max_buffer_size} bytes - possible malformed stream"
            )

        # Search for EOS markers, but validate each candidate is a real IPC stream
        search_start = 0
        while True:
            eos_pos = self._buffer.find(self.EOS_MARKER, search_start)
            if eos_pos == -1:
                break

            stream_end = eos_pos + self.EOS_LEN
            candidate_stream = bytes(self._buffer[:stream_end])

            if self._is_valid_ipc_stream(candidate_stream):
                # Valid stream found - yield it and remove from buffer
                del self._buffer[:stream_end]
                self.streams_parsed += 1
                yield candidate_stream
                search_start = 0  # Reset search for next stream
            else:
                # False positive - EOS marker bytes appeared in data
                # Continue searching after this position
                search_start = eos_pos + 1

    def remaining(self) -> bytes:
        """Return any leftover buffer data after stream ends."""
        return bytes(self._buffer)
