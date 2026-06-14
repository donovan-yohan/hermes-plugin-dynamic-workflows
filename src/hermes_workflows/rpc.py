"""Narrow newline-framed JSON RPC channel between parent and subprocess VM.

The parent Hermes plugin (:mod:`hermes_workflows.vm`) and the sandboxed guest
(:mod:`hermes_workflows.vm_guest`) speak this one tiny protocol over the child's
stdio. There is no other channel: the subprocess has no network, no shared
memory, and a scrubbed environment, so every capability the script reaches for
crosses this surface and is brokered by the parent.

Wire format
-----------
Each frame is exactly one line of UTF-8 JSON terminated by ``\\n``. JSON values
never contain a literal newline (``separators`` are compact and the encoder does
not pretty-print), so line framing is unambiguous. Frames carry a ``t`` (type)
tag:

Parent -> child (written to the child's stdin):
    ``{"t":"boot","script":<str>,"args":<any>,"limits":<obj>,"budget":<obj>}``
    ``{"t":"ret","id":<int>,"ok":true,"value":<any>,"budget":<obj>}``
    ``{"t":"ret","id":<int>,"ok":false,"error":<obj>,"budget":<obj>}``

Child -> parent (written to the child's stdout):
    ``{"t":"ready","meta":<obj>}``
    ``{"t":"call","id":<int>,"method":<str>,"params":<obj>}``
    ``{"t":"done","ok":true,"value":<any>}``
    ``{"t":"done","ok":false,"error":<obj>}``

This module is pure stdlib and stateless beyond the wrapped streams.
"""

from __future__ import annotations

import json
from typing import Any, IO, Optional

__all__ = [
    "RPCProtocolError",
    "encode_frame",
    "write_frame",
    "read_frame",
    "Channel",
    "T_BOOT",
    "T_RET",
    "T_READY",
    "T_CALL",
    "T_DONE",
]

# Frame type tags.
T_BOOT = "boot"
T_RET = "ret"
T_READY = "ready"
T_CALL = "call"
T_DONE = "done"


class RPCProtocolError(Exception):
    """Raised when a frame cannot be decoded or violates the wire contract."""


def encode_frame(obj: dict[str, Any]) -> str:
    """Encode a frame dict into a single newline-free JSON line (no trailing \\n)."""
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def write_frame(stream: IO[str], obj: dict[str, Any]) -> None:
    """Write one frame to a text stream and flush it.

    Flushing on every frame is required: the protocol is strictly
    request/response and a buffered, unflushed frame would deadlock both sides.
    """
    stream.write(encode_frame(obj))
    stream.write("\n")
    stream.flush()


def read_frame(stream: IO[str]) -> Optional[dict[str, Any]]:
    """Read one frame from a text stream.

    Returns the decoded dict, or ``None`` at clean end-of-stream (peer closed
    the pipe). Raises :class:`RPCProtocolError` on a partial line, malformed
    JSON, or a non-object payload — the caller treats that as a protocol crash.
    """
    line = stream.readline()
    if line == "":
        return None  # EOF.
    if not line.endswith("\n"):
        # A line without a terminating newline means the peer died mid-frame.
        raise RPCProtocolError("truncated frame (no newline; peer closed mid-write)")
    text = line.strip()
    if not text:
        raise RPCProtocolError("empty frame")
    try:
        obj = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RPCProtocolError(f"malformed frame JSON: {exc.msg}") from exc
    if not isinstance(obj, dict):
        raise RPCProtocolError(f"frame must be a JSON object, got {type(obj).__name__}")
    if not isinstance(obj.get("t"), str):
        raise RPCProtocolError("frame missing string 't' (type) tag")
    return obj


class Channel:
    """A bidirectional frame channel over a readable and a writable text stream.

    Used by the guest (read=stdin, write=the saved real stdout) and available to
    the parent for symmetric testing. Reads and writes are independent; the
    object holds no protocol state of its own.
    """

    def __init__(self, reader: IO[str], writer: IO[str]) -> None:
        self._reader = reader
        self._writer = writer

    def send(self, obj: dict[str, Any]) -> None:
        write_frame(self._writer, obj)

    def recv(self) -> Optional[dict[str, Any]]:
        return read_frame(self._reader)
