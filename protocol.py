"""Length-prefixed JSON framing and message helpers."""

from __future__ import annotations

import json
import struct
from typing import Any

from config import LENGTH_PREFIX_SIZE, MAX_MESSAGE_SIZE

# Message type constants
AUTH_REQUEST = "AUTH_REQUEST"
AUTH_RESPONSE = "AUTH_RESPONSE"
PUBLIC_MSG = "PUBLIC_MSG"
PRIVATE_MSG = "PRIVATE_MSG"
SYSTEM_MSG = "SYSTEM_MSG"
CMD_REQUEST = "CMD_REQUEST"
CMD_RESPONSE = "CMD_RESPONSE"
HISTORY_RESPONSE = "HISTORY_RESPONSE"
PING = "PING"
PONG = "PONG"


def encode_message(msg_type: str, payload: dict[str, Any] | None = None) -> bytes:
    """Serialize a message to length-prefixed UTF-8 JSON bytes."""
    body = json.dumps(
        {"type": msg_type, "payload": payload or {}},
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    if len(body) > MAX_MESSAGE_SIZE:
        raise ValueError(f"Message exceeds MAX_MESSAGE_SIZE ({MAX_MESSAGE_SIZE})")
    return struct.pack(">I", len(body)) + body


def decode_payload(data: bytes) -> dict[str, Any]:
    """Decode a UTF-8 JSON body into a dict. Raises ValueError on bad data."""
    try:
        obj = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Invalid JSON payload: {exc}") from exc
    if not isinstance(obj, dict):
        raise ValueError("Payload must be a JSON object")
    if "type" not in obj:
        raise ValueError("Missing message type")
    if "payload" not in obj or not isinstance(obj["payload"], dict):
        obj["payload"] = obj.get("payload") if isinstance(obj.get("payload"), dict) else {}
    return obj


async def read_message(reader) -> dict[str, Any] | None:
    """
    Read one framed message from an asyncio StreamReader.
    Returns None on clean EOF; raises on protocol/size errors.
    """
    header = await reader.readexactly(LENGTH_PREFIX_SIZE)
    (length,) = struct.unpack(">I", header)
    if length == 0 or length > MAX_MESSAGE_SIZE:
        raise ValueError(f"Invalid frame length: {length}")
    body = await reader.readexactly(length)
    return decode_payload(body)


async def write_message(writer, msg_type: str, payload: dict[str, Any] | None = None) -> None:
    """Encode and write a framed message, then drain."""
    writer.write(encode_message(msg_type, payload))
    await writer.drain()
