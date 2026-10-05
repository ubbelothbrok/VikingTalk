"""Direct peer-to-peer file transfer over the LAN.

The chat server only brokers the offer/accept handshake: it tells the two
clients about each other. The bytes themselves travel straight between the
peers on a short-lived TCP connection, so a multi-gigabyte file never passes
through the server and never has to fit in a 64 KiB JSON frame.

Wire format on that direct socket:

    sender -> receiver   FILE_DATA_HELLO   {transfer_id, token, size}   (framed)
    receiver -> sender   FILE_DATA_READY   {}                           (framed)
    sender -> receiver   exactly `size` raw bytes
    sender -> receiver   FILE_DATA_TRAILER {sha256}                     (framed)

The digest travels in a trailer rather than the offer so the sender reads the
file only once -- hashing happens as the bytes go out.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import re
import secrets
import uuid
from typing import Callable

import config
from protocol import (
    FILE_DATA_HELLO,
    FILE_DATA_READY,
    FILE_DATA_TRAILER,
    read_message,
    write_message,
)

# A transfer id is only an opaque handle users type; keep it short and safe.
TRANSFER_ID_RE = re.compile(r"^[0-9a-f]{4,32}$")
_UNSAFE_CHARS_RE = re.compile(r'[\x00-\x1f<>:"/\\|?*]')

ProgressCb = Callable[[int, int], None]


def new_transfer_id() -> str:
    return uuid.uuid4().hex[:8]


def new_token() -> str:
    return secrets.token_hex(16)


def human_size(num: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(num) < 1024 or unit == "TB":
            return f"{num:.0f} {unit}" if unit == "B" else f"{num:.1f} {unit}"
        num /= 1024
    return f"{num:.1f} TB"


def sanitize_filename(name: str) -> str:
    """
    Reduce an arbitrary name to a bare, safe basename.

    A peer controls this string, so it must not be able to escape the download
    directory or name a device. Returns "" if nothing usable is left.
    """
    name = os.path.basename(name.replace("\\", "/")).strip()
    name = _UNSAFE_CHARS_RE.sub("_", name).strip(". ")
    if name in ("", ".", ".."):
        return ""
    if len(name) > config.FILENAME_MAX_LEN:
        stem, ext = os.path.splitext(name)
        ext = ext[: config.FILENAME_MAX_LEN]
        name = stem[: config.FILENAME_MAX_LEN - len(ext)] + ext
    return name


def unique_path(directory: str, filename: str) -> str:
    """A path in `directory` that does not exist yet: 'a.iso', 'a (1).iso', ..."""
    stem, ext = os.path.splitext(filename)
    candidate = os.path.join(directory, filename)
    counter = 1
    while os.path.exists(candidate):
        candidate = os.path.join(directory, f"{stem} ({counter}){ext}")
        counter += 1
    return candidate


class TransferError(Exception):
    """A transfer failed for a reason worth showing the user."""


# --- Sending side -----------------------------------------------------------


async def send_file(
    host: str,
    port: int,
    transfer_id: str,
    token: str,
    path: str,
    size: int,
    progress: ProgressCb | None = None,
) -> str:
    """
    Connect to a receiver that is already listening and stream `path` to it.

    Returns the sha256 hex digest of what was sent.
    Raises TransferError (or OSError) if the transfer could not complete.
    """
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), config.TRANSFER_CONNECT_TIMEOUT
        )
    except asyncio.TimeoutError as exc:
        raise TransferError(f"timed out connecting to {host}:{port}") from exc
    except OSError as exc:
        raise TransferError(f"cannot reach {host}:{port} ({exc})") from exc

    try:
        await write_message(
            writer,
            FILE_DATA_HELLO,
            {"transfer_id": transfer_id, "token": token, "size": size},
        )
        try:
            reply = await asyncio.wait_for(
                read_message(reader), config.TRANSFER_CONNECT_TIMEOUT
            )
        except (asyncio.TimeoutError, asyncio.IncompleteReadError) as exc:
            raise TransferError("receiver did not acknowledge the transfer") from exc
        if not reply or reply.get("type") != FILE_DATA_READY:
            raise TransferError("receiver rejected the transfer")

        digest = hashlib.sha256()
        sent = 0
        with open(path, "rb") as fh:
            while sent < size:
                chunk = fh.read(min(config.FILE_CHUNK_SIZE, size - sent))
                if not chunk:
                    break
                digest.update(chunk)
                writer.write(chunk)
                await writer.drain()
                sent += len(chunk)
                if progress:
                    progress(sent, size)
        if sent != size:
            # The file shrank underneath us; closing without a trailer makes
            # the receiver report an incomplete transfer rather than a bad one.
            raise TransferError(
                f"file changed while sending ({human_size(sent)} of {human_size(size)})"
            )

        await write_message(writer, FILE_DATA_TRAILER, {"sha256": digest.hexdigest()})
        return digest.hexdigest()
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


# --- Receiving side ---------------------------------------------------------


class IncomingTransfer:
    """
    A one-shot TCP listener that accepts a single file from one known peer.

    The receiver listens (rather than the sender) so the port only exists
    after the user has explicitly accepted, and closes again the moment the
    transfer ends or the wait times out.
    """

    def __init__(
        self,
        transfer_id: str,
        sender: str,
        filename: str,
        size: int,
        dest_dir: str = config.DOWNLOADS_DIR,
    ) -> None:
        self.transfer_id = transfer_id
        self.sender = sender
        self.filename = filename
        self.size = size
        self.dest_dir = dest_dir
        self.token = new_token()
        self.port = 0
        self.path: str | None = None
        self._server: asyncio.Server | None = None
        self._claimed = False
        self._timeout_task: asyncio.Task | None = None
        self._on_progress: ProgressCb | None = None
        self._on_done: Callable[[bool, str, str | None], None] | None = None

    async def start(
        self,
        on_progress: ProgressCb | None = None,
        on_done: Callable[[bool, str, str | None], None] | None = None,
    ) -> int:
        """Bind an ephemeral port and return it. Call before telling the sender."""
        self._on_progress = on_progress
        self._on_done = on_done
        os.makedirs(self.dest_dir, exist_ok=True)
        self._server = await asyncio.start_server(self._handle, "0.0.0.0", 0)
        self.port = self._server.sockets[0].getsockname()[1]
        self._timeout_task = asyncio.create_task(self._expire())
        return self.port

    async def close(self) -> None:
        if self._timeout_task:
            self._timeout_task.cancel()
            self._timeout_task = None
        if self._server is not None:
            self._server.close()
            try:
                await self._server.wait_closed()
            except Exception:
                pass
            self._server = None

    async def _expire(self) -> None:
        try:
            await asyncio.sleep(config.TRANSFER_CONNECT_TIMEOUT)
        except asyncio.CancelledError:
            return
        if self._claimed:
            return
        await self.close()
        self._finish(False, f"@{self.sender} never connected; transfer cancelled.", None)

    def _finish(self, ok: bool, message: str, path: str | None) -> None:
        callback, self._on_done = self._on_done, None
        if callback is not None:
            callback(ok, message, path)

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Gate every connection on the handshake before committing to it."""
        if self._claimed or not await self._authenticate(reader, writer):
            # Wrong peer (or a stray port scan): drop just this connection and
            # keep listening for the real sender.
            try:
                writer.close()
            except Exception:
                pass
            return

        self._claimed = True
        if self._timeout_task:
            self._timeout_task.cancel()
            self._timeout_task = None
        try:
            await self._receive(reader, writer)
        finally:
            try:
                writer.close()
            except Exception:
                pass
            await self.close()

    async def _authenticate(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> bool:
        try:
            hello = await asyncio.wait_for(read_message(reader), 30)
        except (asyncio.TimeoutError, asyncio.IncompleteReadError, ValueError):
            return False
        payload = (hello or {}).get("payload") or {}
        return (
            (hello or {}).get("type") == FILE_DATA_HELLO
            and payload.get("transfer_id") == self.transfer_id
            and secrets.compare_digest(str(payload.get("token") or ""), self.token)
        )

    async def _receive(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        part_path: str | None = None
        try:
            os.makedirs(self.dest_dir, exist_ok=True)
            final_path = unique_path(self.dest_dir, self.filename)
            part_path = final_path + ".part"
            self.path = final_path

            await write_message(writer, FILE_DATA_READY, {})

            digest = hashlib.sha256()
            received = 0
            with open(part_path, "wb") as fh:
                while received < self.size:
                    want = min(config.FILE_CHUNK_SIZE, self.size - received)
                    chunk = await reader.readexactly(want)
                    digest.update(chunk)
                    fh.write(chunk)
                    received += len(chunk)
                    if self._on_progress:
                        self._on_progress(received, self.size)

            trailer = await asyncio.wait_for(read_message(reader), 30)
            expected = ((trailer or {}).get("payload") or {}).get("sha256")
            if (trailer or {}).get("type") != FILE_DATA_TRAILER or not expected:
                raise TransferError("sender did not send a checksum")
            if not secrets.compare_digest(str(expected), digest.hexdigest()):
                raise TransferError("checksum mismatch -- the file is corrupt")

            os.replace(part_path, final_path)
            part_path = None
            self._finish(
                True,
                f"Received '{os.path.basename(final_path)}' "
                f"({human_size(self.size)}) from @{self.sender}.",
                final_path,
            )
        except (asyncio.IncompleteReadError, asyncio.TimeoutError):
            self._finish(False, "Transfer ended early; the sender disconnected.", None)
        except (TransferError, OSError, ValueError) as exc:
            self._finish(False, f"Transfer failed: {exc}", None)
        finally:
            if part_path and os.path.exists(part_path):
                try:
                    os.remove(part_path)
                except OSError:
                    pass
