#!/usr/bin/env python3
"""
Exercise the peer-to-peer file transfer path without a chat server.

Usage:  python test_filetransfer.py
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import shutil
import sys
import tempfile

import config
from filetransfer import (
    IncomingTransfer,
    TransferError,
    human_size,
    new_token,
    sanitize_filename,
    send_file,
    unique_path,
)
from protocol import FILE_DATA_HELLO, write_message

PASSED: list[str] = []
FAILED: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    (PASSED if ok else FAILED).append(name)
    mark = "ok  " if ok else "FAIL"
    print(f"  [{mark}] {name}{(' -- ' + detail) if detail and not ok else ''}")


def make_file(path: str, size: int) -> str:
    with open(path, "wb") as fh:
        remaining = size
        block = os.urandom(min(size, 1 << 20)) or b"\0"
        while remaining > 0:
            fh.write(block[:remaining])
            remaining -= min(remaining, len(block))
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


async def run_transfer(tmp: str, src: str, size: int, dest_dir: str, filename: str):
    """Receive `src` into `dest_dir`; returns (ok, message, path)."""
    outcome: dict = {}
    done = asyncio.Event()

    def on_done(ok: bool, message: str, path: str | None) -> None:
        outcome.update(ok=ok, message=message, path=path)
        done.set()

    incoming = IncomingTransfer("abc12345", "alice", filename, size, dest_dir)
    port = await incoming.start(on_done=on_done)
    sender = asyncio.create_task(
        send_file("127.0.0.1", port, "abc12345", incoming.token, src, size)
    )
    await asyncio.wait_for(done.wait(), 60)
    try:
        await sender
    except (TransferError, OSError):
        pass
    return outcome.get("ok"), outcome.get("message"), outcome.get("path")


async def main() -> int:
    tmp = tempfile.mkdtemp(prefix="vt-ft-")
    dest = os.path.join(tmp, "downloads")
    try:
        print("Pure functions:")
        check("sanitize strips directories", sanitize_filename("../../etc/passwd") == "passwd")
        check("sanitize strips separators", sanitize_filename(r"C:\win\evil.exe") == "evil.exe")
        check("sanitize rejects dot names", sanitize_filename("..") == "")
        check("sanitize rejects empty", sanitize_filename("   ") == "")
        check("sanitize caps length",
              len(sanitize_filename("x" * 500 + ".iso")) <= config.FILENAME_MAX_LEN)
        check("human_size", human_size(1536) == "1.5 KB" and human_size(900) == "900 B",
              human_size(1536))

        os.makedirs(dest, exist_ok=True)
        open(os.path.join(dest, "a.txt"), "w").close()
        check("unique_path avoids collisions",
              os.path.basename(unique_path(dest, "a.txt")) == "a (1).txt")

        print("\nTransfers:")
        # A file several chunks long, so the chunk loop really loops.
        size = config.FILE_CHUNK_SIZE * 3 + 777
        src = os.path.join(tmp, "payload.bin")
        src_sha = make_file(src, size)
        ok, message, path = await run_transfer(tmp, src, size, dest, "payload.bin")
        received_sha = hashlib.sha256(open(path, "rb").read()).hexdigest() if path else ""
        check("multi-chunk transfer succeeds", bool(ok), str(message))
        check("received bytes are identical", received_sha == src_sha)
        check("no .part file left behind",
              not any(f.endswith(".part") for f in os.listdir(dest)))

        # Same name again -> suffixed, original untouched
        ok2, _, path2 = await run_transfer(tmp, src, size, dest, "payload.bin")
        check("second copy is renamed",
              bool(ok2) and os.path.basename(path2 or "") == "payload (1).bin")

        print("\nRejections:")
        # Wrong token: the listener must ignore the connection and stay open
        incoming = IncomingTransfer("abc12345", "alice", "payload.bin", size, dest)
        port = await incoming.start()
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        await write_message(
            writer, FILE_DATA_HELLO,
            {"transfer_id": "abc12345", "token": new_token(), "size": size},
        )
        closed = not await reader.read(1)  # EOF, no READY frame
        writer.close()
        check("wrong token is refused", closed)
        # ...and the right peer can still get through afterwards
        good = await asyncio.wait_for(
            send_file("127.0.0.1", port, "abc12345", incoming.token, src, size), 60
        )
        check("listener survives a bad peer", good == src_sha)
        await incoming.close()

        # Truncated stream -> failure reported, nothing left on disk
        before = set(os.listdir(dest))
        incoming = IncomingTransfer("abc12345", "alice", "trunc.bin", size * 4, dest)
        outcome: dict = {}
        done = asyncio.Event()
        port = await incoming.start(
            on_done=lambda ok, m, p: (outcome.update(ok=ok, message=m), done.set())
        )
        try:
            # Claim a much larger size than we will actually send
            await send_file("127.0.0.1", port, "abc12345", incoming.token, src, size * 4)
        except (TransferError, OSError):
            pass
        await asyncio.wait_for(done.wait(), 30)
        check("short stream is rejected", outcome.get("ok") is False, str(outcome))
        check("failed transfer leaves no file", set(os.listdir(dest)) == before)

        # Nobody connects -> the port closes itself
        original_timeout = config.TRANSFER_CONNECT_TIMEOUT
        config.TRANSFER_CONNECT_TIMEOUT = 1
        try:
            incoming = IncomingTransfer("abc12345", "alice", "nope.bin", size, dest)
            done = asyncio.Event()
            expired: dict = {}
            await incoming.start(
                on_done=lambda ok, m, p: (expired.update(ok=ok, message=m), done.set())
            )
            await asyncio.wait_for(done.wait(), 10)
            check("unanswered offer times out", expired.get("ok") is False,
                  str(expired))
        finally:
            config.TRANSFER_CONNECT_TIMEOUT = original_timeout

        # Unreachable peer
        try:
            await send_file("127.0.0.1", 1, "abc12345", "tok", src, size)
            check("unreachable peer raises", False)
        except (TransferError, OSError):
            check("unreachable peer raises", True)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print(f"\n{len(PASSED)} passed, {len(FAILED)} failed")
    if FAILED:
        print("Failed: " + ", ".join(FAILED))
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
