#!/usr/bin/env python3
"""
Simulate N concurrent chat clients against a running server.

Usage:
  Easiest:  start test   (starts a temporary server and runs this)

  Manually: start the server, then run  python test_concurrent.py [host] [port] [num_clients]
            With no host, the server is located via LAN discovery (also tests it).
"""

from __future__ import annotations

import asyncio
import sys
import time

import config
from discovery import find_servers
from protocol import (
    AUTH_REQUEST,
    AUTH_RESPONSE,
    PRIVATE_MSG,
    PUBLIC_MSG,
    SYSTEM_MSG,
    read_message,
    write_message,
)


async def one_client(idx: int, num_clients: int, host: str, port: int, results: dict) -> None:
    username = f"bot_{idx}"
    password = "testpass1"
    received = 0
    pm_received = 0
    try:
        reader, writer = await asyncio.open_connection(host, port)
        # Drain welcome
        await read_message(reader)

        await write_message(
            writer,
            AUTH_REQUEST,
            {"action": "register", "username": username, "password": password},
        )
        await read_message(reader)  # register response (ok or already exists)

        await write_message(
            writer,
            AUTH_REQUEST,
            {"action": "login", "username": username, "password": password},
        )
        # AUTH_RESPONSE + HISTORY + maybe SYSTEM from others
        deadline = time.monotonic() + 5
        logged_in = False
        while time.monotonic() < deadline:
            msg = await asyncio.wait_for(read_message(reader), timeout=3)
            if msg and msg.get("type") == AUTH_RESPONSE and msg["payload"].get("success"):
                logged_in = True
                break
        if not logged_in:
            results[idx] = {"ok": False, "error": "login failed", "received": 0}
            writer.close()
            return

        # Send a few public messages
        for n in range(5):
            await write_message(
                writer,
                PUBLIC_MSG,
                {"content": f"hello from {username} #{n}"},
            )
            await asyncio.sleep(0.05)

        # Each bot sends a private message to the next one
        await asyncio.sleep(0.3)  # let every bot finish logging in
        await write_message(
            writer,
            PRIVATE_MSG,
            {"to": f"bot_{(idx + 1) % num_clients}", "content": f"psst from {username}"},
        )

        # Listen briefly for broadcasts
        end = time.monotonic() + 2.0
        while time.monotonic() < end:
            try:
                msg = await asyncio.wait_for(read_message(reader), timeout=0.3)
            except asyncio.TimeoutError:
                continue
            if msg and msg.get("type") == PUBLIC_MSG:
                received += 1
            elif msg and msg.get("type") == PRIVATE_MSG and not msg["payload"].get("echo"):
                pm_received += 1

        await write_message(writer, "CMD_REQUEST", {"command": "logout", "args": []})
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        if pm_received != 1:
            results[idx] = {"ok": False, "error": f"got {pm_received} private messages, expected 1",
                            "received": received}
        else:
            results[idx] = {"ok": True, "received": received}
    except Exception as exc:
        results[idx] = {"ok": False, "error": str(exc), "received": received}


async def main() -> None:
    n = int(sys.argv[3]) if len(sys.argv) > 3 else 5
    if len(sys.argv) > 1:
        host = sys.argv[1]
        port = int(sys.argv[2]) if len(sys.argv) > 2 else config.PORT
    else:
        servers = await asyncio.to_thread(find_servers)
        if not servers:
            print("FAIL: LAN discovery found no server.")
            sys.exit(1)
        host, port = servers[0]
        print(f"Discovery found server at {host}:{port}")

    print(f"Spawning {n} clients against {host}:{port} …")
    results: dict = {}
    await asyncio.gather(*(one_client(i, n, host, port, results) for i in range(n)))

    ok = sum(1 for r in results.values() if r.get("ok"))
    total_rx = sum(r.get("received", 0) for r in results.values())
    print(f"Done: {ok}/{n} clients succeeded, {total_rx} PUBLIC_MSG frames received.")
    for i in sorted(results):
        r = results[i]
        status = "OK" if r.get("ok") else f"FAIL ({r.get('error')})"
        print(f"  bot_{i}: {status}, rx={r.get('received', 0)}")
    if ok < n:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
