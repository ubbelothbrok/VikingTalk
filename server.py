"""Asyncio multi-client TCP chat server."""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import config
from database import Database, is_valid_name
from protocol import (
    AUTH_REQUEST,
    AUTH_RESPONSE,
    CMD_REQUEST,
    CMD_RESPONSE,
    HISTORY_RESPONSE,
    PING,
    PONG,
    PRIVATE_MSG,
    PUBLIC_MSG,
    SYSTEM_MSG,
    read_message,
    write_message,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("chat.server")


def _guess_lan_ip() -> str | None:
    """Best-effort LAN IP for connection hints (does not change bind address)."""
    import socket

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except OSError:
        return None


def _loop_time() -> float:
    return asyncio.get_running_loop().time()


@dataclass(eq=False)
class ClientSession:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    username: str | None = None
    channel: str = config.DEFAULT_CHANNEL
    last_activity: float = 0.0
    last_pong: float = 0.0
    addr: str = ""

    def __hash__(self) -> int:
        return id(self)

    def touch(self) -> None:
        now = _loop_time()
        self.last_activity = now
        self.last_pong = now


class ChatServer:
    def __init__(self, host: str = config.HOST, port: int = config.PORT) -> None:
        self.host = host
        self.port = port
        self.db = Database()
        # username -> ClientSession (logged-in only)
        self.sessions: dict[str, ClientSession] = {}
        # All TCP connections (authenticated or not)
        self.connections: set[ClientSession] = set()
        # channel -> set of usernames
        self.channels: dict[str, set[str]] = {config.DEFAULT_CHANNEL: set()}
        self._server: asyncio.Server | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_client, self.host, self.port
        )
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        addrs = ", ".join(str(s.getsockname()) for s in self._server.sockets or [])
        log.info("Chat server listening on %s", addrs)
        lan_hint = _guess_lan_ip()
        if self.host in ("0.0.0.0", "") and lan_hint:
            log.info(
                "Other devices on your Wi-Fi can connect with:  python client.py %s %s",
                lan_hint,
                self.port,
            )
        async with self._server:
            await self._server.serve_forever()

    async def shutdown(self) -> None:
        log.info("Shutting down…")
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        async with self._lock:
            sessions = list(self.sessions.values())
            self.sessions.clear()
            for ch in self.channels.values():
                ch.clear()

        for session in sessions:
            try:
                await write_message(
                    session.writer,
                    SYSTEM_MSG,
                    {"content": "Server is shutting down. Goodbye."},
                )
            except Exception:
                pass
            self._close_writer(session.writer)

        if self._server:
            self._server.close()
            await self._server.wait_closed()
        self.db.close()
        log.info("Shutdown complete.")

    # --- Connection lifecycle ---

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peer = writer.get_extra_info("peername")
        addr = f"{peer[0]}:{peer[1]}" if peer else "unknown"
        now = _loop_time()
        session = ClientSession(
            reader=reader,
            writer=writer,
            addr=addr,
            last_activity=now,
            last_pong=now,
        )
        async with self._lock:
            self.connections.add(session)
        log.info("Connection from %s", addr)

        try:
            await write_message(
                writer,
                SYSTEM_MSG,
                {
                    "content": (
                        "Welcome! Use /register <user> <pass> or /login <user> <pass>. "
                        "Type /help for commands."
                    )
                },
            )
            while True:
                try:
                    msg = await read_message(reader)
                except asyncio.IncompleteReadError:
                    break
                except ValueError as exc:
                    log.warning("Protocol error from %s: %s", addr, exc)
                    await self._send(
                        session,
                        SYSTEM_MSG,
                        {"content": f"Error: protocol violation ({exc})."},
                    )
                    break

                if msg is None:
                    break

                session.touch()
                await self._dispatch(session, msg)
        except (ConnectionResetError, BrokenPipeError, ConnectionError):
            log.info("Connection closed: %s", addr)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Unhandled error for %s", addr)
        finally:
            await self._cleanup_session(session)

    async def _cleanup_session(self, session: ClientSession) -> None:
        username = session.username
        channel = session.channel
        async with self._lock:
            self.connections.discard(session)
            if username:
                current = self.sessions.get(username)
                if current is session:
                    del self.sessions[username]
                    members = self.channels.get(channel)
                    if members and username in members:
                        members.discard(username)
                else:
                    username = None  # already logged out / replaced
        if username:
            await self._broadcast_channel(
                channel,
                SYSTEM_MSG,
                {"content": f"{username} left the chat."},
                exclude=username,
            )
            log.info("User '%s' disconnected (%s)", username, session.addr)
        self._close_writer(session.writer)

    @staticmethod
    def _close_writer(writer: asyncio.StreamWriter) -> None:
        try:
            writer.close()
        except Exception:
            pass

    # --- Dispatch ---

    async def _dispatch(self, session: ClientSession, msg: dict[str, Any]) -> None:
        msg_type = msg.get("type")
        payload = msg.get("payload") or {}

        if msg_type == PONG:
            session.last_pong = _loop_time()
            return

        if msg_type == PING:
            await self._send(session, PONG, {})
            return

        if msg_type == AUTH_REQUEST:
            await self._handle_auth(session, payload)
            return

        if msg_type == PUBLIC_MSG:
            await self._handle_public(session, payload)
            return

        if msg_type == PRIVATE_MSG:
            await self._handle_private(session, payload)
            return

        if msg_type == CMD_REQUEST:
            await self._handle_command(session, payload)
            return

        await self._send(
            session,
            SYSTEM_MSG,
            {"content": f"Error: unknown message type '{msg_type}'."},
        )

    # --- Auth ---

    async def _handle_auth(self, session: ClientSession, payload: dict[str, Any]) -> None:
        action = (payload.get("action") or "").lower()
        username = (payload.get("username") or "").strip()
        password = payload.get("password") or ""

        if action == "register":
            if not (config.PASSWORD_MIN_LEN <= len(password) <= config.PASSWORD_MAX_LEN):
                await self._send(
                    session,
                    AUTH_RESPONSE,
                    {
                        "success": False,
                        "message": (
                            f"Password must be {config.PASSWORD_MIN_LEN}-"
                            f"{config.PASSWORD_MAX_LEN} characters."
                        ),
                    },
                )
                return
            ok, message = await asyncio.to_thread(
                self.db.register_user, username, password
            )
            await self._send(
                session, AUTH_RESPONSE, {"success": ok, "message": message}
            )
            return

        if action == "login":
            if session.username:
                await self._send(
                    session,
                    AUTH_RESPONSE,
                    {"success": False, "message": "Already logged in. /logout first."},
                )
                return

            valid = await asyncio.to_thread(self.db.verify_user, username, password)
            if not valid:
                await self._send(
                    session,
                    AUTH_RESPONSE,
                    {"success": False, "message": "Invalid username or password."},
                )
                return

            canonical = await asyncio.to_thread(self.db.get_canonical_username, username)
            assert canonical is not None

            async with self._lock:
                if canonical in self.sessions:
                    await self._send(
                        session,
                        AUTH_RESPONSE,
                        {
                            "success": False,
                            "message": f"User '{canonical}' is already logged in.",
                        },
                    )
                    return
                session.username = canonical
                session.channel = config.DEFAULT_CHANNEL
                self.sessions[canonical] = session
                self.channels.setdefault(config.DEFAULT_CHANNEL, set()).add(canonical)

            await self._send(
                session,
                AUTH_RESPONSE,
                {
                    "success": True,
                    "message": f"Welcome, {canonical}!",
                    "username": canonical,
                    "channel": config.DEFAULT_CHANNEL,
                },
            )
            await self._send_history(session, config.DEFAULT_CHANNEL)
            await self._broadcast_channel(
                config.DEFAULT_CHANNEL,
                SYSTEM_MSG,
                {"content": f"{canonical} joined the chat."},
                exclude=canonical,
            )
            log.info("User '%s' logged in from %s", canonical, session.addr)
            return

        await self._send(
            session,
            AUTH_RESPONSE,
            {"success": False, "message": f"Unknown auth action '{action}'."},
        )

    # --- Messaging ---

    async def _handle_public(
        self, session: ClientSession, payload: dict[str, Any]
    ) -> None:
        if not session.username:
            await self._send(
                session, SYSTEM_MSG, {"content": "Error: you must /login first."}
            )
            return

        content = (payload.get("content") or "").strip()
        if not content:
            return
        if len(content) > config.MESSAGE_MAX_LEN:
            await self._send(
                session,
                SYSTEM_MSG,
                {"content": f"Error: message too long (max {config.MESSAGE_MAX_LEN})."},
            )
            return

        saved = await asyncio.to_thread(
            self.db.save_message, session.channel, session.username, content
        )
        await self._broadcast_channel(
            session.channel,
            PUBLIC_MSG,
            {
                "channel": session.channel,
                "username": session.username,
                "content": content,
                "timestamp": saved["timestamp"],
            },
        )

    async def _handle_private(
        self, session: ClientSession, payload: dict[str, Any]
    ) -> None:
        if not session.username:
            await self._send(
                session, SYSTEM_MSG, {"content": "Error: you must /login first."}
            )
            return

        target = (payload.get("to") or "").strip()
        content = (payload.get("content") or "").strip()
        if not target or not content:
            await self._send(
                session,
                SYSTEM_MSG,
                {"content": "Error: usage /msg <username> <message>"},
            )
            return
        if len(content) > config.MESSAGE_MAX_LEN:
            await self._send(
                session,
                SYSTEM_MSG,
                {"content": f"Error: message too long (max {config.MESSAGE_MAX_LEN})."},
            )
            return

        async with self._lock:
            dest = self.sessions.get(target)
            # case-insensitive lookup
            if dest is None:
                lower = target.lower()
                for name, sess in self.sessions.items():
                    if name.lower() == lower:
                        dest = sess
                        target = name
                        break

        if dest is None or dest.username is None:
            await self._send(
                session,
                SYSTEM_MSG,
                {"content": f"Error: User '{target}' not found."},
            )
            return

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        envelope = {
            "from": session.username,
            "to": target,
            "content": content,
            "timestamp": ts,
        }
        await self._send(dest, PRIVATE_MSG, envelope)
        # Echo confirmation to sender
        await self._send(session, PRIVATE_MSG, {**envelope, "echo": True})

    # --- Commands ---

    async def _handle_command(
        self, session: ClientSession, payload: dict[str, Any]
    ) -> None:
        command = (payload.get("command") or "").lower().lstrip("/")
        args = payload.get("args") or []
        if not isinstance(args, list):
            args = []

        handlers = {
            "logout": self._cmd_logout,
            "create": self._cmd_create,
            "join": self._cmd_join,
            "leave": self._cmd_leave,
            "channels": self._cmd_channels,
            "users": self._cmd_users,
            "history": self._cmd_history,
            "help": self._cmd_help,
            "msg": self._cmd_msg,
        }
        handler = handlers.get(command)
        if handler is None:
            await self._send(
                session,
                SYSTEM_MSG,
                {"content": f"Error: unknown command '/{command}'. Try /help."},
            )
            return
        await handler(session, args)

    async def _require_login(self, session: ClientSession) -> bool:
        if session.username:
            return True
        await self._send(
            session, SYSTEM_MSG, {"content": "Error: you must /login first."}
        )
        return False

    async def _cmd_logout(self, session: ClientSession, args: list[str]) -> None:
        if not session.username:
            await self._send(session, SYSTEM_MSG, {"content": "Not logged in."})
            return
        username = session.username
        channel = session.channel
        async with self._lock:
            self.sessions.pop(username, None)
            members = self.channels.get(channel)
            if members:
                members.discard(username)
        session.username = None
        session.channel = config.DEFAULT_CHANNEL
        await self._send(
            session, SYSTEM_MSG, {"content": "You have been logged out."}
        )
        await self._broadcast_channel(
            channel,
            SYSTEM_MSG,
            {"content": f"{username} left the chat."},
        )
        log.info("User '%s' logged out", username)

    async def _cmd_create(self, session: ClientSession, args: list[str]) -> None:
        if not await self._require_login(session):
            return
        if len(args) != 1:
            await self._send(
                session, SYSTEM_MSG, {"content": "Error: usage /create <channel_name>"}
            )
            return
        name = args[0]
        ok, message = await asyncio.to_thread(self.db.ensure_channel, name)
        if ok:
            async with self._lock:
                canonical = await asyncio.to_thread(self.db.get_canonical_channel, name)
                assert canonical is not None
                self.channels.setdefault(canonical, set())
            await self._send(session, SYSTEM_MSG, {"content": message})
        else:
            await self._send(session, SYSTEM_MSG, {"content": f"Error: {message}"})

    async def _cmd_join(self, session: ClientSession, args: list[str]) -> None:
        if not await self._require_login(session):
            return
        if len(args) != 1:
            await self._send(
                session, SYSTEM_MSG, {"content": "Error: usage /join <channel_name>"}
            )
            return
        name = args[0]
        if not is_valid_name(name):
            await self._send(
                session,
                SYSTEM_MSG,
                {"content": "Error: invalid channel name."},
            )
            return

        canonical = await asyncio.to_thread(self.db.get_canonical_channel, name)
        if canonical is None:
            await self._send(
                session,
                SYSTEM_MSG,
                {"content": f"Error: channel '{name}' does not exist. Use /create first."},
            )
            return

        assert session.username is not None
        old = session.channel
        if old == canonical:
            await self._send(
                session, SYSTEM_MSG, {"content": f"Already in #{canonical}."}
            )
            return

        async with self._lock:
            if old in self.channels:
                self.channels[old].discard(session.username)
            self.channels.setdefault(canonical, set()).add(session.username)
            session.channel = canonical

        await self._broadcast_channel(
            old,
            SYSTEM_MSG,
            {"content": f"{session.username} left #{old}."},
            exclude=session.username,
        )
        await self._send(
            session,
            SYSTEM_MSG,
            {"content": f"Joined #{canonical}.", "channel": canonical},
        )
        await self._send_history(session, canonical)
        await self._broadcast_channel(
            canonical,
            SYSTEM_MSG,
            {"content": f"{session.username} joined #{canonical}."},
            exclude=session.username,
        )

    async def _cmd_leave(self, session: ClientSession, args: list[str]) -> None:
        if not await self._require_login(session):
            return
        if session.channel == config.DEFAULT_CHANNEL:
            await self._send(
                session,
                SYSTEM_MSG,
                {"content": "Already in #general. Nothing to leave."},
            )
            return
        await self._cmd_join(session, [config.DEFAULT_CHANNEL])

    async def _cmd_channels(self, session: ClientSession, args: list[str]) -> None:
        if not await self._require_login(session):
            return
        db_channels = await asyncio.to_thread(self.db.list_channels)
        async with self._lock:
            lines = []
            for name in db_channels:
                count = len(self.channels.get(name, set()))
                marker = " *" if name == session.channel else ""
                lines.append(f"  #{name} ({count} online){marker}")
                self.channels.setdefault(name, set())
        body = "Active channels:\n" + ("\n".join(lines) if lines else "  (none)")
        await self._send(session, CMD_RESPONSE, {"command": "channels", "content": body})

    async def _cmd_users(self, session: ClientSession, args: list[str]) -> None:
        if not await self._require_login(session):
            return
        async with self._lock:
            if not self.sessions:
                body = "No users online."
            else:
                lines = [
                    f"  @{name} in #{sess.channel}"
                    for name, sess in sorted(self.sessions.items())
                ]
                body = "Online users:\n" + "\n".join(lines)
        await self._send(session, CMD_RESPONSE, {"command": "users", "content": body})

    async def _cmd_history(self, session: ClientSession, args: list[str]) -> None:
        if not await self._require_login(session):
            return
        limit = config.HISTORY_DEFAULT_LIMIT
        if args:
            try:
                limit = int(args[0])
            except ValueError:
                await self._send(
                    session,
                    SYSTEM_MSG,
                    {"content": "Error: usage /history [N]"},
                )
                return
        await self._send_history(session, session.channel, limit)

    async def _cmd_help(self, session: ClientSession, args: list[str]) -> None:
        text = (
            "Commands:\n"
            "  /register <user> <pass>  Register a new account\n"
            "  /login <user> <pass>     Log in\n"
            "  /logout                  Log out\n"
            "  /msg <user> <text>       Private message\n"
            "  /create <channel>        Create a channel\n"
            "  /join <channel>          Switch channel\n"
            "  /leave                   Return to #general\n"
            "  /channels                List channels\n"
            "  /users                   List online users\n"
            "  /history [N]             Show last N messages\n"
            "  /quit                    Exit the client\n"
            "  /help                    Show this help"
        )
        await self._send(session, SYSTEM_MSG, {"content": text})

    async def _cmd_msg(self, session: ClientSession, args: list[str]) -> None:
        if len(args) < 2:
            await self._send(
                session,
                SYSTEM_MSG,
                {"content": "Error: usage /msg <username> <message>"},
            )
            return
        target, content = args[0], " ".join(args[1:])
        await self._handle_private(session, {"to": target, "content": content})

    # --- Helpers ---

    async def _send_history(
        self,
        session: ClientSession,
        channel: str,
        limit: int = config.HISTORY_DEFAULT_LIMIT,
    ) -> None:
        messages = await asyncio.to_thread(self.db.get_history, channel, limit)
        await self._send(
            session,
            HISTORY_RESPONSE,
            {"channel": channel, "messages": messages},
        )

    async def _send(
        self, session: ClientSession, msg_type: str, payload: dict[str, Any]
    ) -> None:
        try:
            await write_message(session.writer, msg_type, payload)
        except (ConnectionResetError, BrokenPipeError, ConnectionError):
            pass

    async def _broadcast_channel(
        self,
        channel: str,
        msg_type: str,
        payload: dict[str, Any],
        exclude: str | None = None,
    ) -> None:
        async with self._lock:
            members = list(self.channels.get(channel, set()))
            targets = [
                self.sessions[u]
                for u in members
                if u in self.sessions and u != exclude
            ]
        for sess in targets:
            await self._send(sess, msg_type, payload)

    async def _heartbeat_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(config.HEARTBEAT_INTERVAL)
                now = _loop_time()
                async with self._lock:
                    sessions = list(self.connections)
                stale: list[ClientSession] = []
                for sess in sessions:
                    if now - sess.last_pong > config.HEARTBEAT_TIMEOUT:
                        stale.append(sess)
                        continue
                    try:
                        await write_message(sess.writer, PING, {})
                    except Exception:
                        stale.append(sess)
                for sess in stale:
                    log.info(
                        "Heartbeat timeout for %s (%s)",
                        sess.username or "anon",
                        sess.addr,
                    )
                    self._close_writer(sess.writer)
        except asyncio.CancelledError:
            raise


async def main() -> None:
    host = config.HOST
    port = config.PORT
    if len(sys.argv) >= 2:
        host = sys.argv[1]
    if len(sys.argv) >= 3:
        port = int(sys.argv[2])

    server = ChatServer(host, port)
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()

    def _signal_handler() -> None:
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # Windows
            signal.signal(sig, lambda *_: stop.set())

    serve_task = asyncio.create_task(server.start())
    await stop.wait()
    serve_task.cancel()
    try:
        await serve_task
    except asyncio.CancelledError:
        pass
    await server.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
