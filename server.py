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
from discovery import guess_lan_ip, start_responder
from filetransfer import TRANSFER_ID_RE, human_size, sanitize_filename
from protocol import (
    AUTH_REQUEST,
    AUTH_RESPONSE,
    CMD_REQUEST,
    CMD_RESPONSE,
    FILE_ACCEPT,
    FILE_DECLINE,
    FILE_OFFER,
    FILE_RESULT,
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
    peer_ip: str = ""

    def __hash__(self) -> int:
        return id(self)

    def touch(self) -> None:
        now = _loop_time()
        self.last_activity = now
        self.last_pong = now


@dataclass
class FileOffer:
    """A pending peer-to-peer transfer the server is brokering."""

    transfer_id: str
    sender: str
    recipient: str
    filename: str
    size: int
    created: float


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
        # transfer_id -> FileOffer (brokered, never carries file bytes)
        self.offers: dict[str, FileOffer] = {}
        self._server: asyncio.Server | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._discovery = None
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_client, self.host, self.port
        )
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        addrs = ", ".join(str(s.getsockname()) for s in self._server.sockets or [])
        log.info("Chat server listening on %s", addrs)
        try:
            self._discovery = await start_responder(self.port, self.host)
            log.info("LAN discovery enabled on UDP port %s", config.DISCOVERY_PORT)
        except OSError as exc:
            log.warning(
                "LAN discovery unavailable (%s); clients must enter the IP manually.", exc
            )
        lan_hint = guess_lan_ip()
        if self.host in ("0.0.0.0", "") and lan_hint:
            log.info("Server LAN IP: %s  (port %s)", lan_hint, self.port)
            log.info(
                "Other devices on the same network: run the start script and pick "
                "'Join', or connect manually with:  start.bat client %s %s  (Windows)  "
                "or  ./start.sh client %s %s  (macOS/Linux)",
                lan_hint,
                self.port,
                lan_hint,
                self.port,
            )
        async with self._server:
            await self._server.serve_forever()

    async def shutdown(self) -> None:
        log.info("Shutting down…")
        if self._discovery is not None:
            self._discovery.close()
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
            peer_ip=peer[0] if peer else "",
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
            await self._cancel_offers_for(username)
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

        if msg_type == FILE_OFFER:
            await self._handle_file_offer(session, payload)
            return

        if msg_type == FILE_ACCEPT:
            await self._handle_file_accept(session, payload)
            return

        if msg_type == FILE_DECLINE:
            await self._handle_file_decline(session, payload)
            return

        if msg_type == FILE_RESULT:
            await self._handle_file_result(session, payload)
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

        dest = await self._find_session(target)
        if dest is not None and dest.username:
            target = dest.username

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
        await self._cancel_offers_for(username)
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
            "  /send <user> <path>      Send a file directly over the LAN\n"
            "  /accept [id]             Accept an incoming file\n"
            "  /decline [id]            Decline an incoming file\n"
            "  /transfers               List pending/active transfers\n"
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

    # --- File transfer brokering ---
    #
    # The server never relays file bytes. It validates an offer, tells the
    # recipient about it, and hands the sender the recipient's LAN address
    # once they accept; the transfer itself is a direct socket between peers.

    async def _handle_file_offer(
        self, session: ClientSession, payload: dict[str, Any]
    ) -> None:
        if not await self._require_login(session):
            return
        assert session.username is not None

        transfer_id = str(payload.get("transfer_id") or "")
        target = (payload.get("to") or "").strip()
        filename = sanitize_filename(str(payload.get("filename") or ""))
        try:
            size = int(payload.get("size"))
        except (TypeError, ValueError):
            size = -1

        if not TRANSFER_ID_RE.fullmatch(transfer_id):
            await self._send(
                session, SYSTEM_MSG, {"content": "Error: malformed transfer id."}
            )
            return
        if not filename:
            await self._send(
                session, SYSTEM_MSG, {"content": "Error: unusable file name."}
            )
            return
        if not 0 < size <= config.MAX_FILE_SIZE:
            await self._send(
                session,
                SYSTEM_MSG,
                {
                    "content": (
                        "Error: file must be between 1 byte and "
                        f"{human_size(config.MAX_FILE_SIZE)}."
                    )
                },
            )
            return

        dest = await self._find_session(target)
        if dest is None or dest.username is None:
            await self._send(
                session,
                SYSTEM_MSG,
                {"content": f"Error: User '{target}' is not online."},
            )
            return

        async with self._lock:
            if transfer_id in self.offers:
                await self._send(
                    session,
                    SYSTEM_MSG,
                    {"content": "Error: that transfer id is already in use."},
                )
                return
            mine = sum(
                1 for o in self.offers.values() if o.sender == session.username
            )
            if mine >= config.MAX_PENDING_OFFERS_PER_USER:
                await self._send(
                    session,
                    SYSTEM_MSG,
                    {
                        "content": (
                            "Error: too many pending offers "
                            f"(max {config.MAX_PENDING_OFFERS_PER_USER}). "
                            "Wait for them to be answered."
                        )
                    },
                )
                return
            self.offers[transfer_id] = FileOffer(
                transfer_id=transfer_id,
                sender=session.username,
                recipient=dest.username,
                filename=filename,
                size=size,
                created=_loop_time(),
            )

        await self._send(
            dest,
            FILE_OFFER,
            {
                "transfer_id": transfer_id,
                "from": session.username,
                "filename": filename,
                "size": size,
            },
        )
        await self._send(
            session,
            SYSTEM_MSG,
            {
                "content": (
                    f"Offered '{filename}' ({human_size(size)}) to "
                    f"@{dest.username} [{transfer_id}]. Waiting for them to accept."
                )
            },
        )
        log.info(
            "File offer %s: %s -> %s ('%s', %s)",
            transfer_id,
            session.username,
            dest.username,
            filename,
            human_size(size),
        )

    async def _take_offer(
        self, session: ClientSession, payload: dict[str, Any], as_recipient: bool
    ) -> "FileOffer | None":
        """Pop the offer named in `payload` if this session is a party to it."""
        transfer_id = str(payload.get("transfer_id") or "")
        async with self._lock:
            offer = self.offers.get(transfer_id)
            if offer is not None:
                party = offer.recipient if as_recipient else offer.sender
                if party == session.username:
                    del self.offers[transfer_id]
                else:
                    offer = None
        if offer is None:
            await self._send(
                session,
                SYSTEM_MSG,
                {"content": f"Error: no pending transfer '{transfer_id}'."},
            )
        return offer

    async def _handle_file_accept(
        self, session: ClientSession, payload: dict[str, Any]
    ) -> None:
        if not await self._require_login(session):
            return
        offer = await self._take_offer(session, payload, as_recipient=True)
        if offer is None:
            return

        try:
            port = int(payload.get("port"))
        except (TypeError, ValueError):
            port = 0
        token = str(payload.get("token") or "")
        if not 0 < port <= 65535 or not token:
            await self._send(
                session,
                SYSTEM_MSG,
                {"content": "Error: invalid listening port for the transfer."},
            )
            return

        sender = await self._find_session(offer.sender)
        if sender is None:
            await self._send(
                session,
                SYSTEM_MSG,
                {"content": f"Error: @{offer.sender} went offline; transfer cancelled."},
            )
            return

        await self._send(
            sender,
            FILE_ACCEPT,
            {
                "transfer_id": offer.transfer_id,
                "from": offer.recipient,
                "host": session.peer_ip,
                "port": port,
                "token": token,
                "filename": offer.filename,
                "size": offer.size,
            },
        )
        log.info(
            "Transfer %s accepted: %s is listening on %s:%s",
            offer.transfer_id,
            offer.recipient,
            session.peer_ip,
            port,
        )

    async def _handle_file_decline(
        self, session: ClientSession, payload: dict[str, Any]
    ) -> None:
        if not await self._require_login(session):
            return
        offer = await self._take_offer(session, payload, as_recipient=True)
        if offer is None:
            return
        sender = await self._find_session(offer.sender)
        if sender is not None:
            await self._send(
                sender,
                FILE_DECLINE,
                {
                    "transfer_id": offer.transfer_id,
                    "from": offer.recipient,
                    "filename": offer.filename,
                },
            )
        await self._send(
            session,
            SYSTEM_MSG,
            {"content": f"Declined '{offer.filename}' from @{offer.sender}."},
        )

    async def _handle_file_result(
        self, session: ClientSession, payload: dict[str, Any]
    ) -> None:
        """A receiver reporting the outcome back to the sender."""
        if not await self._require_login(session):
            return
        assert session.username is not None
        peer = (payload.get("to") or "").strip()
        dest = await self._find_session(peer)
        if dest is None:
            return
        await self._send(
            dest,
            FILE_RESULT,
            {
                "transfer_id": str(payload.get("transfer_id") or ""),
                "from": session.username,
                "ok": bool(payload.get("ok")),
                "message": str(payload.get("message") or "")[:500],
            },
        )

    async def _cancel_offers_for(self, username: str) -> None:
        """Drop every offer this user is a party to and tell the other side."""
        async with self._lock:
            doomed = [
                o for o in self.offers.values()
                if username in (o.sender, o.recipient)
            ]
            for offer in doomed:
                self.offers.pop(offer.transfer_id, None)
        for offer in doomed:
            other = offer.recipient if offer.sender == username else offer.sender
            peer = await self._find_session(other)
            if peer is not None:
                await self._send(
                    peer,
                    FILE_DECLINE,
                    {
                        "transfer_id": offer.transfer_id,
                        "from": username,
                        "filename": offer.filename,
                        "reason": f"@{username} went offline",
                    },
                )

    async def _expire_offers(self) -> None:
        """Time out offers nobody answered, so ids and memory are not leaked."""
        cutoff = _loop_time() - config.TRANSFER_OFFER_TIMEOUT
        async with self._lock:
            stale = [o for o in self.offers.values() if o.created < cutoff]
            for offer in stale:
                self.offers.pop(offer.transfer_id, None)
        for offer in stale:
            for name in (offer.sender, offer.recipient):
                peer = await self._find_session(name)
                if peer is not None:
                    await self._send(
                        peer,
                        FILE_DECLINE,
                        {
                            "transfer_id": offer.transfer_id,
                            "from": offer.sender,
                            "filename": offer.filename,
                            "reason": "offer expired",
                        },
                    )
            log.info("File offer %s expired", offer.transfer_id)

    # --- Helpers ---

    async def _find_session(self, name: str) -> ClientSession | None:
        """Look up a logged-in session by username, case-insensitively."""
        name = (name or "").strip()
        if not name:
            return None
        async with self._lock:
            dest = self.sessions.get(name)
            if dest is None:
                lower = name.lower()
                for candidate, sess in self.sessions.items():
                    if candidate.lower() == lower:
                        dest = sess
                        break
        return dest

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
                await self._expire_offers()
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
            # Windows: no loop signal handlers; hop back onto the loop thread
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))

    serve_task = asyncio.create_task(server.start())
    stop_task = asyncio.create_task(stop.wait())
    await asyncio.wait({serve_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
    stop_task.cancel()
    if serve_task.done() and not serve_task.cancelled() and serve_task.exception():
        exc = serve_task.exception()
        log.error("Could not start server on %s:%s: %s", host, port, exc)
        log.error("Is another server already running? Try a different port.")
        server.db.close()
        sys.exit(1)
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
