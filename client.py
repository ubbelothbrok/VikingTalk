"""Terminal chat client with non-blocking I/O and prompt redraw."""

from __future__ import annotations

import asyncio
import os
import sys
import threading
import time
from typing import Any

# Never crash on characters the local console can't display (e.g. emoji on a
# Windows code page); show a replacement character instead.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(errors="replace")
    except (AttributeError, ValueError):
        pass

try:
    import colorama
    from colorama import Fore, Style

    if hasattr(colorama, "just_fix_windows_console"):
        colorama.just_fix_windows_console()  # enable ANSI on Windows, no-op elsewhere
    else:
        colorama.init()
except ImportError:  # pragma: no cover
    class _Fore:
        CYAN = YELLOW = GREEN = WHITE = RED = MAGENTA = ""

    class _Style:
        RESET_ALL = BRIGHT = DIM = ""

    Fore = _Fore()  # type: ignore
    Style = _Style()  # type: ignore

import config
from discovery import find_servers
from filetransfer import (
    IncomingTransfer,
    TransferError,
    human_size,
    new_transfer_id,
    sanitize_filename,
    send_file,
)
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

# ANSI helpers
_CLEAR_LINE = "\r\033[2K"
_HIDE_CURSOR = "\033[?25l"
_SHOW_CURSOR = "\033[?25h"


class ChatClient:
    def __init__(
        self, host: str = config.CLIENT_FALLBACK_HOST, port: int = config.PORT
    ) -> None:
        self.host = host
        self.port = port
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.username: str | None = None
        self.channel: str = config.DEFAULT_CHANNEL
        self.running = True
        self.connected = False
        self._input_buffer = ""
        self._print_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._stdin_queue: asyncio.Queue[str | None] = asyncio.Queue()
        self._reconnect_attempts = 0
        # File transfers. Offers wait for the user; outgoing waits for an
        # accept; incoming holds the live one-shot listeners.
        self._offers: dict[str, dict[str, Any]] = {}
        self._outgoing: dict[str, dict[str, Any]] = {}
        self._incoming: dict[str, IncomingTransfer] = {}
        self._send_tasks: set[asyncio.Task] = set()

    # --- Display ---

    def _prompt_text(self) -> str:
        if self.username:
            return f"[{self.channel}] @{self.username} > "
        return "> "

    def _colorize_line(self, kind: str, text: str) -> str:
        colors = {
            "system": Fore.CYAN,
            "private": Fore.YELLOW,
            "own": Fore.GREEN,
            "other": Fore.WHITE,
            "error": Fore.RED,
            "history": Fore.MAGENTA,
            "file": Fore.MAGENTA,
        }
        color = colors.get(kind, Fore.WHITE)
        return f"{color}{text}{Style.RESET_ALL}"

    async def _safe_print(self, text: str, kind: str = "other") -> None:
        """Print a line without clobbering the current input buffer."""
        colored = self._colorize_line(kind, text)
        async with self._print_lock:
            sys.stdout.write(f"{_CLEAR_LINE}{colored}\n")
            sys.stdout.write(f"{self._prompt_text()}{self._input_buffer}")
            sys.stdout.flush()

    def _redraw_prompt(self) -> None:
        sys.stdout.write(f"{_CLEAR_LINE}{self._prompt_text()}{self._input_buffer}")
        sys.stdout.flush()

    # --- Networking ---

    async def connect(self) -> bool:
        try:
            self.reader, self.writer = await asyncio.open_connection(
                self.host, self.port
            )
            self.connected = True
            self._reconnect_attempts = 0
            await self._safe_print(
                f"Connected to {self.host}:{self.port}", "system"
            )
            return True
        except OSError as exc:
            await self._safe_print(f"Connection failed: {exc}", "error")
            self.connected = False
            return False

    async def disconnect(self, send_logout: bool = True) -> None:
        if send_logout and self.connected and self.username and self.writer:
            try:
                await self._send(CMD_REQUEST, {"command": "logout", "args": []})
            except Exception:
                pass
        self.connected = False
        self.username = None
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass
            self.writer = None
            self.reader = None

    async def _send(self, msg_type: str, payload: dict[str, Any] | None = None) -> None:
        if not self.writer:
            raise ConnectionError("Not connected")
        async with self._write_lock:
            await write_message(self.writer, msg_type, payload)

    async def _reconnect_loop(self) -> None:
        delay = config.RECONNECT_BASE_DELAY
        while self.running and not self.connected:
            if self._reconnect_attempts >= config.RECONNECT_MAX_ATTEMPTS:
                await self._safe_print(
                    "Max reconnect attempts reached. Type /quit or retry later.",
                    "error",
                )
                return
            self._reconnect_attempts += 1
            await self._safe_print(
                f"Reconnecting in {delay:.0f}s "
                f"(attempt {self._reconnect_attempts}/{config.RECONNECT_MAX_ATTEMPTS})…",
                "system",
            )
            await asyncio.sleep(delay)
            if await self.connect():
                await self._safe_print(
                    "Reconnected. Please /login again.", "system"
                )
                return
            delay = min(delay * 2, config.RECONNECT_MAX_DELAY)

    # --- Incoming messages ---

    async def _reader_loop(self) -> None:
        assert self.reader is not None
        try:
            while self.running and self.connected:
                try:
                    msg = await read_message(self.reader)
                except asyncio.IncompleteReadError:
                    break
                except ValueError as exc:
                    await self._safe_print(f"Bad frame: {exc}", "error")
                    break
                if msg is None:
                    break
                await self._handle_server_message(msg)
        except (ConnectionResetError, BrokenPipeError, ConnectionError):
            pass
        except Exception as exc:
            await self._safe_print(f"Reader error: {exc}", "error")
        finally:
            was_connected = self.connected
            self.connected = False
            if self.running and was_connected:
                await self._safe_print("Disconnected from server.", "error")
                self.username = None
                await self._cancel_all_transfers()
                asyncio.create_task(self._reconnect_loop())

    async def _handle_server_message(self, msg: dict[str, Any]) -> None:
        msg_type = msg.get("type")
        payload = msg.get("payload") or {}

        if msg_type == PING:
            try:
                await self._send(PONG, {})
            except Exception:
                pass
            return

        if msg_type == PONG:
            return

        if msg_type == AUTH_RESPONSE:
            success = bool(payload.get("success"))
            message = payload.get("message") or ""
            if success:
                self.username = payload.get("username") or self.username
                self.channel = payload.get("channel") or config.DEFAULT_CHANNEL
                await self._safe_print(message, "system")
            else:
                await self._safe_print(message or "Authentication failed.", "error")
            return

        if msg_type == SYSTEM_MSG:
            content = payload.get("content") or ""
            if "channel" in payload and payload["channel"]:
                self.channel = payload["channel"]
            # Detect join confirmation
            if content.startswith("Joined #"):
                # channel already set via payload if present
                pass
            await self._safe_print(content, "system")
            return

        if msg_type == CMD_RESPONSE:
            await self._safe_print(payload.get("content") or "", "system")
            return

        if msg_type == PUBLIC_MSG:
            user = payload.get("username") or "?"
            channel = payload.get("channel") or self.channel
            content = payload.get("content") or ""
            line = f"[{channel}] @{user}: {content}"
            kind = "own" if user == self.username else "other"
            await self._safe_print(line, kind)
            return

        if msg_type == PRIVATE_MSG:
            sender = payload.get("from") or "?"
            content = payload.get("content") or ""
            if payload.get("echo"):
                target = payload.get("to") or "?"
                line = f"[PM to {target}]: {content}"
            else:
                line = f"[PM from {sender}]: {content}"
            await self._safe_print(line, "private")
            return

        if msg_type == FILE_OFFER:
            await self._on_file_offer(payload)
            return

        if msg_type == FILE_ACCEPT:
            await self._on_file_accept(payload)
            return

        if msg_type == FILE_DECLINE:
            await self._on_file_decline(payload)
            return

        if msg_type == FILE_RESULT:
            ok = bool(payload.get("ok"))
            await self._safe_print(
                f"@{payload.get('from') or '?'}: {payload.get('message') or ''}",
                "file" if ok else "error",
            )
            self._outgoing.pop(str(payload.get("transfer_id") or ""), None)
            return

        if msg_type == HISTORY_RESPONSE:
            channel = payload.get("channel") or self.channel
            messages = payload.get("messages") or []
            if not messages:
                await self._safe_print(f"(no history in #{channel})", "history")
                return
            await self._safe_print(
                f"--- last {len(messages)} messages in #{channel} ---", "history"
            )
            for m in messages:
                line = f"[{m.get('channel', channel)}] @{m.get('username')}: {m.get('content')}"
                kind = "own" if m.get("username") == self.username else "other"
                await self._safe_print(line, kind)
            await self._safe_print("--- end history ---", "history")
            return

        await self._safe_print(f"Unknown server message: {msg_type}", "error")

    # --- File transfer ---
    #
    # The server only brokers: /send announces an offer, the recipient's
    # /accept opens a one-shot port, and the bytes then flow straight from
    # peer to peer -- never through the server, never through JSON frames.

    def _print_soon(self, text: str, kind: str = "file") -> None:
        """Print from a synchronous callback running on the event loop."""
        task = asyncio.create_task(self._safe_print(text, kind))
        self._send_tasks.add(task)
        task.add_done_callback(self._send_tasks.discard)

    def _progress_reporter(self, label: str):
        """A throttled progress callback: one line a second, plus the last one."""
        state = {"last": 0.0, "start": time.monotonic()}

        def report(done: int, total: int) -> None:
            now = time.monotonic()
            if done < total and now - state["last"] < 1.0:
                return
            state["last"] = now
            elapsed = max(now - state["start"], 1e-6)
            pct = (done / total * 100) if total else 100.0
            self._print_soon(
                f"{label} {pct:5.1f}%  {human_size(done)} / {human_size(total)}"
                f"  ({human_size(done / elapsed)}/s)"
            )

        return report

    def _resolve_offer_id(self, args: list[str], verb: str) -> str | None:
        """Pick the offer an /accept or /decline refers to (default: the only one)."""
        if args:
            transfer_id = args[0].lower()
            if transfer_id in self._offers:
                return transfer_id
            self._print_soon(f"No pending offer '{transfer_id}'.", "error")
            return None
        if not self._offers:
            self._print_soon("No incoming file offers.", "error")
            return None
        if len(self._offers) > 1:
            self._print_soon(
                f"Several offers pending; name one: /{verb} <id>  "
                f"(ids: {', '.join(sorted(self._offers))})",
                "error",
            )
            return None
        return next(iter(self._offers))

    async def _cmd_send_file(self, raw: str) -> None:
        """/send <user> <path> -- paths may contain spaces and may be quoted."""
        parts = raw.strip().split(None, 1)
        if len(parts) < 2 or not parts[1].strip():
            await self._safe_print("Error: usage /send <username> <path>", "error")
            return
        if not self.connected or not self.username:
            await self._safe_print("Please /login first.", "error")
            return

        target = parts[0]
        # A username never contains whitespace, so the whole tail is the path.
        # Taking it raw keeps Windows backslashes intact (shlex would eat them).
        raw_path = parts[1].strip()
        for quote in ('"', "'"):
            if len(raw_path) > 1 and raw_path[0] == quote and raw_path[-1] == quote:
                raw_path = raw_path[1:-1]
                break
        path = os.path.abspath(os.path.expanduser(raw_path))
        if not os.path.isfile(path):
            await self._safe_print(f"Error: no such file: {path}", "error")
            return
        filename = sanitize_filename(os.path.basename(path))
        if not filename:
            await self._safe_print("Error: unusable file name.", "error")
            return
        try:
            size = os.path.getsize(path)
        except OSError as exc:
            await self._safe_print(f"Error: cannot read file ({exc})", "error")
            return
        if size == 0:
            await self._safe_print("Error: file is empty.", "error")
            return
        if size > config.MAX_FILE_SIZE:
            await self._safe_print(
                f"Error: file is {human_size(size)}; the limit is "
                f"{human_size(config.MAX_FILE_SIZE)}.",
                "error",
            )
            return

        transfer_id = new_transfer_id()
        self._outgoing[transfer_id] = {
            "to": target,
            "path": path,
            "filename": filename,
            "size": size,
        }
        try:
            await self._send(
                FILE_OFFER,
                {
                    "to": target,
                    "transfer_id": transfer_id,
                    "filename": filename,
                    "size": size,
                },
            )
        except Exception as exc:
            self._outgoing.pop(transfer_id, None)
            await self._safe_print(f"Send failed: {exc}", "error")

    async def _cmd_accept(self, args: list[str]) -> None:
        transfer_id = self._resolve_offer_id(args, "accept")
        if transfer_id is None:
            return
        offer = self._offers.pop(transfer_id)

        incoming = IncomingTransfer(
            transfer_id=transfer_id,
            sender=offer["from"],
            filename=offer["filename"],
            size=offer["size"],
            dest_dir=config.DOWNLOADS_DIR,
        )
        try:
            port = await incoming.start(
                on_progress=self._progress_reporter(
                    f"Receiving {offer['filename']}"
                ),
                on_done=lambda ok, message, path, tid=transfer_id: self._on_receive_done(
                    tid, ok, message, path
                ),
            )
        except OSError as exc:
            await self._safe_print(f"Could not open a port to receive: {exc}", "error")
            return

        self._incoming[transfer_id] = incoming
        try:
            await self._send(
                FILE_ACCEPT,
                {"transfer_id": transfer_id, "port": port, "token": incoming.token},
            )
        except Exception as exc:
            await incoming.close()
            self._incoming.pop(transfer_id, None)
            await self._safe_print(f"Send failed: {exc}", "error")
            return
        await self._safe_print(
            f"Accepted '{offer['filename']}' -- waiting for @{offer['from']} "
            f"to connect on port {port}…",
            "file",
        )

    async def _cmd_decline(self, args: list[str]) -> None:
        transfer_id = self._resolve_offer_id(args, "decline")
        if transfer_id is None:
            return
        self._offers.pop(transfer_id, None)
        try:
            await self._send(FILE_DECLINE, {"transfer_id": transfer_id})
        except Exception as exc:
            await self._safe_print(f"Send failed: {exc}", "error")

    async def _cmd_transfers(self) -> None:
        lines: list[str] = []
        for tid, offer in sorted(self._offers.items()):
            lines.append(
                f"  [{tid}] incoming  '{offer['filename']}' "
                f"({human_size(offer['size'])}) from @{offer['from']}  "
                f"-- /accept {tid} or /decline {tid}"
            )
        for tid, out in sorted(self._outgoing.items()):
            lines.append(
                f"  [{tid}] outgoing  '{out['filename']}' "
                f"({human_size(out['size'])}) to @{out['to']}  -- waiting"
            )
        for tid, inc in sorted(self._incoming.items()):
            lines.append(
                f"  [{tid}] receiving '{inc.filename}' "
                f"({human_size(inc.size)}) from @{inc.sender}"
            )
        body = "Transfers:\n" + ("\n".join(lines) if lines else "  (none)")
        await self._safe_print(body, "file")

    async def _on_file_offer(self, payload: dict[str, Any]) -> None:
        transfer_id = str(payload.get("transfer_id") or "")
        filename = sanitize_filename(str(payload.get("filename") or ""))
        sender = str(payload.get("from") or "?")
        try:
            size = int(payload.get("size"))
        except (TypeError, ValueError):
            return
        if not transfer_id or not filename:
            return
        self._offers[transfer_id] = {
            "from": sender,
            "filename": filename,
            "size": size,
        }
        await self._safe_print(
            f"@{sender} wants to send you '{filename}' ({human_size(size)}).  "
            f"/accept {transfer_id}  or  /decline {transfer_id}",
            "file",
        )

    async def _on_file_accept(self, payload: dict[str, Any]) -> None:
        transfer_id = str(payload.get("transfer_id") or "")
        out = self._outgoing.get(transfer_id)
        if out is None:
            return
        host = str(payload.get("host") or "")
        token = str(payload.get("token") or "")
        try:
            port = int(payload.get("port"))
        except (TypeError, ValueError):
            port = 0
        if not host or not token or not 0 < port <= 65535:
            await self._safe_print(
                f"Transfer {transfer_id}: the recipient sent an unusable address.",
                "error",
            )
            self._outgoing.pop(transfer_id, None)
            return

        await self._safe_print(
            f"@{payload.get('from')} accepted '{out['filename']}'. "
            f"Sending directly to {host}:{port}…",
            "file",
        )
        task = asyncio.create_task(
            self._run_send(transfer_id, host, port, token, out)
        )
        self._send_tasks.add(task)
        task.add_done_callback(self._send_tasks.discard)

    async def _on_file_decline(self, payload: dict[str, Any]) -> None:
        transfer_id = str(payload.get("transfer_id") or "")
        filename = payload.get("filename") or "file"
        reason = payload.get("reason")
        who = payload.get("from") or "?"
        was_mine = self._outgoing.pop(transfer_id, None) is not None
        incoming = self._incoming.pop(transfer_id, None)
        was_offer = self._offers.pop(transfer_id, None) is not None
        if incoming is not None:
            await incoming.close()
        detail = f" ({reason})" if reason else ""
        if was_mine and not reason:
            text = f"@{who} declined '{filename}'."
        elif was_mine:
            text = f"Sending '{filename}' to @{who} was cancelled{detail}."
        elif was_offer or incoming is not None:
            text = f"Incoming '{filename}' from @{who} was cancelled{detail}."
        else:
            text = f"Transfer of '{filename}' was cancelled{detail}."
        await self._safe_print(text, "error")

    async def _run_send(
        self,
        transfer_id: str,
        host: str,
        port: int,
        token: str,
        out: dict[str, Any],
    ) -> None:
        try:
            await send_file(
                host=host,
                port=port,
                transfer_id=transfer_id,
                token=token,
                path=out["path"],
                size=out["size"],
                progress=self._progress_reporter(f"Sending {out['filename']}"),
            )
            await self._safe_print(
                f"Sent '{out['filename']}' ({human_size(out['size'])}) "
                f"to @{out['to']}. Waiting for their confirmation…",
                "file",
            )
        except (TransferError, OSError) as exc:
            self._outgoing.pop(transfer_id, None)
            await self._safe_print(
                f"Could not send '{out['filename']}': {exc}", "error"
            )
        except asyncio.CancelledError:
            raise

    def _on_receive_done(
        self, transfer_id: str, ok: bool, message: str, path: str | None
    ) -> None:
        """Called from the receiver when a transfer ends, for better or worse."""
        incoming = self._incoming.pop(transfer_id, None)
        self._print_soon(
            f"{message} -> {path}" if ok and path else message,
            "file" if ok else "error",
        )
        if incoming is not None and self.connected and self.username:
            task = asyncio.create_task(
                self._report_result(transfer_id, incoming.sender, ok, message)
            )
            self._send_tasks.add(task)
            task.add_done_callback(self._send_tasks.discard)

    async def _report_result(
        self, transfer_id: str, sender: str, ok: bool, message: str
    ) -> None:
        """Tell the sender how it went, through the server."""
        try:
            await self._send(
                FILE_RESULT,
                {
                    "transfer_id": transfer_id,
                    "to": sender,
                    "ok": ok,
                    "message": message,
                },
            )
        except Exception:
            pass

    async def _cancel_all_transfers(self) -> None:
        for incoming in list(self._incoming.values()):
            await incoming.close()
        self._incoming.clear()
        self._offers.clear()
        self._outgoing.clear()

    # --- Input ---

    def _start_input_thread(self) -> None:
        """
        Read the keyboard on a daemon thread and hand keys to the event loop.
        Character-at-a-time on a terminal (so partial input survives incoming
        messages), line-at-a-time otherwise. A daemon thread never blocks exit.
        """
        loop = asyncio.get_running_loop()
        is_tty = sys.stdin.isatty()

        if is_tty and sys.platform != "win32":
            import termios
            import tty

            fd = sys.stdin.fileno()
            self._saved_tty = (fd, termios.tcgetattr(fd))
            tty.setcbreak(fd)

        def post(callback, value) -> None:
            try:
                loop.call_soon_threadsafe(callback, value)
            except RuntimeError:  # loop already closed
                pass

        def worker() -> None:
            try:
                if is_tty and sys.platform == "win32":
                    import msvcrt

                    while True:
                        ch = msvcrt.getwch()
                        if ch in ("\x00", "\xe0"):  # arrow/function key prefix
                            msvcrt.getwch()
                            continue
                        post(self._on_key, ch)
                elif is_tty:
                    while True:
                        ch = sys.stdin.read(1)
                        if not ch:
                            break
                        post(self._on_key, ch)
                else:
                    for line in sys.stdin:
                        post(self._stdin_queue.put_nowait, line.rstrip("\r\n"))
            except Exception:
                pass
            post(self._stdin_queue.put_nowait, None)

        threading.Thread(target=worker, name="stdin", daemon=True).start()
        if is_tty:
            self._redraw_prompt()

    def _restore_terminal(self) -> None:
        saved = getattr(self, "_saved_tty", None)
        if saved:
            import termios

            termios.tcsetattr(saved[0], termios.TCSADRAIN, saved[1])
            self._saved_tty = None
        sys.stdout.write(_SHOW_CURSOR)
        sys.stdout.flush()

    def _on_key(self, ch: str) -> None:
        """Handle one keypress (runs on the event loop thread)."""
        if ch in ("\n", "\r"):
            line = self._input_buffer
            self._input_buffer = ""
            self._stdin_queue.put_nowait(line)
            self._redraw_prompt()
        elif ch in ("\x7f", "\b"):  # backspace (Unix sends DEL, Windows sends BS)
            if self._input_buffer:
                self._input_buffer = self._input_buffer[:-1]
                self._redraw_prompt()
        elif ch == "\x03":  # Ctrl+C
            self._stdin_queue.put_nowait("/quit")
        elif ch in ("\x04", "\x1a"):  # Ctrl+D (Unix) / Ctrl+Z (Windows)
            self._stdin_queue.put_nowait(None)
        elif ch == "\x15":  # Ctrl+U clear line
            self._input_buffer = ""
            self._redraw_prompt()
        elif ch == "\x1b":  # Escape (start of arrow-key sequences on Unix): ignore
            pass
        elif ch.isprintable():
            self._input_buffer += ch
            sys.stdout.write(ch)
            sys.stdout.flush()

    async def _process_input_line(self, line: str) -> None:
        line = line.strip()
        if not line:
            return

        if line.startswith("/"):
            await self._handle_local_command(line)
        else:
            if not self.connected:
                await self._safe_print("Not connected. Waiting to reconnect…", "error")
                return
            if not self.username:
                await self._safe_print("Please /login first.", "error")
                return
            try:
                await self._send(PUBLIC_MSG, {"content": line})
            except Exception as exc:
                await self._safe_print(f"Send failed: {exc}", "error")

    async def _handle_local_command(self, line: str) -> None:
        parts = line.split()
        cmd = parts[0].lower().lstrip("/")
        args = parts[1:]

        if cmd == "send":
            # Keep the tail raw; a path may contain spaces or quotes.
            await self._cmd_send_file(line.split(maxsplit=1)[1] if len(parts) > 1 else "")
            return

        if cmd == "accept":
            await self._cmd_accept(args)
            return

        if cmd == "decline":
            await self._cmd_decline(args)
            return

        if cmd == "transfers":
            await self._cmd_transfers()
            return

        if cmd == "quit":
            self.running = False
            await self._cancel_all_transfers()
            await self.disconnect(send_logout=True)
            await self._safe_print("Goodbye.", "system")
            return

        if cmd == "help":
            # Prefer server help if connected; otherwise local
            if self.connected:
                try:
                    await self._send(CMD_REQUEST, {"command": "help", "args": []})
                    return
                except Exception:
                    pass
            help_text = (
                "Commands:\n"
                "  /register <user> <pass>  Register\n"
                "  /login <user> <pass>     Log in\n"
                "  /logout                  Log out\n"
                "  /msg <user> <text>       Private message\n"
                "  /send <user> <path>      Send a file directly over the LAN\n"
                "  /accept [id]             Accept an incoming file\n"
                "  /decline [id]            Decline an incoming file\n"
                "  /transfers               List pending/active transfers\n"
                "  /create <channel>        Create channel\n"
                "  /join <channel>          Join channel\n"
                "  /leave                   Leave to #general\n"
                "  /channels                List channels\n"
                "  /users                   List users\n"
                "  /history [N]             Channel history\n"
                "  /quit                    Exit\n"
                "  /help                    This help"
            )
            await self._safe_print(help_text, "system")
            return

        if cmd in ("register", "login"):
            if len(args) < 2:
                await self._safe_print(
                    f"Error: usage /{cmd} <username> <password>", "error"
                )
                return
            if not self.connected:
                await self._safe_print("Not connected to server.", "error")
                return
            username, password = args[0], " ".join(args[1:])
            try:
                await self._send(
                    AUTH_REQUEST,
                    {"action": cmd, "username": username, "password": password},
                )
            except Exception as exc:
                await self._safe_print(f"Send failed: {exc}", "error")
            return

        if cmd == "msg":
            if len(args) < 2:
                await self._safe_print(
                    "Error: usage /msg <username> <message>", "error"
                )
                return
            if not self.connected:
                await self._safe_print("Not connected to server.", "error")
                return
            try:
                await self._send(
                    PRIVATE_MSG,
                    {"to": args[0], "content": " ".join(args[1:])},
                )
            except Exception as exc:
                await self._safe_print(f"Send failed: {exc}", "error")
            return

        # Forward remaining commands to the server
        if not self.connected:
            await self._safe_print("Not connected to server.", "error")
            return
        try:
            await self._send(CMD_REQUEST, {"command": cmd, "args": args})
        except Exception as exc:
            await self._safe_print(f"Send failed: {exc}", "error")

    # --- Main ---

    async def run(self) -> None:
        print(
            f"{Fore.CYAN}CLI Chat Client — connecting to "
            f"{self.host}:{self.port}{Style.RESET_ALL}"
        )
        if not await self.connect():
            await self._reconnect_loop()
            if not self.connected:
                return

        reader_task = asyncio.create_task(self._reader_loop())
        self._start_input_thread()

        try:
            while self.running:
                # If reader died and reconnect succeeded, restart reader
                if self.connected and reader_task.done():
                    reader_task = asyncio.create_task(self._reader_loop())

                try:
                    line = await asyncio.wait_for(
                        self._stdin_queue.get(), timeout=0.5
                    )
                except asyncio.TimeoutError:
                    continue

                if line is None:
                    self.running = False
                    break
                await self._process_input_line(line)
        except KeyboardInterrupt:
            self.running = False
        finally:
            self.running = False
            await self.disconnect(send_logout=True)
            self._restore_terminal()
            reader_task.cancel()
            try:
                await reader_task
            except asyncio.CancelledError:
                pass
            print()


def _choose_server() -> tuple[str, int]:
    """Find a server on the LAN; ask the user if there are several."""
    print(f"{Fore.CYAN}Searching the local network for a VikingTalk server…{Style.RESET_ALL}")
    servers = find_servers()
    if not servers:
        print(
            f"{Fore.YELLOW}No server found automatically. "
            f"Trying {config.CLIENT_FALLBACK_HOST}:{config.PORT}.\n"
            f"If the server is on another device, run: start.bat client <server-ip> "
            f"(Windows) or ./start.sh client <server-ip> (macOS/Linux)"
            f"{Style.RESET_ALL}"
        )
        return config.CLIENT_FALLBACK_HOST, config.PORT
    if len(servers) == 1:
        return servers[0]
    print("Found several servers:")
    for i, (ip, port) in enumerate(servers, 1):
        print(f"  {i}) {ip}:{port}")
    while True:
        choice = input(f"Pick one [1-{len(servers)}]: ").strip() or "1"
        if choice.isdigit() and 1 <= int(choice) <= len(servers):
            return servers[int(choice) - 1]


async def main() -> None:
    if len(sys.argv) >= 2:
        host = sys.argv[1]
        port = int(sys.argv[2]) if len(sys.argv) >= 3 else config.PORT
    else:
        host, port = await asyncio.to_thread(_choose_server)
    client = ChatClient(host, port)
    await client.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nGoodbye.")
