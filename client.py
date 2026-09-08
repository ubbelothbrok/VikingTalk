"""Terminal chat client with non-blocking I/O and prompt redraw."""

from __future__ import annotations

import asyncio
import sys
from typing import Any

try:
    import colorama
    from colorama import Fore, Style

    colorama.init()
except ImportError:  # pragma: no cover
    class _Fore:
        CYAN = YELLOW = GREEN = WHITE = RED = MAGENTA = ""

    class _Style:
        RESET_ALL = BRIGHT = DIM = ""

    Fore = _Fore()  # type: ignore
    Style = _Style()  # type: ignore

import config
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

# ANSI helpers
_CLEAR_LINE = "\r\033[2K"
_HIDE_CURSOR = "\033[?25l"
_SHOW_CURSOR = "\033[?25h"


class ChatClient:
    def __init__(self, host: str = config.HOST, port: int = config.PORT) -> None:
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

    # --- Input ---

    async def _stdin_reader(self) -> None:
        """
        Read stdin character-by-character when a TTY is available so we can
        preserve partial input when server messages arrive. Falls back to
        line-buffered reads otherwise.
        """
        loop = asyncio.get_running_loop()
        if sys.stdin.isatty() and sys.platform != "win32":
            await self._tty_input_loop(loop)
        else:
            await self._line_input_loop(loop)

    async def _line_input_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        while self.running:
            try:
                line = await loop.run_in_executor(None, sys.stdin.readline)
            except Exception:
                break
            if line == "":
                await self._stdin_queue.put(None)
                break
            await self._stdin_queue.put(line.rstrip("\n\r"))

    async def _tty_input_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        import termios
        import tty

        fd = sys.stdin.fileno()
        old = termios.tcgetattr(fd)
        try:
            tty.setcbreak(fd)
            self._redraw_prompt()
            while self.running:
                ch = await loop.run_in_executor(None, sys.stdin.read, 1)
                if not ch:
                    await self._stdin_queue.put(None)
                    break
                if ch in ("\n", "\r"):
                    line = self._input_buffer
                    self._input_buffer = ""
                    sys.stdout.write("\n")
                    sys.stdout.flush()
                    await self._stdin_queue.put(line)
                    self._redraw_prompt()
                elif ch in ("\x7f", "\b"):  # backspace
                    if self._input_buffer:
                        self._input_buffer = self._input_buffer[:-1]
                        self._redraw_prompt()
                elif ch == "\x03":  # Ctrl+C
                    await self._stdin_queue.put("/quit")
                elif ch == "\x04":  # Ctrl+D
                    await self._stdin_queue.put(None)
                    break
                elif ch == "\x15":  # Ctrl+U clear line
                    self._input_buffer = ""
                    self._redraw_prompt()
                elif ord(ch) >= 32:
                    self._input_buffer += ch
                    sys.stdout.write(ch)
                    sys.stdout.flush()
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old)
            sys.stdout.write(_SHOW_CURSOR)
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

        if cmd == "quit":
            self.running = False
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
        stdin_task = asyncio.create_task(self._stdin_reader())

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
            stdin_task.cancel()
            reader_task.cancel()
            for t in (stdin_task, reader_task):
                try:
                    await t
                except asyncio.CancelledError:
                    pass
            print()


async def main() -> None:
    host = config.HOST
    port = config.PORT
    if len(sys.argv) >= 2:
        host = sys.argv[1]
    if len(sys.argv) >= 3:
        port = int(sys.argv[2])
    client = ChatClient(host, port)
    await client.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nGoodbye.")
