"""SQLite persistence for users and message history."""

from __future__ import annotations

import re
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any

import bcrypt

from config import DB_PATH, DEFAULT_CHANNEL, HISTORY_DEFAULT_LIMIT

_NAME_RE = re.compile(r"^[A-Za-z0-9_]{1,32}$")


def is_valid_name(name: str) -> bool:
    """Usernames and channel names: alphanumeric + underscore only."""
    return bool(name) and bool(_NAME_RE.fullmatch(name))


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


class Database:
    """Thread-safe SQLite wrapper for credentials and chat history."""

    def __init__(self, path: str = DB_PATH) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL UNIQUE COLLATE NOCASE,
                    password_hash BLOB NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS channels (
                    name TEXT PRIMARY KEY COLLATE NOCASE,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel_name TEXT NOT NULL,
                    username TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    content TEXT NOT NULL,
                    FOREIGN KEY (channel_name) REFERENCES channels(name)
                );

                CREATE INDEX IF NOT EXISTS idx_messages_channel_id
                    ON messages(channel_name, id DESC);
                """
            )
            # Ensure default channel exists
            self._conn.execute(
                "INSERT OR IGNORE INTO channels(name, created_at) VALUES (?, ?)",
                (DEFAULT_CHANNEL, _utc_now()),
            )
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # --- Users ---

    def register_user(self, username: str, password: str) -> tuple[bool, str]:
        if not is_valid_name(username):
            return False, "Username must be alphanumeric/underscore, 1-32 chars."
        pw_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt())
        with self._lock:
            try:
                self._conn.execute(
                    "INSERT INTO users(username, password_hash, created_at) VALUES (?, ?, ?)",
                    (username, pw_hash, _utc_now()),
                )
                self._conn.commit()
            except sqlite3.IntegrityError:
                return False, f"Username '{username}' is already taken."
        return True, f"Registered successfully as '{username}'. You can /login now."

    def verify_user(self, username: str, password: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT password_hash FROM users WHERE username = ? COLLATE NOCASE",
                (username,),
            ).fetchone()
        if row is None:
            return False
        stored = row["password_hash"]
        if isinstance(stored, str):
            stored = stored.encode("utf-8")
        try:
            return bcrypt.checkpw(password.encode("utf-8"), stored)
        except ValueError:
            return False

    def get_canonical_username(self, username: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT username FROM users WHERE username = ? COLLATE NOCASE",
                (username,),
            ).fetchone()
        return row["username"] if row else None

    # --- Channels ---

    def ensure_channel(self, name: str) -> tuple[bool, str]:
        if not is_valid_name(name):
            return False, "Channel name must be alphanumeric/underscore, 1-32 chars."
        with self._lock:
            try:
                self._conn.execute(
                    "INSERT INTO channels(name, created_at) VALUES (?, ?)",
                    (name, _utc_now()),
                )
                self._conn.commit()
            except sqlite3.IntegrityError:
                return False, f"Channel '{name}' already exists."
        return True, f"Channel '{name}' created."

    def channel_exists(self, name: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM channels WHERE name = ? COLLATE NOCASE",
                (name,),
            ).fetchone()
        return row is not None

    def get_canonical_channel(self, name: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT name FROM channels WHERE name = ? COLLATE NOCASE",
                (name,),
            ).fetchone()
        return row["name"] if row else None

    def list_channels(self) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT name FROM channels ORDER BY name COLLATE NOCASE"
            ).fetchall()
        return [r["name"] for r in rows]

    # --- Messages ---

    def save_message(self, channel: str, username: str, content: str) -> dict[str, Any]:
        ts = _utc_now()
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO messages(channel_name, username, timestamp, content) "
                "VALUES (?, ?, ?, ?)",
                (channel, username, ts, content),
            )
            self._conn.commit()
            msg_id = cur.lastrowid
        return {
            "id": msg_id,
            "channel": channel,
            "username": username,
            "timestamp": ts,
            "content": content,
        }

    def get_history(
        self, channel: str, limit: int = HISTORY_DEFAULT_LIMIT
    ) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, channel_name, username, timestamp, content "
                "FROM messages WHERE channel_name = ? COLLATE NOCASE "
                "ORDER BY id DESC LIMIT ?",
                (channel, limit),
            ).fetchall()
        messages = [
            {
                "id": r["id"],
                "channel": r["channel_name"],
                "username": r["username"],
                "timestamp": r["timestamp"],
                "content": r["content"],
            }
            for r in reversed(rows)
        ]
        return messages
