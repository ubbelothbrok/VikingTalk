"""Shared configuration for the CLI chat application."""

import os

_HERE = os.path.dirname(os.path.abspath(__file__))

# 0.0.0.0 = accept connections from other devices on your network
# Clients should connect to this machine's LAN IP (not 0.0.0.0).
HOST = "0.0.0.0"
PORT = int(os.environ.get("VIKINGTALK_PORT", "5555"))

# Address the client uses when no server is found on the LAN.
# (0.0.0.0 is only valid for binding; Windows refuses to connect to it.)
CLIENT_FALLBACK_HOST = "127.0.0.1"

# UDP port used for automatic server discovery on the LAN
DISCOVERY_PORT = int(os.environ.get("VIKINGTALK_DISCOVERY_PORT", "5556"))
DISCOVERY_TIMEOUT = 1.5

# Protocol limits
MAX_MESSAGE_SIZE = 65536  # 64 KiB payload cap
LENGTH_PREFIX_SIZE = 4

# Heartbeat / keep-alive (seconds)
HEARTBEAT_INTERVAL = 30
HEARTBEAT_TIMEOUT = 60

# Persistence
# Stored next to this file so it does not depend on the working directory
DB_PATH = os.environ.get("VIKINGTALK_DB", os.path.join(_HERE, "chat.db"))
HISTORY_DEFAULT_LIMIT = 50

# Channels
DEFAULT_CHANNEL = "general"

# Validation
USERNAME_MAX_LEN = 32
CHANNEL_MAX_LEN = 32
PASSWORD_MIN_LEN = 4
PASSWORD_MAX_LEN = 128
MESSAGE_MAX_LEN = 2000

# File transfer (direct peer-to-peer; the server only brokers the handshake)
FILE_CHUNK_SIZE = 256 * 1024
# 8 GiB default ceiling; raise with VIKINGTALK_MAX_FILE_SIZE if you need more
MAX_FILE_SIZE = int(os.environ.get("VIKINGTALK_MAX_FILE_SIZE", str(8 * 1024**3)))
DOWNLOADS_DIR = os.environ.get(
    "VIKINGTALK_DOWNLOADS", os.path.join(_HERE, "downloads")
)
# How long an unanswered offer stays open, and how long the receiver keeps
# its one-shot port open waiting for the sender to connect.
TRANSFER_OFFER_TIMEOUT = 180
TRANSFER_CONNECT_TIMEOUT = 60
# Guard rails so a peer cannot exhaust server memory with bogus offers
MAX_PENDING_OFFERS_PER_USER = 8
FILENAME_MAX_LEN = 200

# Client reconnect
RECONNECT_BASE_DELAY = 1.0
RECONNECT_MAX_DELAY = 30.0
RECONNECT_MAX_ATTEMPTS = 8
