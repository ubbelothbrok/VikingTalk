"""Shared configuration for the CLI chat application."""

# 0.0.0.0 = accept connections from other devices on your network
# Clients should connect to this machine's LAN IP (not 0.0.0.0).
HOST = "0.0.0.0"
PORT = 5555

# Protocol limits
MAX_MESSAGE_SIZE = 65536  # 64 KiB payload cap
LENGTH_PREFIX_SIZE = 4

# Heartbeat / keep-alive (seconds)
HEARTBEAT_INTERVAL = 30
HEARTBEAT_TIMEOUT = 60

# Persistence
DB_PATH = "chat.db"
HISTORY_DEFAULT_LIMIT = 50

# Channels
DEFAULT_CHANNEL = "general"

# Validation
USERNAME_MAX_LEN = 32
CHANNEL_MAX_LEN = 32
PASSWORD_MIN_LEN = 4
PASSWORD_MAX_LEN = 128
MESSAGE_MAX_LEN = 2000

# Client reconnect
RECONNECT_BASE_DELAY = 1.0
RECONNECT_MAX_DELAY = 30.0
RECONNECT_MAX_ATTEMPTS = 8
