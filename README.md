# VikingTalk

A terminal-based chat application in Python. One device runs the server; other devices connect as clients over TCP and chat in real time.

## Features

- User registration and login (bcrypt password hashing)
- Public channels with join/create/leave
- Private messages between users
- SQLite message history (last 50 messages on channel join)
- Colored terminal UI with non-blocking input
- Asyncio server for concurrent clients
- Heartbeat keep-alive and graceful shutdown
- Automatic client reconnect with exponential backoff

---

## Requirements

- Python 3.10+
- `pip`
- Same local network for multi-device chat

---

## Quick Start (Single Machine)

### 1. Install dependencies

```bash
git clone https://github.com/ubbelothbrok/CLI_chatting.git
cd CLI_chatting
python3 -m venv .venv
source .venv/bin/activate          # macOS/Linux
# .venv\Scripts\activate           # Windows PowerShell

pip install -r requirements.txt
```

### 2. Start the server

```bash
python server.py
```

You should see:

```text
Chat server listening on ('0.0.0.0', 5555)
Other devices on your Wi-Fi can connect with:  python client.py <LAN-IP> 5555
```

### 3. Start a client (new terminal)

```bash
source .venv/bin/activate
python client.py
```

### 4. Register and log in

```text
/register alice secret1
/login alice secret1
```

Then type normally to chat in `#general`.

---

## Multi-Device Setup (One Device Talks to Others)

Use this when one Mac/PC hosts the server and phones/laptops/other PCs connect as clients.

### On the host device (server machine)

1. Keep the server running:

   ```bash
   python server.py
   ```

2. Note the **LAN IP** printed in the server log, for example:

   ```text
   python client.py 10.33.138.229 5555
   ```

   If it is not shown, find your host IP with one of these commands:

   ```bash
   hostname -I                 # Linux
   ipconfig getifaddr en0      # macOS
   ```

### On each other device (client machines)

1. Copy this project folder to the device (or clone it).
2. Install dependencies (same as Quick Start).
3. Connect using the **host's LAN IP**, not `127.0.0.1`:

   ```bash
   python client.py 10.33.138.229 5555
   ```

   Replace `10.33.138.229` with your host machine's actual IP.

4. Register and log in with a unique username. User accounts and message history are stored in the server's `chat.db` file:

   ```text
   /register bob secret1
   /login bob secret1
   ```

### Important rules

| Do | Don't |
|----|-------|
| Use the host's LAN IP on other devices | Use `127.0.0.1` on other devices |
| Keep all devices on the same Wi-Fi | Expect it to work across different networks without extra setup |
| Restart the server after changing `config.py` | Assume old server settings apply |

### If connection fails

1. Confirm the server is running on the host.
2. Confirm both devices are on the same Wi-Fi.
3. On macOS, allow Python through the firewall:
   **System Settings → Network → Firewall → Options → allow Python**
4. Try pinging the host IP from the client device.

---

## Commands

All commands start with `/`.

| Command | Syntax | Description |
|---------|--------|-------------|
| `/register` | `/register <user> <pass>` | Create a new account |
| `/login` | `/login <user> <pass>` | Log in to the server |
| `/logout` | `/logout` | Log out (stay connected) |
| `/msg` | `/msg <user> <text>` | Send a private message |
| `/create` | `/create <channel>` | Create a new channel |
| `/join` | `/join <channel>` | Switch to a channel |
| `/leave` | `/leave` | Return to `#general` |
| `/users` | `/users` | List online users and their channels |
| `/channels` | `/channels` | List all channels with online counts |
| `/history` | `/history [N]` | Show last N messages (default 50) |
| `/help` | `/help` | Show command help |
| `/quit` | `/quit` | Exit the client |

**Chat messages:** type any line without `/` to broadcast to your current channel.

### Examples

```text
/register alice secret1
/login alice secret1
Hello everyone!
/msg bob Are you free later?
/create gaming
/join gaming
/users
/channels
/history 20
/quit
```

---

## Validation Rules

- **Username:** letters, numbers, underscore only; 1–32 characters
- **Password:** 4–128 characters
- **Channel name:** letters, numbers, underscore only; 1–32 characters
- **Message length:** up to 2000 characters

---

## Terminal Colors

| Message type | Color |
|--------------|-------|
| System (join/leave, errors) | Cyan |
| Private messages | Yellow |
| Your own messages | Green |
| Other users' messages | White |

Prompt format:

```text
[general] @alice >
```

---

## Project Structure

```text
CLI_chatting/
├── server.py           # Asyncio TCP server
├── client.py           # Terminal client with prompt redraw
├── protocol.py         # Length-prefixed JSON framing
├── database.py         # SQLite + bcrypt credentials
├── config.py           # Host, port, limits, heartbeat
├── test_concurrent.py  # Multi-client smoke test
├── requirements.txt    # Python dependencies
├── chat.db             # SQLite database (created and updated at runtime)
└── README.md           # This file
```

---

## Configuration

Edit `config.py` to change defaults:

| Setting | Default | Description |
|---------|---------|-------------|
| `HOST` | `0.0.0.0` | Server bind address (`0.0.0.0` = all interfaces) |
| `PORT` | `5555` | Server port |
| `DEFAULT_CHANNEL` | `general` | Channel users join on login |
| `HISTORY_DEFAULT_LIMIT` | `50` | Messages sent on channel join |
| `HEARTBEAT_INTERVAL` | `30` | Seconds between pings |
| `HEARTBEAT_TIMEOUT` | `60` | Disconnect if no pong within this time |
| `RECONNECT_BASE_DELAY` | `1.0` | Initial client reconnect delay in seconds |
| `RECONNECT_MAX_DELAY` | `30.0` | Maximum client reconnect delay in seconds |
| `RECONNECT_MAX_ATTEMPTS` | `8` | Maximum automatic reconnect attempts |

### Local-only mode

To restrict the server to this machine only, change in `config.py`:

```python
HOST = "127.0.0.1"
```

Then clients on the same machine connect with:

```bash
python client.py
```

---

## Custom Host and Port

**Server:**

```bash
python server.py 0.0.0.0 5555
```

**Client:**

```bash
python client.py 10.33.138.229 5555
```

Arguments: `python server.py [host] [port]` and `python client.py [host] [port]`.

---

## Testing with Multiple Clients

Start the server in one terminal, then run the smoke test in another:

```bash
python3 test_concurrent.py 127.0.0.1 5555 5
```

The optional arguments are `[host] [port] [num_clients]`; the default is the configured host and port with 5 clients. The test registers bots, logs them in, sends messages, checks broadcasts, and exits with a non-zero status if any client fails.

For manual testing, open several terminals and run `python client.py` in each.

---

## Protocol Overview

- **Transport:** TCP over IPv4
- **Framing:** 4-byte big-endian length prefix + UTF-8 JSON body
- **Message types:** `AUTH_REQUEST`, `AUTH_RESPONSE`, `PUBLIC_MSG`, `PRIVATE_MSG`, `SYSTEM_MSG`, `CMD_REQUEST`, `CMD_RESPONSE`, `HISTORY_RESPONSE`, `PING`, `PONG`

Example frame:

```json
{
  "type": "PUBLIC_MSG",
  "payload": {
    "channel": "general",
    "username": "alice",
    "content": "Hello everyone!",
    "timestamp": "2026-09-08 16:30:00"
  }
}
```

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `Connection failed` | Start the server first; check IP and port |
| `User already logged in` | That username is active elsewhere; use `/logout` or pick another name |
| `Invalid username or password` | Register first with `/register`, then `/login` |
| `User 'X' not found` | Target user is offline or username is wrong |
| `Channel does not exist` | Create it with `/create <name>` first |
| Other device can't connect | Same Wi-Fi, correct LAN IP, firewall allows Python |
| Port already in use | Stop old server or change `PORT` in `config.py` |

---

## Security Notes

- Passwords are hashed with bcrypt; never stored in plaintext.
- This app is intended for **local/LAN use**. Do not expose port 5555 to the public internet without TLS and additional hardening.
- There is no encryption on the wire; messages travel as plain TCP on your network.
- Keep `chat.db` private: it contains password hashes, usernames, and message history.

---

## License

Use and modify freely for learning and local development.
