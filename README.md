# VikingTalk

A terminal chat app in Python for **Windows, macOS and Linux**. One device hosts the chat; every other device on the same Wi-Fi/LAN joins it and everyone chats in real time, whatever operating system they use.

## Features

- Works the same on Windows, macOS and Linux, and they can all chat with each other
- One-command start: the launcher sets up its own Python environment on first run
- Finds the server on your network automatically (no IP typing needed)
- User registration and login (bcrypt password hashing)
- Public channels with join/create/leave, plus private messages
- SQLite message history (last 50 messages on channel join)
- Colored terminal UI that keeps your half-typed message when new messages arrive
- Heartbeat keep-alive, graceful shutdown, automatic client reconnect

---

## Requirements

- **Python 3.9 or newer**, which is the only thing you install yourself:
  - **Windows:** <https://www.python.org/downloads/>. Tick **"Add python.exe to PATH"** in the installer.
  - **macOS:** <https://www.python.org/downloads/> or `brew install python`
  - **Linux (Debian/Ubuntu):** `sudo apt install python3 python3-venv`
- Internet the **first** time you start it on each device (to download 2 small packages)
- All devices on the **same Wi-Fi / LAN** to chat between devices

---

## Quick Start

### 1. Get the code

```bash
git clone https://github.com/ubbelothbrok/CLI_chatting.git
cd CLI_chatting
```

### 2. Start it

| OS | Command |
|----|---------|
| **Windows** | `start.bat` (or double-click `start.bat` in Explorer) |
| **macOS** | `./start.sh` (or double-click `start.command` in Finder) |
| **Linux** | `./start.sh` |

The first run creates a virtual environment for that OS (`.venv-windows`, `.venv-macos` or `.venv-linux`) and installs the dependencies. Later runs start instantly. You never need to create or activate a venv yourself.

You'll see a menu:

```text
  1) Host a chat AND join it   (do this on ONE device)
  2) Join a chat on the network (all other devices)
  3) Run server only
  4) Self-test
```

### 3. Chat across devices

1. On **one** device choose **1**. It starts the server and opens a chat window.
2. On **every other** device (any OS) choose **2**. It finds the server on the network by itself.
3. Everyone registers and logs in, then just types:

   ```text
   /register alice secret1
   /login alice secret1
   Hello everyone!
   ```

Closing the host's chat (`/quit`) also stops the server.

> **Why isn't there a `.venv` in the repo?** A virtual environment contains compiled files for one OS and one Python install, so a Linux `.venv` cannot run on Windows or macOS. The start scripts build the right one on each device automatically, and `.gitignore` keeps them out of git.

---

## Command-Line Options

The menu is optional. You can pass a mode directly. On Windows replace `./start.sh` with `start.bat` (typing just `start` runs a different, built-in Windows command):

```bash
./start.sh host                      # server in background + chat (port 5555)
./start.sh client                    # join: find the server automatically
./start.sh client 192.168.1.20       # join a specific server IP
./start.sh client 192.168.1.20 6000  # ...on a specific port
./start.sh server                    # server only (e.g. an always-on machine)
./start.sh server 0.0.0.0 6000       # server on a custom host/port
./start.sh test                      # self-test: temporary server + 5 bot clients
```

---

## If Devices Can't Find Each Other

Automatic discovery uses a UDP broadcast on port **5556**, and chat uses TCP port **5555**. If option 2 says *"No server found"*:

1. Make sure both devices are on the **same Wi-Fi** (guest networks often block device-to-device traffic).
2. Connect by IP instead. The host prints its address when it starts:

   ```text
   Server LAN IP: 192.168.1.20  (port 5555)
   ```

   Then on the other device run `start.bat client 192.168.1.20` (Windows) or `./start.sh client 192.168.1.20` (macOS/Linux).
3. Allow the server through the **firewall on the host device**:
   - **Windows:** when the "Windows Defender Firewall" popup appears, tick **Private networks** and click *Allow*. If you missed it: *Windows Security → Firewall → Allow an app → Python*.
   - **macOS:** click *Allow* on the "accept incoming connections" popup, or *System Settings → Network → Firewall → Options → allow Python*.
   - **Linux (ufw):** `sudo ufw allow 5555/tcp && sudo ufw allow 5556/udp`
4. Find the host IP manually if needed: `ipconfig` (Windows), `ipconfig getifaddr en0` (macOS), `hostname -I` (Linux).

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
├── start.py            # Cross-platform launcher: builds the per-OS venv, menu, modes
├── start.bat           # Windows wrapper (double-click)
├── start.sh            # macOS/Linux wrapper
├── start.command       # macOS Finder double-click wrapper
├── server.py           # Asyncio TCP server
├── client.py           # Terminal client (Windows + Unix keyboard handling)
├── discovery.py        # UDP broadcast server discovery on the LAN
├── protocol.py         # Length-prefixed JSON framing
├── database.py         # SQLite + bcrypt credentials
├── config.py           # Host, ports, limits, heartbeat
├── test_concurrent.py  # Multi-client test (public + private messages, discovery)
└── requirements.txt    # Python dependencies (colorama, bcrypt)
```

Created at runtime and **not** committed: `.venv-<os>/`, `chat.db` (accounts and history), `server.log`.

---

## Configuration

Edit `config.py` to change defaults (restart the server afterwards):

| Setting | Default | Description |
|---------|---------|-------------|
| `HOST` | `0.0.0.0` | Server bind address (`0.0.0.0` = reachable from the LAN, `127.0.0.1` = this machine only) |
| `PORT` | `5555` | Chat TCP port |
| `DISCOVERY_PORT` | `5556` | UDP port for automatic discovery |
| `DEFAULT_CHANNEL` | `general` | Channel users join on login |
| `HISTORY_DEFAULT_LIMIT` | `50` | Messages sent on channel join |
| `HEARTBEAT_INTERVAL` | `30` | Seconds between pings |
| `HEARTBEAT_TIMEOUT` | `60` | Disconnect if no pong within this time |
| `RECONNECT_MAX_ATTEMPTS` | `8` | Maximum automatic reconnect attempts |

The environment variables `VIKINGTALK_PORT`, `VIKINGTALK_DISCOVERY_PORT` and `VIKINGTALK_DB` override the port, discovery port and database path.

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
| `Python 3 was not found` | Install Python (see Requirements). On Windows tick "Add python.exe to PATH". |
| `Could not create the virtual environment` (Linux) | `sudo apt install python3-venv`, then start again |
| `Installing dependencies failed` | The first run needs internet; connect and start again |
| `No server found automatically` | See "If Devices Can't Find Each Other" above |
| `Could not start server ... address already in use` | A server is already running, or use another port: `./start.sh server 0.0.0.0 6000` (`start.bat` on Windows) |
| `User already logged in` | That username is active elsewhere; use `/logout` or pick another name |
| `Invalid username or password` | Register first with `/register`, then `/login` |
| `Channel does not exist` | Create it with `/create <name>` first |
| `./start.sh: Permission denied` | `chmod +x start.sh start.command` |
| Something broke after a Python upgrade | Delete the `.venv-<os>` folder; the next start rebuilds it |

---

## Security Notes

- Passwords are hashed with bcrypt; never stored in plaintext.
- This app is intended for **local/LAN use**. Do not expose port 5555 to the public internet without TLS and additional hardening.
- There is no encryption on the wire; messages travel as plain TCP on your network.
- Keep `chat.db` private: it contains password hashes, usernames, and message history.

---

## License

Use and modify freely for learning and local development.
