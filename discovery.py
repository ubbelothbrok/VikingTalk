"""UDP broadcast discovery so clients can find a server on the LAN without an IP."""

from __future__ import annotations

import asyncio
import json
import socket
import time

import config

DISCOVER_REQUEST = b"VIKINGTALK_DISCOVER"


def guess_lan_ip() -> str | None:
    """Best-effort LAN IP of this machine (no packets are actually sent)."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("10.255.255.255", 1))
            ip = s.getsockname()[0]
            if not ip.startswith("127."):
                return ip
    except OSError:
        pass
    try:
        ip = socket.gethostbyname(socket.gethostname())
        if not ip.startswith("127."):
            return ip
    except OSError:
        pass
    return None


class _DiscoveryResponder(asyncio.DatagramProtocol):
    def __init__(self, tcp_port: int) -> None:
        self.tcp_port = tcp_port
        self.transport: asyncio.DatagramTransport | None = None

    def connection_made(self, transport) -> None:
        self.transport = transport

    def datagram_received(self, data: bytes, addr) -> None:
        if data.strip() != DISCOVER_REQUEST or self.transport is None:
            return
        reply = json.dumps({"service": "VikingTalk", "port": self.tcp_port})
        self.transport.sendto(reply.encode("utf-8"), addr)


async def start_responder(
    tcp_port: int, bind_host: str = "0.0.0.0"
) -> asyncio.DatagramTransport:
    """
    Answer discovery broadcasts on the same interface(s) the chat server
    listens on, so a loopback-only server is never advertised to the LAN.
    Raises OSError if the UDP port is unavailable.
    """
    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(
        lambda: _DiscoveryResponder(tcp_port),
        local_addr=(bind_host or "0.0.0.0", config.DISCOVERY_PORT),
        family=socket.AF_INET,
    )
    return transport


def find_servers(timeout: float = config.DISCOVERY_TIMEOUT) -> list[tuple[str, int]]:
    """Broadcast a discovery request and return [(ip, port), ...] of servers that answer."""
    targets = {"255.255.255.255", "127.0.0.1"}
    lan_ip = guess_lan_ip()
    if lan_ip:
        # Subnet broadcast for the common /24 home network; helps on macOS
        targets.add(lan_ip.rsplit(".", 1)[0] + ".255")

    found: list[tuple[str, int]] = []
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind(("", 0))
        for target in targets:
            try:
                sock.sendto(DISCOVER_REQUEST, (target, config.DISCOVERY_PORT))
            except OSError:
                pass

        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            sock.settimeout(remaining)
            try:
                data, addr = sock.recvfrom(1024)
            except socket.timeout:
                break
            except OSError:
                # Windows reports ICMP "port unreachable" as a recv error; keep listening
                continue
            try:
                info = json.loads(data.decode("utf-8"))
                port = int(info["port"])
            except (ValueError, KeyError, TypeError, UnicodeDecodeError):
                continue
            if info.get("service") != "VikingTalk":
                continue
            entry = (addr[0], port)
            if entry not in found:
                found.append(entry)

    # The same server can answer on loopback and on its LAN IP; prefer the LAN IP
    if lan_ip:
        found = [
            (ip, p) for ip, p in found
            if not (ip.startswith("127.") and (lan_ip, p) in found)
        ]
    return found
