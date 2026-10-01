#!/usr/bin/env python3
"""
VikingTalk launcher: works on Windows, macOS and Linux.

On first run it creates a virtual environment for *this* operating system
(.venv-windows / .venv-macos / .venv-linux), installs requirements.txt, and
then starts the app inside it. Later runs start immediately.

Usage:
  python start.py                     interactive menu
  python start.py server [host] [port]
  python start.py client [host] [port]   (no host = find server on the LAN)
  python start.py host                 run a server in the background and join it
  python start.py test                 self-test on this machine
"""

import hashlib
import os
import socket
import subprocess
import sys
import tempfile
import time

MIN_PYTHON = (3, 9)
HERE = os.path.dirname(os.path.abspath(__file__))
REQUIREMENTS = os.path.join(HERE, "requirements.txt")

if sys.platform == "win32":
    OS_NAME = "windows"
elif sys.platform == "darwin":
    OS_NAME = "macos"
else:
    OS_NAME = "linux"

VENV_DIR = os.path.join(HERE, ".venv-" + OS_NAME)
if OS_NAME == "windows":
    VENV_PYTHON = os.path.join(VENV_DIR, "Scripts", "python.exe")
else:
    VENV_PYTHON = os.path.join(VENV_DIR, "bin", "python")
STAMP = os.path.join(VENV_DIR, ".requirements.sha256")


def say(msg):
    print("[VikingTalk] " + msg, flush=True)


def fail(msg):
    print("\n[VikingTalk] ERROR: " + msg, file=sys.stderr, flush=True)
    sys.exit(1)


def requirements_hash():
    with open(REQUIREMENTS, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def venv_python_works():
    if not os.path.exists(VENV_PYTHON):
        return False
    try:
        return subprocess.call(
            [VENV_PYTHON, "-c", "import sys"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ) == 0
    except OSError:
        return False


def create_venv():
    import shutil
    import venv

    if os.path.exists(VENV_DIR):
        say("Existing environment is broken (Python was probably upgraded); rebuilding.")
        shutil.rmtree(VENV_DIR, ignore_errors=True)
    say("First run on %s: creating virtual environment in %s ..."
        % (OS_NAME, os.path.basename(VENV_DIR)))
    try:
        venv.EnvBuilder(with_pip=True).create(VENV_DIR)
    except Exception as exc:
        shutil.rmtree(VENV_DIR, ignore_errors=True)
        hint = ""
        if OS_NAME == "linux":
            hint = ("\nOn Debian/Ubuntu install the venv module first:\n"
                    "    sudo apt install python3-venv")
        fail("Could not create the virtual environment: %s%s" % (exc, hint))


def install_requirements():
    say("Installing dependencies (needs internet once) ...")
    cmd = [VENV_PYTHON, "-m", "pip", "install", "--disable-pip-version-check",
           "-q", "-r", REQUIREMENTS]
    if subprocess.call(cmd) != 0:
        fail("Installing dependencies failed. Check your internet connection "
             "and run the start script again.")
    with open(STAMP, "w") as f:
        f.write(requirements_hash())
    say("Environment ready.")


def ensure_venv():
    if not venv_python_works():
        create_venv()
    try:
        with open(STAMP) as f:
            up_to_date = f.read().strip() == requirements_hash()
    except OSError:
        up_to_date = False
    if not up_to_date:
        install_requirements()


def run_in_venv(args, **kwargs):
    """Run a project script with the venv's Python; Ctrl+C goes to the child."""
    import signal

    proc = subprocess.Popen([VENV_PYTHON] + args, cwd=HERE, **kwargs)
    # If the launcher itself is told to stop (kill, closed terminal), pass it on
    # so the server/client never keeps running orphaned in the background.
    for name in ("SIGTERM", "SIGHUP"):
        if hasattr(signal, name):
            signal.signal(getattr(signal, name),
                          lambda signum, _frame: proc.send_signal(signum))
    while True:
        try:
            return proc.wait()
        except KeyboardInterrupt:
            # The child got the same Ctrl+C and is shutting down; wait for it.
            continue


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_port(port, proc, timeout=15.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return False
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.2)
    return False


def stop_process(proc):
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def cmd_host(args):
    """Start a server in the background, then join it from this terminal."""
    port = int(args[0]) if args else 5555
    log_path = os.path.join(HERE, "server.log")
    log = open(log_path, "a")
    env = dict(os.environ, VIKINGTALK_PORT=str(port))
    server = subprocess.Popen(
        [VENV_PYTHON, "server.py", "0.0.0.0", str(port)],
        cwd=HERE, stdout=log, stderr=subprocess.STDOUT, env=env,
    )
    try:
        if not wait_for_port(port, server):
            log.close()
            fail("The server did not start. See server.log (is port %d in use?)" % port)
        lan_ip = None
        try:
            sys.path.insert(0, HERE)
            from discovery import guess_lan_ip
            lan_ip = guess_lan_ip()
        except Exception:
            pass
        say("Server running in the background (log: server.log).")
        if lan_ip:
            say("Other devices: run the start script and choose 'Join'. "
                "Manual connect: start.bat client %s %d (Windows) or "
                "./start.sh client %s %d (macOS/Linux)" % (lan_ip, port, lan_ip, port))
        say("Closing this chat also stops the server.\n")
        return run_in_venv(["client.py", "127.0.0.1", str(port)])
    finally:
        stop_process(server)
        log.close()


def cmd_test():
    """Start a throwaway server and run the multi-client test against it."""
    port = free_port()
    disc_port = free_port()
    db_path = os.path.join(tempfile.mkdtemp(prefix="vikingtalk-test-"), "test.db")
    env = dict(os.environ, VIKINGTALK_PORT=str(port),
               VIKINGTALK_DISCOVERY_PORT=str(disc_port), VIKINGTALK_DB=db_path)
    say("Starting a temporary test server on port %d ..." % port)
    server = subprocess.Popen(
        [VENV_PYTHON, "server.py", "127.0.0.1", str(port)],
        cwd=HERE, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        if not wait_for_port(port, server):
            fail("Test server did not start.")
        code = run_in_venv(["test_concurrent.py"], env=env)
    finally:
        stop_process(server)
    say("Self-test %s." % ("PASSED" if code == 0 else "FAILED"))
    return code


def menu():
    print()
    print("  ==============================")
    print("         V I K I N G T A L K")
    print("  ==============================")
    print("  1) Host a chat AND join it   (do this on ONE device)")
    print("  2) Join a chat on the network (all other devices)")
    print("  3) Run server only")
    print("  4) Self-test")
    print("  q) Quit")
    try:
        choice = input("\n  Choose [1-4]: ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        return None
    return {"1": "host", "2": "client", "3": "server", "4": "test"}.get(choice)


def main():
    if sys.version_info < MIN_PYTHON:
        fail("Python %d.%d or newer is required (you have %s). "
             "Download it from https://www.python.org/downloads/"
             % (MIN_PYTHON + (sys.version.split()[0],)))

    args = sys.argv[1:]
    mode = args.pop(0).lower() if args else None
    if mode in ("-h", "--help", "help"):
        print(__doc__)
        return 0
    if mode is None:
        mode = menu()
        if mode is None:
            return 0
    if mode not in ("server", "client", "host", "test"):
        fail("Unknown mode '%s'. Use: server, client, host or test." % mode)

    ensure_venv()

    if mode == "server":
        return run_in_venv(["server.py"] + args)
    if mode == "client":
        return run_in_venv(["client.py"] + args)
    if mode == "host":
        return cmd_host(args)
    return cmd_test()


if __name__ == "__main__":
    sys.exit(main() or 0)
