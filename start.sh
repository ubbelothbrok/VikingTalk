#!/bin/sh
# VikingTalk launcher for macOS and Linux.  Usage: ./start.sh [server|client|host|test] [args]
cd "$(dirname "$0")" || exit 1
for py in python3 python; do
    if command -v "$py" >/dev/null 2>&1; then
        exec "$py" start.py "$@"
    fi
done
echo "Python 3 was not found."
echo "  macOS:  install from https://www.python.org/downloads/  (or: brew install python)"
echo "  Linux:  sudo apt install python3 python3-venv   (Debian/Ubuntu)"
exit 1
