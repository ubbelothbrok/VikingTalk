#!/bin/sh
# macOS: double-click this file in Finder to start VikingTalk.
cd "$(dirname "$0")" || exit 1
exec ./start.sh "$@"
