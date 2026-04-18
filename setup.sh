#!/usr/bin/env bash
set -euo pipefail

# --- validation ---
if [[ $# -lt 1 ]]; then
  echo "Usage: $(basename "$0") <source_file>" >&2
  exit 1
fi

SRC=$1
shift
EXTRA_ARGS=("$@")

if [[ ! -f "$SRC" ]]; then 
  echo "Error: source file not found: $SRC" >&2
  exit 1
fi

if [[ ! -r "$SRC" ]]; then
  echo "Error: source file is not readable: $SRC" >&2
  exit 1
fi

# --- setup ---
DEST=/dev/shm/$(basename "$SRC")
BS=$((8*1024*1024)) # 8M in bytes

# check ramdisk enough space
SRC_SIZE=$(stat -c%s "$SRC")
AVAILABLE=$(df --output=avail -B1 /dev/shm | tail -1)
if [[ $SRC_SIZE -gt $AVAILABLE ]]; then
    echo "Error: not enough space in /dev/shm (need $SRC_SIZE bytes, have $AVAILABLE)" >&2
    exit 1
fi

# cleanup on exit, interrupt, or error
cleanup() {
    local exit_code=$?
    if [[ $exit_code -ne 0 ]]; then
        echo "Error: script failed, cleaning up $DEST" >&2
        rm -f "$DEST"
    fi
    exit $exit_code
}
trap cleanup EXIT INT TERM

# --- copy ---
touch "$DEST"

if ! dd if="$SRC" of="$DEST" bs=8M status=progress; then
    echo "Error: dd copy failed" >&2
    exit 1
fi

# append exactly 8 null bytes
if ! dd if=/dev/zero of="$DEST" bs=1 count=8 conv=notrunc oflag=append; then
    echo "Error: zero-padding failed" >&2
    exit 1
fi

echo "Done: $SRC -> $DEST ($(stat -c%s "$DEST") bytes)"
