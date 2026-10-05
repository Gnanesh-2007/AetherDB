#!/bin/sh
set -e

# Ensure data directory exists and has correct permissions
mkdir -p /data
if [ "$(id -u)" = "0" ]; then
    chown -R aether:aether /data /app 2>/dev/null || true
    chmod 775 /data 2>/dev/null || true
    exec gosu aether "$@"
fi

exec "$@"
