#!/bin/sh
set -eu

administrator_key_source=/openwa-data/.api-key
runtime_directory=/run/codex-whatsapp
administrator_key_target=$runtime_directory/openwa-administrator-key
bridge_uid=${CODEX_BRIDGE_UID:-1000}
bridge_gid=${CODEX_BRIDGE_GID:-1000}
attempt=0

while [ ! -s "$administrator_key_source" ]; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 60 ]; then
        echo "OpenWA administrator key was not created within 60 seconds" >&2
        exit 1
    fi
    sleep 1
done

install -d -m 0700 -o "$bridge_uid" -g "$bridge_gid" "$runtime_directory"
install -m 0600 -o "$bridge_uid" -g "$bridge_gid" \
    "$administrator_key_source" "$administrator_key_target"

exec setpriv \
    --reuid="$bridge_uid" \
    --regid="$bridge_gid" \
    --clear-groups \
    codex-whatsapp-bridge "$@"
