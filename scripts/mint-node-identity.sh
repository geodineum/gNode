#!/bin/bash
#
# Mint a per-node ValKey identity for a node that has ALREADY joined.
#
# `constellation expand` mints an identity as part of enrolling a NEW node,
# and its add_peer step is not idempotent: it appends a second [Peer] block,
# allocates a fresh VPN IP, and leaves the previous peer live and stale. So
# expand must never be re-run against a working member just to issue it a
# credential.
#
# This does the credential half only. It touches no WireGuard state, no peer
# registry, and nothing the node is currently using to stay connected.
#
# Run ON THE MASTER:
#   sudo ./mint-node-identity.sh <node-name> [--rotate]
#
# Grants come from acl-daemon-tier.rule -- the same single definition expand
# reads. If that file is missing, run write-acl-daemon-tier-rule.sh first.
#
# Idempotent by default: an existing identity is reported, not replaced.
# --rotate issues a new password, which BREAKS the node until it is updated.
set -uo pipefail

NODE="${1:-}"
ROTATE="false"
[ "${2:-}" = "--rotate" ] && ROTATE="true"
[ -n "$NODE" ] || { echo "Usage: sudo $0 <node-name> [--rotate]" >&2; exit 1; }

# Match the sanitisation expand applies, so a name mints the same user here.
SAFE="$(printf '%s' "$NODE" | tr -c 'a-zA-Z0-9_' '_')"
USER_NAME="gnode_node_${SAFE}"

CREDS="${GNODE_CREDENTIAL_DIR:-/etc/geodineum/credentials}"
RULE_FILE="/etc/geodineum/components/gnode-daemon/acl-daemon-tier.rule"

[ -r /etc/geodineum/bootstrap.env ] && . /etc/geodineum/bootstrap.env
HOST="${VALKEY_HOST:-127.0.0.1}"
PORT="${VALKEY_PORT:-47445}"

PW_FILE="${CREDS}/valkey.password"
[ -r "$PW_FILE" ] || PW_FILE="${CREDS}/valkey_admin.password"
[ -r "$PW_FILE" ] || { echo "FATAL: no admin credential — run as root ON THE MASTER." >&2; exit 1; }

if [ ! -r "$RULE_FILE" ]; then
    echo "FATAL: ${RULE_FILE} not found." >&2
    echo "       Run write-acl-daemon-tier-rule.sh first. The grant is never" >&2
    echo "       typed by hand -- one definition of the privilege boundary." >&2
    exit 1
fi

vk() { REDISCLI_AUTH="$(cat "$PW_FILE")" valkey-cli -h "$HOST" -p "$PORT" "$@"; }

EXISTS="false"
vk ACL GETUSER "$USER_NAME" 2>/dev/null | grep -q . && EXISTS="true"

if [ "$EXISTS" = "true" ] && [ "$ROTATE" != "true" ]; then
    echo "Identity ${USER_NAME} already exists."
    echo "Its password is held only by the node; it is not recoverable here."
    echo "Re-issue with --rotate ONLY if the node has lost it: rotating breaks"
    echo "that node's ValKey auth until the new password is installed on it."
    exit 0
fi

NODE_PW="$(head -c 32 /dev/urandom | base64 | tr -d '/+=' | head -c 32)"

# Unquoted on purpose: the rule file holds whitespace-separated ACL tokens
# that must arrive as separate arguments.
# valkey-cli exits 0 even when the server rejects the command, so the reply
# is checked rather than the exit status.
# shellcheck disable=SC2046
SETOUT="$(vk ACL SETUSER "$USER_NAME" resetpass ">${NODE_PW}" $(cat "$RULE_FILE") 2>&1)"
if [ "$SETOUT" != "OK" ]; then
    echo "FATAL: ACL SETUSER ${USER_NAME} rejected by the server:" >&2
    echo "       ${SETOUT}" >&2
    echo "       Grant used: $(cat "$RULE_FILE")" >&2
    exit 1
fi
vk ACL SAVE >/dev/null 2>&1 || true

# Prove the grant landed rather than trusting SETUSER's OK.
if ! vk ACL DRYRUN "$USER_NAME" PING >/dev/null 2>&1; then
    echo "WARNING: ${USER_NAME} created but fails a DRYRUN PING — inspect:" >&2
    echo "         ACL GETUSER ${USER_NAME}" >&2
fi

echo "Minted ${USER_NAME} ($([ "$EXISTS" = "true" ] && echo rotated || echo new))"
echo "Grant:  $(cat "$RULE_FILE")"
echo
echo "=== Run these ON ${NODE} (the password is shown once) ==="
cat <<APPLY
sudo install -d -m 0755 -o gnode -g gnode /etc/geodineum/components/gnode-daemon
printf '%s' '${NODE_PW}' | sudo tee /etc/geodineum/credentials/valkey_node.password >/dev/null
sudo chown gnode:geodineum-creds /etc/geodineum/credentials/valkey_node.password
sudo chmod 0640 /etc/geodineum/credentials/valkey_node.password
printf 'VALKEY_NODE_USER=%s\n' '${USER_NAME}' | sudo tee /etc/geodineum/components/gnode-daemon/node-identity.env >/dev/null
sudo chown root:gnode /etc/geodineum/components/gnode-daemon/node-identity.env
sudo chmod 0640 /etc/geodineum/components/gnode-daemon/node-identity.env

# Point daemon.env at the identity, then restart onto it.
sudo /opt/geodineum/gNode/scripts/install-gnode-service.sh
sudo systemctl restart gnode-daemon

# Verify: expect user=${USER_NAME}
sudo journalctl -u gnode-daemon -n 20 --no-pager | grep -iE 'auth|valkey|error'
APPLY
echo
echo "=== Then verify FROM THE MASTER ==="
echo "  valkey-cli ... CLIENT LIST | grep -o 'user=[^ ]*' | sort | uniq -c"
echo "  (${USER_NAME} should appear once the node reconnects)"
echo
echo "=== Rollback on ${NODE}, if it fails to authenticate ==="
echo "  sudo sed -i 's|^VALKEY_USER=.*|VALKEY_USER=\"gnode_daemon\"|' \\"
echo "    /etc/geodineum/components/gnode-daemon/daemon.env"
echo "  sudo sed -i 's|^VALKEY_PASSWORD_FILE=.*|VALKEY_PASSWORD_FILE=\"/etc/geodineum/credentials/valkey_daemon.password\"|' \\"
echo "    /etc/geodineum/components/gnode-daemon/daemon.env"
echo "  sudo rm -f /etc/geodineum/components/gnode-daemon/node-identity.env"
echo "  sudo systemctl restart gnode-daemon"
