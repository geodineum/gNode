#!/bin/bash
#
# Write /etc/geodineum/components/gnode-daemon/acl-daemon-tier.rule from the
# live gnode_daemon user.
#
# `constellation expand` mints each node a per-node ValKey identity of the
# daemon tier, and reads the grant from that rule file rather than carrying
# its own copy — one definition of a privilege boundary. install.sh writes
# the file when it provisions gnode_daemon, so fresh installs have it.
#
# A master installed BEFORE that change does not, and `expand` there logs
# "no per-node identity minted" and falls back to the shared login. This
# brings such a master up to date without re-running the full installer,
# which is not a proportionate thing to do to a live master.
#
# The rule is DERIVED FROM THE RUNNING USER, never typed here: hand-copying
# the grant would recreate exactly the duplicate definition the rule file
# exists to remove.
#
# Usage:  sudo ./write-acl-daemon-tier-rule.sh [--dry-run]
set -uo pipefail

CREDS="${GNODE_CREDENTIAL_DIR:-/etc/geodineum/credentials}"
RULE_DIR="/etc/geodineum/components/gnode-daemon"
RULE_FILE="${RULE_DIR}/acl-daemon-tier.rule"
SRC_USER="${SRC_USER:-gnode_daemon}"
DRY_RUN="false"
[ "${1:-}" = "--dry-run" ] && DRY_RUN="true"

[ -r /etc/geodineum/bootstrap.env ] && . /etc/geodineum/bootstrap.env
HOST="${VALKEY_HOST:-127.0.0.1}"
PORT="${VALKEY_PORT:-47445}"

PW="${CREDS}/valkey.password"
[ -r "$PW" ] || PW="${CREDS}/valkey_admin.password"
[ -r "$PW" ] || { echo "FATAL: no admin credential readable — run as root ON THE MASTER." >&2; exit 1; }

# ACL LIST gives one line per user, which reconstructs into SETUSER tokens.
# ACL GETUSER's field/value layout spans lines per field and is far more
# fragile to parse.
RULE="$(REDISCLI_AUTH="$(cat "$PW")" valkey-cli -h "$HOST" -p "$PORT" ACL LIST 2>/dev/null \
    | awk -v u="$SRC_USER" '$1=="user" && $2==u {
        out="on resetkeys resetchannels"
        for (i=3;i<=NF;i++) {
            t=$i
            if (t ~ /^#/) continue                                # password hashes: identity, not grant
            if (t=="on"||t=="off"||t=="nopass") continue           # state, not grant
            if (t=="resetkeys"||t=="resetchannels"||t=="reset") continue
            if (t=="sanitize-payload"||t=="skip-sanitize-payload") continue
            out = out " " t
        }
        print out
    }')"

if [ -z "$RULE" ]; then
    echo "FATAL: user '${SRC_USER}' not found in ACL LIST on ${HOST}:${PORT}." >&2
    echo "       Is this the master, and has the installer provisioned the daemon user?" >&2
    exit 1
fi

# A rule that grants nothing, or everything with no restriction, means the
# parse went wrong. Refuse rather than mint node identities from garbage.
case "$RULE" in
    *"+@"*) : ;;
    *) echo "FATAL: derived rule has no command grant — refusing to write: ${RULE}" >&2; exit 1 ;;
esac

echo "Derived from live user '${SRC_USER}':"
echo "  ${RULE}"

if [ "$DRY_RUN" = "true" ]; then
    echo "(dry run — ${RULE_FILE} not written)"
    exit 0
fi

if [ -r "$RULE_FILE" ] && [ "$(cat "$RULE_FILE")" = "$RULE" ]; then
    echo "Already current: ${RULE_FILE}"
    exit 0
fi

install -d -m 0755 -o gnode -g gnode "$RULE_DIR" 2>/dev/null || mkdir -p "$RULE_DIR"
printf '%s\n' "$RULE" > "$RULE_FILE"
chown root:gnode "$RULE_FILE" 2>/dev/null || true
chmod 0640 "$RULE_FILE"
echo "Wrote ${RULE_FILE} (root:gnode 0640)"
echo "'geodineum constellation expand' will now mint per-node identities."
