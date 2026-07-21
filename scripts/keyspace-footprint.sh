#!/bin/bash
#
# Measure the ValKey keyspace a node actually touches.
#
# Narrowing an ACL from `~*` to a scoped pattern needs evidence of real
# access, not the keyspace the schema implies. This accumulates that
# evidence and validates a candidate user against it before anything is
# applied.
#
# Usage:
#   ./keyspace-footprint.sh sample [seconds]   capture a window (default 60)
#   ./keyspace-footprint.sh report             summarise the profile
#   ./keyspace-footprint.sh propose            candidate ACL keyspace
#   ./keyspace-footprint.sh dryrun <user>      replay observations at a user
#
# Environment:
#   VALKEY_HOST / VALKEY_PORT   from bootstrap.env when unset
#   KEYSPACE_PROFILE            profile path (default /var/lib/geodineum/keyspace-profile.tsv)
#   EXCLUDE_USERS               users to drop, space separated (default "default")
#
# SAMPLING, not continuous capture: MONITOR costs throughput, so this takes a
# short window and appends. Run hourly from the timer and the rare paths
# (nightly backup, cache purge, COMMS dispatch) appear without ever holding
# MONITOR open. One window is a sample of one minute; treat `propose` as
# meaningless until the profile spans days including a backup run.
#
# Absence is not evidence: a pattern missing from the profile was not
# observed, which is not the same as unused.
#
# The profile contains real key names, some embedding site ids. It is 0600
# and lives outside any repo. Do not commit it.
set -uo pipefail

CREDS="${GNODE_CREDENTIAL_DIR:-/etc/geodineum/credentials}"
PROFILE="${KEYSPACE_PROFILE:-/var/lib/geodineum/keyspace-profile.tsv}"
EXCLUDE_USERS="${EXCLUDE_USERS:-default}"
ACTION="${1:-sample}"
WINDOW="${2:-60}"

[ -r /etc/geodineum/bootstrap.env ] && . /etc/geodineum/bootstrap.env
HOST="${VALKEY_HOST:-127.0.0.1}"
PORT="${VALKEY_PORT:-47445}"

ADMIN_PW="${CREDS}/valkey.password"
[ -r "$ADMIN_PW" ] || ADMIN_PW="${CREDS}/valkey_admin.password"
[ -r "$ADMIN_PW" ] || { echo "FATAL: no admin credential readable (run as root)" >&2; exit 1; }

vk() { REDISCLI_AUTH="$(cat "$ADMIN_PW")" valkey-cli -h "$HOST" -p "$PORT" "$@"; }

excluded() {
    local u
    for u in $EXCLUDE_USERS; do [ "$1" = "$u" ] && return 0; done
    return 1
}

# Collapse a concrete key to the shape an ACL pattern must cover. Site ids and
# digests become wildcards; trailing segments that carry meaning (:production,
# :registry, :config) are preserved, because an ACL that ignores them is wider
# than it needs to be.
normalise_key() {
    sed -E \
        -e 's/\{[^}]*\}/{*}/g' \
        -e 's/(^|:)gnode:site:[^:]+/\1gnode:site:*/' \
        -e 's/:[0-9a-f]{16,}(:|$)/:*\1/gI' \
        -e 's/:[0-9]{6,}(:|$)/:*\1/g' 2>/dev/null
}

case "$ACTION" in
sample)
    echo "== sampling ${WINDOW}s on ${HOST}:${PORT} =="
    echo "   MONITOR reduces throughput for the duration. Keep windows short."

    # addr -> user. Parse CLIENT LIST field-exact: a greedy .*addr= matches
    # laddr= (the LOCAL address) and silently maps every client to a key no
    # MONITOR line can ever match.
    snapshot_clients() {
        vk CLIENT LIST 2>/dev/null | awk '
            { a=""; u="";
              for (i=1;i<=NF;i++) {
                  if ($i ~ /^addr=/) { a=substr($i,6) }
                  if ($i ~ /^user=/) { u=substr($i,6) }
              }
              if (a!="" && u!="") print a"\t"u
            }'
    }
    # PHP connects per request and disconnects, so a single snapshot at
    # window-start sees almost nothing. Resample throughout and union.
    MAPFILE_="$(mktemp)"; TMP="$(mktemp)"
    trap 'rm -f "$MAPFILE_" "$TMP" "${TMP}.tok"' EXIT
    ( i=0; while [ "$i" -lt "$WINDOW" ]; do snapshot_clients >> "$MAPFILE_"; sleep 1; i=$((i+1)); done ) &
    MAPPID_=$!
    snapshot_clients >> "$MAPFILE_"

    timeout "$WINDOW" env REDISCLI_AUTH="$(cat "$ADMIN_PW")" \
        valkey-cli -h "$HOST" -p "$PORT" MONITOR > "$TMP" 2>/dev/null
    kill "$MAPPID_" 2>/dev/null; wait "$MAPPID_" 2>/dev/null

    declare -A ADDR_USER
    while IFS="$(printf '\t')" read -r a u; do
        [ -n "$a" ] && ADDR_USER["$a"]="$u"
    done < <(sort -u "$MAPFILE_")
    echo "   captured $(wc -l < "$TMP") lines; ${#ADDR_USER[@]} distinct clients seen"

    # Split each line into src + quoted tokens. awk keeps escaped quotes
    # inside key names from truncating the token list.
    awk '{
        line=$0; src="";
        if (match(line, /\[[0-9]+ [^]]*\]/)) {
            f=substr(line, RSTART+1, RLENGTH-2); split(f, parts, " "); src=parts[2]
        }
        sub(/^[^]]*\] /, "", line)
        out=src; n=0
        while (match(line, /"([^"\\]|\\.)*"/)) {
            tok=substr(line, RSTART+1, RLENGTH-2); gsub(/\\"/, "\"", tok)
            out=out "\t" tok; n++
            line=substr(line, RSTART+RLENGTH)
        }
        if (n) print out
    }' "$TMP" > "${TMP}.tok"

    # Pass 1: resolve keys per UNIQUE signature. ValKey declares a command's
    # keys; guessing arg-1 yields COUNT/CREATE/SETINFO and function names.
    declare -A SIG_KEYS
    while IFS= read -r sig; do
        [ -z "$sig" ] && continue
        IFS="$(printf '\t')" read -r -a sargv <<<"$sig"
        out="$(vk COMMAND GETKEYS "${sargv[@]}" 2>/dev/null)"
        case "$out" in ERR*|*"no key"*) out="" ;; esac
        SIG_KEYS["$sig"]="$out"
    done < <(cut -f2- "${TMP}.tok" | sort -u)
    echo "   resolved keys for ${#SIG_KEYS[@]} distinct command signatures"

    # Pass 2: attribute. Commands issued from inside Lua report "lua" as the
    # source, but scripts execute atomically -- nothing interleaves between an
    # FCALL and its completion -- so every lua line belongs to the most recent
    # preceding client command. That attribution is exact, not heuristic, and
    # it matters because those commands are ACL-checked against the caller.
    mkdir -p "$(dirname "$PROFILE")" 2>/dev/null
    touch "$PROFILE"; chmod 0600 "$PROFILE"
    last_user=""
    kept=0; dropped=0
    while IFS="$(printf '\t')" read -r src rest; do
        [ -z "$rest" ] && continue
        if [ "$src" = "lua" ]; then
            obs_user="${last_user:-UNATTRIBUTED}"; origin="lua"
        else
            obs_user="${ADDR_USER[$src]:-UNATTRIBUTED}"; origin="client"
            [ "$obs_user" != "UNATTRIBUTED" ] && last_user="$obs_user"
        fi
        if excluded "$obs_user"; then dropped=$((dropped+1)); continue; fi
        kept=$((kept+1))
        cmd_u="$(printf '%s' "${rest%%$(printf '\t')*}" | tr 'a-z' 'A-Z')"
        keys="${SIG_KEYS[$rest]:-}"
        if [ -z "$keys" ]; then
            printf '%s\t%s\t\t%s\n' "$obs_user" "$cmd_u" "$origin" >> "$PROFILE"
        else
            while IFS= read -r k; do
                [ -z "$k" ] && continue
                printf '%s\t%s\t%s\t%s\n' "$obs_user" "$cmd_u" \
                    "$(printf '%s' "$k" | normalise_key)" "$origin" >> "$PROFILE"
            done <<<"$keys"
        fi
        case "$cmd_u" in
            FCALL*) fn="${rest#*$(printf '\t')}"; fn="${fn%%$(printf '\t')*}"
                    printf '%s\tFN:%s\t\t%s\n' "$obs_user" "$fn" "$origin" >> "$PROFILE" ;;
        esac
    done < "${TMP}.tok"
    echo "   kept ${kept}, dropped ${dropped} (excluded users: ${EXCLUDE_USERS})"
    echo "   profile now $(wc -l < "$PROFILE") observations -> $PROFILE"
    ;;

report)
    [ -s "$PROFILE" ] || { echo "No profile yet. Run: $0 sample" >&2; exit 1; }
    echo "== observations by user (client vs lua-internal) =="
    awk -F'\t' '{print $1"\t"$4}' "$PROFILE" | sort | uniq -c | sort -rn
    echo
    echo "== command surface by user =="
    awk -F'\t' '{print $1"\t"$2}' "$PROFILE" | sort -u | awk -F'\t' '{print $1}' \
        | uniq -c | sort -rn
    echo
    echo "== key shapes by user (the ACL-relevant output) =="
    awk -F'\t' '$3!=""{print $1"\t"$3}' "$PROFILE" | sort -u
    echo
    echo "Distinct shapes: $(awk -F'\t' '$3!=""{print $3}' "$PROFILE" | sort -u | wc -l)"
    echo "Window coverage: $(wc -l < "$PROFILE") observations accumulated"
    ;;

propose)
    [ -s "$PROFILE" ] || { echo "No profile yet. Run: $0 sample" >&2; exit 1; }
    echo "== candidate keyspace, per user (REVIEW; do not paste blindly) =="
    for u in $(awk -F'\t' '$3!=""{print $1}' "$PROFILE" | sort -u); do
        echo "  ${u}:"
        awk -F'\t' -v U="$u" '$1==U && $3!=""{print $3}' "$PROFILE" \
            | sed -E -e 's/^\{\*\}/*/' \
                     -e 's|^(\*:[^:]+):.*|\1:*|' \
                     -e 's|^([a-z]+:[a-z]+):.*|\1:*|' \
            | sort -u | sed 's/^/    ~/'
    done
    echo
    echo "Derived from $(wc -l < "$PROFILE") observations. A pattern absent here"
    echo "was NOT OBSERVED, which is not the same as unused. Gate with:"
    echo "  $0 dryrun <candidate_user>"
    ;;

dryrun)
    user="${2:-}"
    [ -n "$user" ] || { echo "Usage: $0 dryrun <user>" >&2; exit 1; }
    if ! vk ACL DRYRUN "$user" PING >/dev/null 2>&1; then
        echo "FATAL: ACL DRYRUN unavailable, or user '$user' does not exist." >&2
        echo "DRYRUN is what makes narrowing safe. Do not narrow without it." >&2
        exit 1
    fi
    echo "== replaying observed access against '$user' =="
    denied=0; ok=0
    while IFS="$(printf '\t')" read -r _u cmd key _o; do
        case "$cmd" in ""|FN:*) continue ;; esac
        if [ -n "$key" ]; then r="$(vk ACL DRYRUN "$user" "$cmd" "$key" 2>&1)"
        else                   r="$(vk ACL DRYRUN "$user" "$cmd" 2>&1)"; fi
        if [ "$r" = "OK" ]; then ok=$((ok+1))
        else denied=$((denied+1)); echo "  DENIED: $cmd ${key:-} -> $r"; fi
    done < <(awk -F'\t' '{print $1"\t"$2"\t"$3"\t"$4}' "$PROFILE" | sort -u)
    echo
    echo "permitted=${ok} denied=${denied}"
    if [ "$denied" -eq 0 ]; then
        echo "No OBSERVED access breaks under this user. Coverage is only as"
        echo "good as the profile: check it spans a backup run before trusting it."
    else
        echo "Narrowing to this user WOULD break the access listed above."
    fi
    ;;
*)
    sed -n '3,31p' "$0"; exit 1 ;;
esac
