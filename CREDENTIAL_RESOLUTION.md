# gNode Credential Resolution

Definitive contract for how a client locates the ValKey password for an ACL
user. Any client in any language — including direct-FCALL clients with no
gNode-Client wrapper — MUST resolve credentials exactly as described here.
The PHP reference implementation is `CredentialResolver` in gNode-Client; it
implements this contract and nothing more.

---

## Username → filename

A ValKey ACL username maps to a password filename by replacing the `gnode_`
prefix with `valkey_` and appending `.password`. A username without the
`gnode_` prefix simply gains `.password`.

| ACL username                       | Password filename                       |
|------------------------------------|-----------------------------------------|
| `gnode_daemon`                     | `valkey_daemon.password`                |
| `gnode_client`                     | `valkey_client.password`                |
| `gnode_client_<site_or_component>` | `valkey_client_<site_or_component>.password` |

`<site_or_component>` is the identifier with dots/hyphens normalised to
underscores (e.g. `staging.example.com` → `gnode_client_staging_example_com`).

## Resolution order

First match wins. A client stops at the first source that yields a non-empty
password.

1. **`VALKEY_PASSWORD`** environment variable — the literal password.
2. **`VALKEY_PASSWORD_FILE`** environment variable — a path to read.
3. **Centralized** — `/etc/geodineum/credentials/{filename}` *(canonical for
   production)*.
4. **Standard** — `/opt/geodineum/gNode/.gnode/{filename}`.
5. **Legacy** — `/opt/gNode/.gnode/{filename}`.

`/etc/geodineum/credentials/{filename}` is THE location for deployed systems.
There is no per-site credential directory: credentials are flat files under
`/etc/geodineum/credentials/`, keyed by the derived filename. (The
`/etc/geodineum/sites/<id>/` tree holds per-site *config*, never credentials.)

## Ownership (read side)

A credential is owned `root:<group> 0640` and is read via group membership; a
client reads as whatever OS user its process runs as. The resolution logic is
identical regardless of ownership — it locates the file; the kernel decides
whether the running user may read it. Per-component isolation means each
component's credential is private to its own runtime group, so a client only
ever resolves credentials it is entitled to read.

## Failure

If no source yields a password, a client MUST fail loudly at connect time
(ValKey requires AUTH; an unauthenticated handle dies later with NOAUTH).
Error output SHOULD report which sources were checked and the running user,
to make a permission mismatch self-diagnosing.

## Conformance

- **PHP**: depend on gNode-Client and call
  `gCore\gNode\Config\CredentialResolver::tryResolve($user)` /
  `resolve($user)`. Do not re-derive paths.
- **Other languages**: implement the username→filename mapping and the
  resolution order above. Do not invent additional paths; if a new location
  is ever needed, it is added to this contract first, then to every reader.
