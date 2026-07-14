# Writing gNode Extensions

gNode extensions are optional modules that add commands, Lua libraries, or
both to a running daemon. They are loaded from:

- `GNODE_EXT_<NAME>_PATH` env vars (unsigned, dev-only)
- `GNODE_EXT_DIR/<subdir>` (signed, production)

Signed extensions are verified at build time (`daemon/build.rs`) AND
runtime (`daemon/src/extensions/mod.rs::verify_extension_signature`)
against the Ed25519 public key at `daemon/src/ext_author.rs::AUTHOR_PUBKEY`.

This guide walks through:
- the extension layout,
- the `extension.yaml` schema,
- what the signature actually covers,
- how to sign with `scripts/geodineum-sign-extensions.sh`,
- and the clean-slate signer rotation procedure (rare).

---

## 1. Extension repo layout

```
my-extension/
├── extension.yaml            # manifest (see §2)
├── extension.sig             # Ed25519 signature (produced by sign script)
├── src/
│   └── handlers/
│       ├── foo.rs            # Rust handler, provides `register()`
│       └── bar.rs
└── functions/
    └── gnode_foo.lua         # Lua library, FUNCTION LOAD'd on startup
```

`src/handlers/` is optional for pure-Lua extensions.
`functions/` is optional for pure-Rust extensions.
`extension.yaml` is always required.

---

## 2. `extension.yaml` schema

```yaml
# Required
name: foo                          # lowercase snake_case, [a-z_][a-z0-9_]*
display_name: Foo
version: "1.0.0"
description: >-
  One-paragraph summary of what Foo provides.

# Optional — declared Rust feature flag that gates compiled handlers.
# When set, the daemon checks that the feature is compiled in before
# marking the extension operational. Omit for pure-Lua extensions.
rust_feature: foo

# Rust handler source files. Each file must live at src/handlers/<name>
# and export a `register(handlers, async_handlers, desc_vec)` function
# matching the signature of base/CMS handler modules.
handler_files:
  - foo.rs
  - bar.rs

# Lua libraries shipped with this extension. Each must exist at
# functions/<name>.lua. FUNCTION LOAD'd on daemon start.
lua_libraries:
  - gnode_foo

# Commands this extension registers. Documentation only — actual
# registration is done by each handler's register() function.
commands:
  - foo_store
  - foo_retrieve

# Runtime gate: operator can disable an otherwise-valid extension here.
enabled: true
```

**Every handler file and every Lua library file listed here is covered
by the signature** (§4). Listing a file in the manifest and then
forgetting to `git commit` it breaks signing. Listing a file and
then editing it after signing breaks verification.

---

## 3. Writing a Rust handler

Each `src/handlers/<name>.rs` must expose a public `register()` with the
same signature as base + CMS handler modules:

```rust
// src/handlers/foo.rs
use std::collections::HashMap;
use crate::integration::command_handler::{
    CommandHandlerFn, AsyncCommandHandlerFn, CommandDescriptor,
};

pub fn register(
    handlers: &mut HashMap<String, CommandHandlerFn>,
    async_handlers: &mut HashMap<String, AsyncCommandHandlerFn>,
    desc_vec: &mut Vec<CommandDescriptor>,
) {
    handlers.insert("foo_store".to_string(), foo_store);
    // … async_handlers.insert(…);
    // … desc_vec.push(…);
}

fn foo_store(/* params */) -> /* CommandResult */ {
    // …
}
```

Build.rs stages these files into `OUT_DIR/<name>_handlers/` and emits
an `include!()` stub so the gNode daemon can dispatch them. You do not
edit the daemon source to register new commands — the `register()` in
your handler is invoked by build-generated `register_signed_extensions()`.

---

## 4. Signature scope (what actually gets signed)

The signature is NOT over `extension.yaml` alone. It is over a
**canonical-hashes text form** that covers the manifest PLUS sha256
hashes of every handler file and every Lua library file:

```
format-version: 1
extension: <name>
extension-yaml-sha256: <hex>
handler: <filename> <hex>              (sorted by filename, lowercase hex)
...
lua-library: <name> <hex>              (sorted by name, lowercase hex)
...
```

Every byte that ends up executing — Rust handlers, Lua functions —
is hash-bound. An attacker who writes to `GNODE_EXT_DIR` can no longer
swap `.rs` or `.lua` bodies while keeping the manifest signature valid.

The signer and both verifiers (build.rs, runtime extensions/mod.rs)
compute this canonical text byte-identically.

---

## 5. Signing an extension

```bash
# First time: receive the priv key from the project author out-of-band,
# chmod 600 it, store offline.
chmod 600 ~/keys/ext_signer.key

# Sign. This writes extension.sig next to extension.yaml.
cd ~/my-extension
/opt/geodineum/gNode/scripts/geodineum-sign-extensions.sh \
    ~/my-extension \
    --key ~/keys/ext_signer.key
```

The script prints the canonical form it signed. Diff it against
expectations before trusting the result.

To re-sign after editing a handler or Lua file:

```bash
/opt/geodineum/gNode/scripts/geodineum-sign-extensions.sh \
    ~/my-extension --key ~/keys/ext_signer.key --force
```

---

## 6. Installing a signed extension

```bash
# Copy or symlink the extension repo under GNODE_EXT_DIR.
sudo ln -s /home/you/my-extension /opt/geodineum/extensions/my-extension
sudo systemctl restart gnode-daemon
```

At startup the daemon will:
1. Discover `/opt/geodineum/extensions/my-extension/`.
2. Re-compute the canonical hashes and verify the signature.
3. If valid: register the extension's commands, FUNCTION LOAD its Lua
   libraries.
4. If invalid: log a warning and skip the extension entirely. The
   daemon continues without it.

---

## 7. Signer rotation (rare)

The Ed25519 pubkey is baked into `daemon/src/ext_author.rs`. Rotating
the signer requires a daemon re-release.

```bash
# 1. Generate a new keypair.
/opt/geodineum/gNode/scripts/geodineum-gen-ext-keys.sh ~/new-ext-keys

# 2. Replace the baked-in pubkey.
cp ~/new-ext-keys/ext_author.rs \
   ~/gh/gNode/daemon/src/ext_author.rs

# 3. Rebuild.
cd ~/gh/gNode/daemon
cargo build --release

# 4. Re-sign every existing extension with the new priv key.
for ext in ~/gh/pro/gNode/gNode-*; do
    /opt/geodineum/gNode/scripts/geodineum-sign-extensions.sh \
        "$ext" --key ~/new-ext-keys/ext_signer.key --force
done
```

Losing the private key permanently locks you out of signing new
extensions under that identity. **Keep an off-machine backup.**
