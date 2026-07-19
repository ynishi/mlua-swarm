# Convenience wrappers around common workflows.
# List recipes: `just` / `just --list`.

# Install the mse LaunchAgent (renders the baked plist template, writes it
# to ~/Library/LaunchAgents/, and bootstraps the job — idempotent).
install-server:
    mse server install

# Uninstall the mse LaunchAgent (bootout + remove the plist — idempotent).
uninstall-server:
    mse server uninstall

# Preview note: the plist template is now baked into the `mse` CLI binary
# (see crates/mlua-swarm-cli/src/server/plist.template). There is no
# stand-alone render flag; run `just install-server` (idempotent) to
# install without a manual preview step, or read
# `mse://guides/server-management` for the full lifecycle guide.
render-plist:
    @echo "The plist template is baked into the mse CLI binary."
    @echo "There is no --render / --dry-run flag; run 'just install-server' (idempotent)."
    @echo "Full guide: mse://guides/server-management (via 'mse mcp' resources/read)."

# Reload the running mse LaunchAgent (after editing ~/.mse/config.toml or
# after `cargo install --path crates/mlua-swarm-cli`).
reload-server:
    mse server restart

# Common Rust workflow shortcuts.
test:
    cargo test --workspace

check:
    cargo clippy --workspace --all-targets -- -D warnings
    cargo fmt --check
    cargo check --workspace
