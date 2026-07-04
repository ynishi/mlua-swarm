# Convenience wrappers around common workflows.
# List recipes: `just` / `just --list`.

# Install the mse LaunchAgent (expands the plist template to your $HOME).
install-server:
    scripts/launchd/install.sh

# Uninstall the mse LaunchAgent (bootout + remove the plist).
uninstall-server:
    scripts/launchd/install.sh --uninstall

# Render the plist template to stdout without installing it.
render-plist:
    scripts/launchd/install.sh --render

# Reload the running mse LaunchAgent (after editing ~/.mse/config.toml or
# after `cargo install --path crates/mlua-swarm-cli`).
reload-server:
    launchctl kickstart -k gui/$(id -u)/com.mse.server

# Common Rust workflow shortcuts.
test:
    cargo test --workspace

check:
    cargo clippy --workspace --all-targets -- -D warnings
    cargo fmt --check
