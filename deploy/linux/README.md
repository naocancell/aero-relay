# Relay Link Ubuntu package

This package is for the Ubuntu server deployment. It contains the Rust
relay-linkd backend, Linux amd64 FFmpeg and MediaMTX executables, the
managed-video configuration template, and systemd units for the server and
relay roles. It does not contain the Tauri desktop control window.

The bundled media versions are FFmpeg 9.0.1 and MediaMTX v1.18.2. The FFmpeg
Linux executable is built from the official 9.0.1 source archive; the
MediaMTX executable comes from its official Linux amd64 release archive. The
exact archive URLs and SHA-256 values are recorded in
/usr/local/share/relay-link/media-versions.env after installation.

## Install

On an Ubuntu amd64 server:

    sudo apt install ./relay-linkd_0.1.0_amd64.deb
    sudoedit /usr/local/bin/config/config.toml

The installer creates the relay-link system user and group and copies the
example configuration only when the target files do not already exist. It
does not enable or start a role automatically.

Enable exactly one role:

    sudo systemctl enable --now relay-linkd-server.service
    # or, on a relay host:
    sudo systemctl enable --now relay-linkd-relay.service

Check the service and executable-relative log file with:

    systemctl status relay-linkd-server.service
    sudo tail -f /usr/local/bin/log/relay-linkd.log

The systemd RuntimeDirectory=relay-link setting creates the Unix IPC
directory with the correct service-user permissions. Running the binary
directly as an unprivileged shell user will not create /run/relay-link; use
the service unit for normal operation.
