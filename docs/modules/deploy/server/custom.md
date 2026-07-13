# Run Peerbit on an existing server

The CLI can configure NGINX and a Let's Encrypt certificate so browsers and
remote Peerbit clients can reach a node you operate.

This setup is currently supported on Linux and has been tested on Ubuntu 22.04.

## Before you start

- Forward or allow TCP ports 80, 443, 4002, 4003, and 9002.
- Either create an A or AAAA record for a domain you control, or obtain an
  invite for the managed Peerchecker DNS beta described below.
- If your DNS provider can proxy records, use DNS-only mode unless that proxy
  explicitly supports Peerbit's TCP and WebSocket ports.

## Install and configure

1. Install Node.js 22 and the Peerbit server CLI:

   ```sh
   curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash -
   sudo apt-get install -y nodejs
   sudo npm install -g @peerbit/server
   ```

2. On the administrator machine, install the same CLI and print the identity
   that will sign remote-management requests:

   ```sh
   peerbit id
   ```

   Keep the returned peer ID. Using a separate `--directory` here and in later
   `peerbit remote` commands is supported, but those commands must use the same
   directory so they use the identity you grant below.

3. On the server, configure the domain and certificate. Replace the
   placeholders with the DNS name you created and an email address used for
   certificate notices:

   ```sh
   sudo peerbit domain configure node.example.com --email you@example.com --wait
   ```

   The command may install Docker, writes versioned local state under
   `.peerbit-domain`, and starts the certificate container. It does not create
   or change DNS records.
   Running it again recognizes the managed (or legacy Peerbit) container and
   reuses its certificate bind mount even from a different working directory.
   By default, the old container is kept as a rollback backup until its
   replacement remains stable and the expected Peerbit UI is available over
   HTTPS. The readiness check uses an unpredictable file unique to the staged
   generation, so a cached page or another Peerbit node cannot complete it. Use
   `--no-wait` only when you intentionally want to return before the HTTPS
   readiness gate; that also opts out of readiness-based rollback.
   Readiness failure restores the previous container and active configuration.
   Concurrent configuration attempts for the same container are rejected, and
   an interrupted recognized backup is recovered on the next attempt. Expect
   brief HTTPS downtime during the restart. An unrelated container with the
   same name is rejected rather than removed.

   Invited beta users can instead lease a random
   `p-….nodes.peerchecker.com` name. The lease service URL is supplied by the
   beta administrator. Enter the invite without putting it in shell history,
   then preserve only these two variables across `sudo`:

   ```sh
   read -rsp "Peerchecker invite: " PEERBIT_DNS_LEASE_ACCESS_TOKEN; echo
   export PEERBIT_DNS_LEASE_ACCESS_TOKEN
   export PEERBIT_DNS_LEASE_SERVICE_URL="https://<project>.supabase.co/functions/v1/dns-lease"
   sudo --preserve-env=PEERBIT_DNS_LEASE_ACCESS_TOKEN,PEERBIT_DNS_LEASE_SERVICE_URL \
     peerbit domain lease claim \
     --address "${PUBLIC_IP:?set PUBLIC_IP to this server's public IP}" \
     --email you@example.com --wait
   unset PEERBIT_DNS_LEASE_ACCESS_TOKEN
   sudo chown "$(id -un):$(id -gn)" .peerbit-domain \
     .peerbit-domain/lease.json .peerbit-domain/lease.json.lock-target
   ```

   Set `PUBLIC_IP` to the server's real public IP before running the command.
   Port 80 must be publicly reachable so the CLI can prove control of that IP.
   It must be temporarily free for the initial claim unless the canonical
   Peerbit NGINX configuration already proxies the managed challenge route.
   The invite is used only for the claim and is never stored. A separate lease
   token is stored atomically with mode `0600` in
   `.peerbit-domain/lease.json`. The service issues a short-lived, single-use
   challenge nonce after a claim and before each renewal. The CLI serves that
   nonce only at the exact challenge route while verification is in flight;
   activation removes the claim nonce from local state, and renewal nonces are
   never persisted.

   `peerbit start` renews configured leases in the background; operators can
   also use `peerbit domain lease status`, `renew`, and `release`. Releasing
   removes the managed DNS record but does not remove local certificate files.
   The ownership handoff above is required when the claim runs under `sudo` but
   the node runs as your normal account; do not make the token file group- or
   world-readable. If you select a custom `--state-file`, start the node with
   the matching `--dns-lease-state-file` option or
   `PEERBIT_DNS_LEASE_STATE_FILE` variable.

   Once configured, managed NGINX proxies only the challenge route from public
   port 80 to `127.0.0.1:8093`. Background renewal therefore keeps NGINX
   running and does not require permission to inspect Docker. A legacy
   configuration or a second claim can use the guarded migration fallback:
   the CLI verifies the canonical container is Peerbit-managed, briefly stops
   it by immutable container ID, and verifies its restart after the challenge.
   That fallback requires Docker access and refuses to stop an unrelated
   listener or container. Pending and active lease state is preserved after
   challenge, network, or certificate failures; fix the cause and rerun the
   same claim so its idempotency key and lease token are reused. Use
   `peerbit domain lease release` only when you intend to abandon the lease.

4. Run the node as a supervised service so it restarts after a crash or reboot
   and continues the DNS lease heartbeat. The service must use the same normal
   Linux account and absolute working directory that own `.peerbit-domain`.
   Replace every angle-bracket placeholder, including the complete peer ID from
   step 2 and the path printed by `command -v peerbit`:

   ```ini
   # /etc/systemd/system/peerbit.service
   [Unit]
   Description=Peerbit node
   Wants=network-online.target
   After=network-online.target docker.service

   [Service]
   Type=simple
   User=<linux-user>
   WorkingDirectory=<absolute-node-working-directory>
   Environment=PEERBIT_DNS_LEASE_STATE_FILE=<absolute-node-working-directory>/.peerbit-domain/lease.json
   ExecStart=<absolute-path-to-peerbit> start --grant-access <administrator-peer-id>
   Restart=always
   RestartSec=5

   [Install]
   WantedBy=multi-user.target
   ```

   Then load and start it:

   ```sh
   sudo systemctl daemon-reload
   sudo systemctl enable --now peerbit
   sudo systemctl status peerbit
   ```

   Use `journalctl -u peerbit -f` for logs. Do not substitute an interactive
   `&`/`disown` process for a managed lease: if that process disappears, its
   renewal heartbeat disappears too and the DNS record will eventually be
   reaped.

5. Back on the administrator machine, register the authenticated endpoint:

   ```sh
   peerbit remote add production https://node.example.com
   ```

   `remote add` verifies authenticated access before saving the entry, so a
   missing or different `--grant-access` identity will be rejected.

Run `peerbit --help` or `peerbit start --help` for more options.
