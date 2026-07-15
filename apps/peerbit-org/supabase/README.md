# Peerbit Supabase services

This folder contains the database schema and Edge Functions for the Updates email subscription flow and managed Peerchecker DNS leases.

## What this provides

- Email subscribers + preferences (`all` | `post` | `release`) stored in Supabase Postgres
- Double opt-in (confirm link)
- Unsubscribe link in every email
- A `updates-sync` function that can be called from CI to send emails for newly published updates
- Invite-gated, expiring DNS leases under `nodes.peerchecker.com`
- Direct IP ownership challenges before an exact DNS-only Cloudflare record is created
- Idempotent claim, verify, renew, release, and scheduled cleanup operations

## Setup (high level)

1. Create a Supabase project
2. Apply migrations in `migrations/`
3. Deploy the Edge Functions in `functions/`
4. Configure secrets (Resend + site URLs + sync secret)
5. Configure GitHub Actions + site env var to point to the subscribe endpoint

## Required secrets / env vars

Use `../.env.supabase` (see `../.env.supabase.example`) as a single place to keep the values you need.

### Supabase Edge Functions secrets

Set these as function secrets in Supabase:

- `RESEND_API_KEY`
- `RESEND_FROM` (example: `Peerbit <updates@peerbit.org>`)
- `SITE_URL` (example: `https://www.peerbit.org`)
- `UPDATES_SYNC_SECRET` (random long string used by CI to call `updates-sync`)
- `UPDATES_ALLOWED_ORIGINS` (comma separated, example: `https://www.peerbit.org,https://peerbit.org,http://localhost:5193,http://localhost:5194`)

## Managed DNS leases

The `dns-lease` function is a deliberately small beta control plane. Supabase stores ownership, quotas, lease state, and token hashes. Cloudflare remains the authoritative DNS provider. A narrow Cloudflare Worker is the only component that holds the Cloudflare API token; the public Edge Function can ask that broker only for exact operations inside the random managed-name namespace.

Each invite row is one stable owner. The clear invite token is given to that owner once and is never stored by the CLI or database; only its SHA-256 hash is stored. A caller cannot choose its owner ID. Invite rows have independent enable/expiry controls, an active lease quota, and a lifetime claim limit so a leaked invite cannot create unbounded tombstones. A singleton policy row also caps all new claims in a rolling seven-day window; its conservative beta default preserves certificate-issuance headroom across invites.

The lifecycle is:

1. The CLI generates a 32-byte lease token and an idempotency key.
2. `POST /dns-lease/claim` authenticates the invite, reserves a random `p-<20 hex>.nodes.peerchecker.com` label, and returns a short-lived server-issued proof token plus a direct-IP challenge URL.
3. The CLI temporarily serves that unpredictable token as the exact response body at the URL on port 80.
4. `POST /dns-lease/verify` fetches the committed public IP with no redirects and a strict timeout, then asks the Worker to create an exact A or AAAA record with `proxied: false`.
5. Renewal first requests a fresh single-use nonce from `renew-challenge`, proves control of the same IP, and then calls `renew`. `release` removes the record, and `cleanup` reaps expired records. Before renewal or deletion, the service verifies the Worker's zone identity and re-reads the record by immutable ID and hostname. Every representation must have the same name, type, content, lease comment, and DNS-only state.

### API contract

The deployed base URL is `https://<project>.supabase.co/functions/v1/dns-lease`. All requests are JSON `POST`s. Responses never contain invite or lease credentials. Claim and `renew-challenge` responses contain only their short-lived server-issued proof token.

- `claim` — `Authorization: Bearer <invite token>` with `{ "idempotencyKey", "recordType": "A" | "AAAA", "address", "leaseToken" }`. The lease token is a client-generated 32-byte unpadded base64url string (43 characters). Returns the pending lease plus `challengeToken`, `challengeUrl`, and `challengeExpiresAt`. A client-supplied challenge token is rejected.
- `verify` — `Authorization: Bearer <lease token>` with `{ "id" }`. Creates or recovers the matching managed Cloudflare record and returns the active lease.
- `renew-challenge` — `Authorization: Bearer <lease token>` with `{ "id" }`. Returns an idempotently recoverable, short-lived server nonce and direct-IP challenge URL for an active lease.
- `renew` — `Authorization: Bearer <lease token>` with `{ "id" }`. Atomically consumes the stored nonce, verifies it at the direct IP, checks the DNS record, and extends a non-expired lease.
- `release` — `Authorization: Bearer <lease token>` with `{ "id" }`. Safely deletes the matching record and tombstones the lease.
- `cleanup` — `Authorization: Bearer <DNS_LEASE_CLEANUP_SECRET>` with an empty JSON object. Intended for a Supabase Cron HTTP job.

Claim retries are safe only when the caller reuses the same idempotency key, address, record type, and lease token. The service recovers the original server nonce after a lost response. Completed and expired labels are tombstones and are never reused.

### DNS lease deployment

1. Create a Cloudflare API token limited to `Zone / DNS / Edit` and `Zone / Zone / Read` for `peerchecker.com`. Store it only as a secret of `../cloudflare/dns-lease-broker`, along with its zone ID and a new broker shared secret, then deploy the Worker.
2. Apply `migrations/20260713000000_dns_leases.sql`.
3. Set the Supabase DNS lease secrets from `../.env.supabase.example`. Supabase receives only the broker URL/shared secret, never the Cloudflare API token.
4. Deploy `dns-lease`. Its `verify_jwt = false` setting is intentional: the function authenticates hashed opaque bearer tokens itself.
5. Schedule a Supabase Cron HTTP request to `.../functions/v1/dns-lease/cleanup`, for example every five minutes, with the cleanup bearer secret.
6. Generate each invite token locally, store its SHA-256 hash in `dns_lease_invites`, and transmit the clear token to its owner through a separate secure channel.

One way to generate an invite and its hash is:

```bash
node -e 'const c=require("node:crypto");const t=c.randomBytes(32).toString("base64url");console.log(`token=${t}`);console.log(`sha256=${c.createHash("sha256").update(t).digest("hex")}`)'
```

Then insert only the printed hash:

```sql
insert into public.dns_lease_invites (
  owner_name,
  token_hash,
  active_lease_limit,
  total_claim_limit
)
values ('beta-user-name', '<sha256>', 1, 10);
```

The Worker rejects every hostname outside `p-<20 hex>.nodes.peerchecker.com`, every non-public address, every proxied record, and every delete that does not match the expected lease UUID and record shape. This application-level restriction matters because Cloudflare API tokens cannot be scoped to a DNS record-name prefix. The Worker must remain the only writer for this namespace.

Cleanup uses fenced retries and exponential backoff. After the configured bounded failure count, a drifted record moves to `quarantined`, emits an operator-visible error, and stops consuming the owner's active quota while retaining all evidence needed for manual remediation.

Keep the records DNS-only: ordinary Cloudflare proxying does not carry Peerbit's arbitrary transport ports. Also account for Let's Encrypt's registered-domain certificate limits before widening access beyond a small beta.

### DNS lease tests

The pure validation and Cloudflare request helpers can be tested without Supabase, Deno, or live credentials:

```bash
pnpm dlx deno@2.9.2 check \
  --config supabase/functions/dns-lease/deno.json \
  supabase/functions/dns-lease/index.ts
node --test \
  supabase/functions/_shared/dns-lease.test.ts \
  supabase/functions/_shared/cloudflare-dns.test.ts \
  supabase/functions/_shared/dns-lease-renewal.test.ts
```

### Site env var

Point the UI form action to the subscribe function:

- Local dev: set `VITE_UPDATES_EMAIL_FORM_ACTION=https://<project>.supabase.co/functions/v1/updates-subscribe` (e.g. in `apps/peerbit-org/.env.local`)
- Site builds (including GitHub Actions and `pnpm deploy:docs`) derive this automatically from `SUPABASE_UPDATES_SYNC_URL` if an explicit form action is not set

Production deployment fails closed when either variable would embed a signup endpoint that is malformed, does not resolve, cannot answer a non-mutating `OPTIONS` request, or does not allow `https://peerbit.org` to `POST`. Leaving both variables unset intentionally disables the signup form and remains deployable. The GitHub Pages workflow runs this check after the build and before sync/upload; `pnpm deploy:docs` applies the same gate for manual deployment.

### GitHub Actions secrets

Add these repository secrets so `site.yml` can trigger sending:

- `SUPABASE_UPDATES_SYNC_URL=https://<project>.supabase.co/functions/v1/updates-sync`
- `SUPABASE_UPDATES_SYNC_SECRET=<same as UPDATES_SYNC_SECRET>`

### Manual sync (optional)

After building the site (so `apps/peerbit-org/dist/content/docs/updates/index.json` exists), you can trigger a sync manually:

```bash
UPDATES_SYNC_URL="https://<project>.supabase.co/functions/v1/updates-sync" \
UPDATES_SYNC_SECRET="..." \
pnpm site:sync-updates
```
