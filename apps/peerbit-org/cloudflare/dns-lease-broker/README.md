# Peerbit DNS lease broker

This Cloudflare Worker is the only component that receives the Cloudflare API
token used by the managed DNS lease service. Supabase authenticates to the
Worker with a separate shared bearer secret. The Cloudflare token must never be
stored in Supabase, committed to this repository, or sent to a client.

The broker can operate only on names with this exact shape:

```text
p-<20 lowercase hexadecimal characters>.nodes.peerchecker.com
```

It accepts only public `A` and `AAAA` addresses, DNS-only records
(`proxied: false`), Cloudflare-supported TTLs, and comments of the form
`Peerbit managed lease <UUID>`. Every request first verifies that the configured
Cloudflare zone ID resolves to exactly `peerchecker.com`.

## API contract

All endpoints accept `POST`, require `Content-Type: application/json`, and use:

```http
Authorization: Bearer <BROKER_SHARED_SECRET>
```

Bodies are limited to 8 KiB and unknown JSON fields are rejected.

Each request has one absolute 35-second deadline, including request-body reads.
Individual Cloudflare API calls are capped at five seconds or the smaller
remaining budget. The broker refuses to begin create/delete mutations unless
enough time remains for their required verification and safe reconciliation
calls, and a caller disconnect aborts the in-flight downstream request. Record
listing is capped at ten results; larger sets fail closed for manual cleanup.

| Route | JSON body | Successful result |
| --- | --- | --- |
| `/zone` | `{}` | `{ "zoneId", "zoneName": "peerchecker.com" }` |
| `/records/list` | `{ "name", "type"? }` | `{ "zoneId", "records" }` |
| `/records/get` | `{ "recordId" }` | `{ "zoneId", "record" }` (`record` can be `null`) |
| `/records/create` | `{ "leaseId", "name", "type", "address", "ttl" }` | `{ "zoneId", "record" }` |
| `/records/delete` | `{ "recordId", "leaseId", "name", "type", "address" }` | `{ "zoneId", "deletedId" }` (`deletedId` can be `null`) |

`create` checks that the name is unoccupied, creates the exact DNS-only record,
and lists the name again to detect races. Retrying an identical create is
idempotent. `delete` first fetches the record by ID and verifies its name, type,
public address, and exact lease comment. It never deletes by name and never
deletes a record that does not match the caller's complete expectation.

Errors have this stable shape and do not include Cloudflare response bodies:

```json
{
  "error": { "code": "DNS_RECORD_CONFLICT", "message": "..." },
  "requestId": "..."
}
```

## Cloudflare setup

Create a dedicated Cloudflare API token restricted to the `peerchecker.com`
zone with only these permissions:

- Zone / Zone / Read
- Zone / DNS / Edit

Do not use a Global API Key or an account-wide token. The broker deliberately
performs a zone-read preflight before every DNS operation, so both permissions
are required.

Install the exact pinned Wrangler release and authenticate interactively:

```bash
cd apps/peerbit-org/cloudflare/dns-lease-broker
pnpm install --ignore-workspace
pnpm exec wrangler login
```

Create the shared secret in a trusted password manager or secret-management
tool as exactly 32 cryptographically random bytes encoded as 43 characters of
unpadded base64url (`A-Z`, `a-z`, `0-9`, `_`, and `-`). Supabase enforces this
exact format. Do not put the value in command arguments, shell history, a file,
or clipboard tooling that retains history. Enter all values through Wrangler's
interactive hidden prompt:

```bash
pnpm exec wrangler secret put CLOUDFLARE_API_TOKEN
pnpm exec wrangler secret put CLOUDFLARE_ZONE_ID
pnpm exec wrangler secret put BROKER_SHARED_SECRET
```

`CLOUDFLARE_ZONE_ID` is not confidential, but storing it through the same
interactive flow avoids committing an environment-specific ID. Give Supabase
only the deployed Worker URL and the same broker shared secret:

```text
DNS_LEASE_DNS_BROKER_URL=https://peerbit-dns-lease-broker.<account>.workers.dev
DNS_LEASE_DNS_BROKER_SECRET=<same BROKER_SHARED_SECRET>
```

Deploy and inspect the authenticated zone check before enabling lease traffic:

```bash
pnpm run check
pnpm run deploy
pnpm run tail
```

The committed Worker configuration enables the `workers.dev` endpoint. A
Cloudflare Custom Domain can replace it later without changing the broker
contract. Keep the endpoint inaccessible to browser clients; only the Supabase
Edge Function should possess the shared secret.

## Local verification

Unit tests use an injected fetch implementation and never contact Cloudflare:

```bash
pnpm test
```

For `wrangler dev`, put development-only values in `.dev.vars`; that file is
ignored. Never copy the production Cloudflare token into automated test
fixtures.

## Rotation and incident response

Rotate `BROKER_SHARED_SECRET` in the Worker and Supabase together. Rotate the
Cloudflare token independently in the Worker; Supabase does not need to know
about that change. If either secret is exposed, revoke it before investigating.
Audit logs contain request IDs, routes, managed names, opaque record IDs,
statuses, and durations, but never bearer secrets, Cloudflare response bodies,
lease UUIDs, or IP addresses.
