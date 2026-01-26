# Peerbit.org Updates (Supabase)

This folder contains the database schema and Edge Functions for the **Updates** email subscription flow.

## What this provides

- Email subscribers + preferences (`all` | `post` | `release`) stored in Supabase Postgres
- Double opt-in (confirm link)
- Unsubscribe link in every email
- A `updates-sync` function that can be called from CI to send emails for newly published updates

## Setup (high level)

1) Create a Supabase project
2) Apply migrations in `migrations/`
3) Deploy the Edge Functions in `functions/`
4) Configure secrets (Resend + site URLs + sync secret)
5) Configure GitHub Actions + site env var to point to the subscribe endpoint

## Required secrets / env vars

Use `../.env.supabase` (see `../.env.supabase.example`) as a single place to keep the values you need.

### Supabase Edge Functions secrets

Set these as function secrets in Supabase:

- `RESEND_API_KEY`
- `RESEND_FROM` (example: `Peerbit <updates@peerbit.org>`)
- `SITE_URL` (example: `https://peerbit.org`)
- `UPDATES_SYNC_SECRET` (random long string used by CI to call `updates-sync`)
- `UPDATES_ALLOWED_ORIGINS` (comma separated, example: `https://peerbit.org,http://localhost:5173`)

### Site env var

Point the UI form action to the subscribe function:

- Local dev: set `VITE_UPDATES_EMAIL_FORM_ACTION=https://<project>.supabase.co/functions/v1/updates-subscribe` (e.g. in `apps/peerbit-org/.env.local`)
- GitHub Actions: `site.yml` derives this automatically from `SUPABASE_UPDATES_SYNC_URL` (if set)

### GitHub Actions secrets

Add these repository secrets so `site.yml` can trigger sending:

- `SUPABASE_UPDATES_SYNC_URL=https://<project>.supabase.co/functions/v1/updates-sync`
- `SUPABASE_UPDATES_SYNC_SECRET=<same as UPDATES_SYNC_SECRET>`
