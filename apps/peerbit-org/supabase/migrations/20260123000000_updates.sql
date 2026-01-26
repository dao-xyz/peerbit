create extension if not exists pgcrypto;

create table if not exists public.updates_subscribers (
	id uuid primary key default gen_random_uuid(),
	email text not null unique,
	topic text not null default 'all' check (topic in ('all', 'post', 'release')),
	status text not null default 'pending' check (status in ('pending', 'active', 'unsubscribed')),
	confirm_token_hash text,
	confirm_token_expires_at timestamptz,
	confirm_sent_at timestamptz,
	unsubscribe_token text not null,
	confirmed_at timestamptz,
	unsubscribed_at timestamptz,
	created_at timestamptz not null default now(),
	updated_at timestamptz not null default now()
);

create table if not exists public.updates_sent (
	id uuid primary key default gen_random_uuid(),
	kind text not null check (kind in ('post', 'release')),
	href text not null unique,
	title text not null,
	date date,
	excerpt text,
	sent_at timestamptz not null default now()
);

create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
	new.updated_at = now();
	return new;
end;
$$;

drop trigger if exists set_updated_at on public.updates_subscribers;
create trigger set_updated_at
before update on public.updates_subscribers
for each row execute procedure public.set_updated_at();

alter table public.updates_subscribers enable row level security;
alter table public.updates_sent enable row level security;

