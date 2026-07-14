create table public.dns_lease_invites (
	id uuid primary key default gen_random_uuid(),
	owner_name text not null check (char_length(owner_name) between 1 and 120),
	token_hash text not null unique check (token_hash ~ '^[0-9a-f]{64}$'),
	active_lease_limit smallint not null default 1 check (active_lease_limit between 1 and 20),
	total_claim_limit integer not null default 10 check (total_claim_limit between 1 and 10000),
	enabled boolean not null default true,
	expires_at timestamptz,
	last_used_at timestamptz,
	created_at timestamptz not null default now(),
	updated_at timestamptz not null default now()
);

create table public.dns_lease_policy (
	id smallint primary key default 1 check (id = 1),
	max_claims_per_7_days smallint not null default 20 check (max_claims_per_7_days between 1 and 100),
	updated_at timestamptz not null default now()
);

insert into public.dns_lease_policy (id)
values (1)
on conflict (id) do nothing;

create table public.dns_leases (
	id uuid primary key default gen_random_uuid(),
	invite_id uuid not null references public.dns_lease_invites(id) on delete restrict,
	label text not null unique check (label ~ '^p-[a-f0-9]{20}$'),
	domain text generated always as (label || '.nodes.peerchecker.com') stored unique,
	record_type text not null check (record_type in ('A', 'AAAA')),
	target_address inet not null,
	status text not null default 'pending' check (
		status in ('pending', 'provisioning', 'active', 'release_pending', 'expiring', 'quarantined', 'released', 'expired')
	),
	idempotency_key text not null check (char_length(idempotency_key) between 8 and 128),
	lease_token_hash text not null unique check (lease_token_hash ~ '^[0-9a-f]{64}$'),
	challenge_id uuid unique,
	challenge_token_hash text check (challenge_token_hash is null or challenge_token_hash ~ '^[0-9a-f]{64}$'),
	challenge_expires_at timestamptz,
	provisioning_id uuid unique,
	provisioning_expires_at timestamptz,
	cloudflare_zone_id text check (cloudflare_zone_id is null or cloudflare_zone_id ~ '^[a-f0-9]{32}$'),
	cloudflare_record_id text unique,
	verify_available_at timestamptz not null default now(),
	cleanup_available_at timestamptz not null default now(),
	cleanup_failures smallint not null default 0 check (cleanup_failures between 0 and 20),
	pending_expires_at timestamptz not null,
	lease_expires_at timestamptz,
	verified_at timestamptz,
	renewed_at timestamptz,
	released_at timestamptz,
	quarantined_at timestamptz,
	error_message text,
	created_at timestamptz not null default now(),
	updated_at timestamptz not null default now(),
	unique (invite_id, idempotency_key),
	check (
		(record_type = 'A' and family(target_address) = 4)
		or (record_type = 'AAAA' and family(target_address) = 6)
	),
	check (
		(family(target_address) = 4 and masklen(target_address) = 32)
		or (family(target_address) = 6 and masklen(target_address) = 128)
	),
	constraint dns_leases_challenge_consistency check (
		(
			challenge_id is null
			and challenge_token_hash is null
			and challenge_expires_at is null
		)
		or (
			challenge_id is not null
			and challenge_token_hash is not null
			and challenge_expires_at is not null
		)
	),
	constraint dns_leases_pending_challenge check (
		status <> 'pending'
		or (
			challenge_id is not null
			and challenge_expires_at <= pending_expires_at
		)
	),
	constraint dns_leases_challenge_state check (
		challenge_id is null or status in ('pending', 'provisioning', 'active')
	),
	constraint dns_leases_active_challenge_expiry check (
		status <> 'active'
		or challenge_expires_at is null
		or challenge_expires_at <= lease_expires_at
	),
	constraint dns_leases_provisioning_consistency check (
		(
			provisioning_id is null
			and provisioning_expires_at is null
		)
		or (
			provisioning_id is not null
			and provisioning_expires_at is not null
		)
	),
	constraint dns_leases_provisioning_fence check (
		(status = 'provisioning') = (provisioning_id is not null)
	),
	constraint dns_leases_provider_record check (
		cloudflare_record_id is null or cloudflare_zone_id is not null
	),
	constraint dns_leases_active_fields check (
		status <> 'active'
		or (
			cloudflare_zone_id is not null
			and cloudflare_record_id is not null
			and lease_expires_at is not null
			and verified_at is not null
		)
	),
	constraint dns_leases_quarantine_consistency check (
		(status = 'quarantined' and quarantined_at is not null)
		or (status <> 'quarantined' and quarantined_at is null)
	)
);

create index dns_leases_cleanup_idx
	on public.dns_leases(status, pending_expires_at, lease_expires_at);
create index dns_leases_cleanup_available_idx
	on public.dns_leases(status, cleanup_available_at);
create index dns_leases_invite_status_idx
	on public.dns_leases(invite_id, status);
create index dns_leases_created_at_idx
	on public.dns_leases(created_at);
create index dns_leases_provisioning_expiry_idx
	on public.dns_leases(status, provisioning_expires_at);
create index dns_leases_quarantined_idx
	on public.dns_leases(status, quarantined_at);

create or replace function public.is_public_dns_target(address inet)
returns boolean
language sql
immutable
strict
as $$
	select case family(address)
		when 4 then not (
			address <<= inet '0.0.0.0/8'
			or address <<= inet '10.0.0.0/8'
			or address <<= inet '100.64.0.0/10'
			or address <<= inet '127.0.0.0/8'
			or address <<= inet '169.254.0.0/16'
			or address <<= inet '172.16.0.0/12'
			or address <<= inet '192.0.0.0/24'
			or address <<= inet '192.0.2.0/24'
			or address <<= inet '192.88.99.0/24'
			or address <<= inet '192.168.0.0/16'
			or address <<= inet '198.18.0.0/15'
			or address <<= inet '198.51.100.0/24'
			or address <<= inet '203.0.113.0/24'
			or address <<= inet '224.0.0.0/4'
			or address <<= inet '240.0.0.0/4'
		)
		when 6 then (
			address <<= inet '2000::/3'
			and not address <<= inet '2001::/23'
			and not address <<= inet '2001:db8::/32'
			and not address <<= inet '2002::/16'
			and not address <<= inet '3fff::/20'
		)
		else false
	end;
$$;

revoke all on function public.is_public_dns_target(inet)
	from public, anon, authenticated;
grant execute on function public.is_public_dns_target(inet)
	to service_role;

alter table public.dns_leases
	drop constraint if exists dns_leases_public_target;
alter table public.dns_leases
	add constraint dns_leases_public_target check (public.is_public_dns_target(target_address));

drop trigger if exists set_updated_at on public.dns_lease_invites;
create trigger set_updated_at
before update on public.dns_lease_invites
for each row execute procedure public.set_updated_at();

drop trigger if exists set_updated_at on public.dns_leases;
create trigger set_updated_at
before update on public.dns_leases
for each row execute procedure public.set_updated_at();

drop trigger if exists set_updated_at on public.dns_lease_policy;
create trigger set_updated_at
before update on public.dns_lease_policy
for each row execute procedure public.set_updated_at();

alter table public.dns_lease_invites enable row level security;
alter table public.dns_leases enable row level security;
alter table public.dns_lease_policy enable row level security;

drop function if exists public.claim_dns_lease(text, text, text, inet, text, text, text, timestamptz);
drop function if exists public.claim_dns_lease(text, text, text, inet, text, uuid, text, text, timestamptz);

create or replace function public.claim_dns_lease(
	p_invite_token_hash text,
	p_label text,
	p_record_type text,
	p_target_address inet,
	p_lease_token_hash text,
	p_challenge_id uuid,
	p_challenge_token_hash text,
	p_challenge_expires_at timestamptz,
	p_idempotency_key text,
	p_pending_expires_at timestamptz
)
returns public.dns_leases
language plpgsql
security definer
set search_path = pg_catalog, public
as $$
declare
	v_invite public.dns_lease_invites;
	v_policy public.dns_lease_policy;
	v_lease public.dns_leases;
	v_active_count integer;
	v_recent_count integer;
	v_total_count integer;
begin
	select * into v_invite
	from public.dns_lease_invites
	where token_hash = p_invite_token_hash
		and enabled
		and (expires_at is null or expires_at > now())
	for update;

	if not found then
		raise exception using errcode = 'P0001', message = 'invite_not_available';
	end if;

	perform pg_advisory_xact_lock(hashtextextended(v_invite.id::text, 0));

	select * into v_lease
	from public.dns_leases
	where invite_id = v_invite.id and idempotency_key = p_idempotency_key
	for update;

	if found then
		if v_lease.status <> 'pending' or v_lease.pending_expires_at <= now() then
			raise exception using errcode = 'P0001', message = 'idempotency_key_already_completed';
		end if;
		-- Challenge material is server-issued. A retried HTTP claim may arrive with
		-- freshly generated challenge parameters, but it must recover the original
		-- challenge stored by the first successful transaction.
		if v_lease.record_type <> p_record_type
			or v_lease.target_address <> p_target_address
			or v_lease.lease_token_hash <> p_lease_token_hash then
			raise exception using errcode = 'P0001', message = 'idempotency_key_payload_mismatch';
		end if;

		update public.dns_lease_invites set last_used_at = now() where id = v_invite.id;
		return v_lease;
	end if;

	select * into v_policy
	from public.dns_lease_policy
	where id = 1
	for update;

	if not found then
		raise exception using errcode = 'P0001', message = 'lease_policy_not_available';
	end if;

	select count(*) into v_recent_count
	from public.dns_leases
	where created_at >= now() - interval '7 days';

	if v_recent_count >= v_policy.max_claims_per_7_days then
		raise exception using errcode = 'P0001', message = 'lease_global_rate_exceeded';
	end if;

	select count(*) into v_active_count
	from public.dns_leases
	where invite_id = v_invite.id
		-- Quarantine is operator-owned and deliberately does not consume the
		-- caller's active quota. The lifetime and global claim caps still apply.
		and status in ('pending', 'provisioning', 'active', 'release_pending', 'expiring');

	if v_active_count >= v_invite.active_lease_limit then
		raise exception using errcode = 'P0001', message = 'lease_quota_exceeded';
	end if;

	select count(*) into v_total_count
	from public.dns_leases
	where invite_id = v_invite.id;

	if v_total_count >= v_invite.total_claim_limit then
		raise exception using errcode = 'P0001', message = 'lease_claim_limit_exceeded';
	end if;

	insert into public.dns_leases (
		invite_id,
		label,
		record_type,
		target_address,
		idempotency_key,
		lease_token_hash,
		challenge_id,
		challenge_token_hash,
		challenge_expires_at,
		pending_expires_at
	) values (
		v_invite.id,
		p_label,
		p_record_type,
		p_target_address,
		p_idempotency_key,
		p_lease_token_hash,
		p_challenge_id,
		p_challenge_token_hash,
		p_challenge_expires_at,
		p_pending_expires_at
	)
	returning * into v_lease;

	update public.dns_lease_invites set last_used_at = now() where id = v_invite.id;
	return v_lease;
end;
$$;

revoke all on function public.claim_dns_lease(text, text, text, inet, text, uuid, text, timestamptz, text, timestamptz)
	from public, anon, authenticated;
grant execute on function public.claim_dns_lease(text, text, text, inet, text, uuid, text, timestamptz, text, timestamptz)
	to service_role;

revoke all on table public.dns_lease_invites from public, anon, authenticated;
revoke all on table public.dns_leases from public, anon, authenticated;
revoke all on table public.dns_lease_policy from public, anon, authenticated;
grant all on table public.dns_lease_invites to service_role;
grant all on table public.dns_leases to service_role;
grant all on table public.dns_lease_policy to service_role;
