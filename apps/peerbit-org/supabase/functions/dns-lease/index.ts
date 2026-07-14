import { createClient, type SupabaseClient } from "@supabase/supabase-js";
import {
  type CloudflareDnsConfig,
  type CloudflareDnsRecord,
  createDnsRecord,
  deleteDnsRecord,
  findDnsRecords,
  getDnsRecord,
  verifyDnsZone,
} from "../_shared/cloudflare-dns.ts";
import { sha256Hex } from "../_shared/crypto.ts";
import {
  bearerToken,
  challengeUrl,
  deriveChallengeToken,
  type DnsRecordType,
  parsePublicDnsTarget,
  validIdempotencyKey,
  validSecret,
} from "../_shared/dns-lease.ts";
import { recoverConsumedRenewalAvailability } from "../_shared/dns-lease-renewal.ts";

type LeaseStatus =
  | "pending"
  | "provisioning"
  | "active"
  | "release_pending"
  | "expiring"
  | "released"
  | "expired"
  | "quarantined";

type LeaseRow = {
  id: string;
  invite_id: string;
  label: string;
  domain: string;
  record_type: DnsRecordType;
  target_address: string;
  status: LeaseStatus;
  lease_token_hash: string;
  challenge_id: string | null;
  challenge_token_hash: string | null;
  challenge_expires_at: string | null;
  provisioning_id: string | null;
  provisioning_expires_at: string | null;
  cloudflare_record_id: string | null;
  cloudflare_zone_id: string | null;
  verify_available_at: string;
  cleanup_available_at: string;
  cleanup_failures: number;
  pending_expires_at: string;
  lease_expires_at: string | null;
  quarantined_at: string | null;
  updated_at: string;
};

const LEASE_COLUMNS =
  "id,invite_id,label,domain,record_type,target_address,status,lease_token_hash,challenge_id,challenge_token_hash,challenge_expires_at,provisioning_id,provisioning_expires_at,cloudflare_record_id,cloudflare_zone_id,verify_available_at,cleanup_available_at,cleanup_failures,pending_expires_at,lease_expires_at,quarantined_at,updated_at";

class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

function json(status: number, body: unknown) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "Cache-Control": "no-store",
      "Content-Type": "application/json; charset=utf-8",
    },
  });
}

function envInt(
  name: string,
  fallback: number,
  minimum: number,
  maximum: number,
) {
  const value = Number(Deno.env.get(name) ?? fallback);
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new Error(
      name + " must be an integer between " + minimum + " and " + maximum,
    );
  }
  return value;
}

function challengeSecret() {
  const secret = Deno.env.get("DNS_LEASE_CHALLENGE_SECRET");
  if (!validSecret(secret)) {
    throw new Error(
      "DNS_LEASE_CHALLENGE_SECRET must be 32 random bytes encoded as base64url",
    );
  }
  return secret;
}

function dnsConfig(): CloudflareDnsConfig {
  const brokerUrl = Deno.env.get("DNS_LEASE_DNS_BROKER_URL");
  const brokerSecret = Deno.env.get("DNS_LEASE_DNS_BROKER_SECRET");
  if (!brokerUrl || !validSecret(brokerSecret)) {
    throw new Error("Missing DNS broker configuration");
  }
  let parsed: URL;
  try {
    parsed = new URL(brokerUrl);
  } catch {
    throw new Error("DNS_LEASE_DNS_BROKER_URL must be a valid URL");
  }
  if (parsed.protocol !== "https:") {
    throw new Error("DNS_LEASE_DNS_BROKER_URL must use HTTPS");
  }
  return {
    brokerUrl: parsed.toString().replace(/\/$/, ""),
    brokerSecret,
    ttl: envInt("DNS_LEASE_RECORD_TTL_SECONDS", 300, 60, 86_400),
    requestTimeoutMs: envInt(
      "DNS_LEASE_BROKER_TIMEOUT_MS",
      40_000,
      36_000,
      45_000,
    ),
  };
}

function provisioningLockSeconds() {
  return envInt("DNS_LEASE_PROVISIONING_LOCK_SECONDS", 300, 240, 600);
}

const PROVISIONING_MUTATION_SAFETY_SECONDS = 60;

async function requestBody(req: Request) {
  const length = Number(req.headers.get("content-length") ?? "0");
  if (Number.isFinite(length) && length > 8_192) {
    throw new ApiError(413, "Request body is too large");
  }
  const text = await boundedStreamText(
    req.body,
    8_192,
    new ApiError(413, "Request body is too large"),
  );
  try {
    const value = JSON.parse(text) as unknown;
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      throw new Error();
    }
    return value as Record<string, unknown>;
  } catch {
    throw new ApiError(400, "Expected a JSON object");
  }
}

function actionFromUrl(url: URL) {
  const parts = url.pathname.split("/").filter(Boolean);
  const functionIndex = parts.lastIndexOf("dns-lease");
  return functionIndex >= 0 ? parts[functionIndex + 1] : undefined;
}

function randomLabel() {
  const bytes = new Uint8Array(10);
  crypto.getRandomValues(bytes);
  return "p-" +
    Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function publicLease(row: LeaseRow) {
  return {
    id: row.id,
    domain: row.domain,
    recordType: row.record_type,
    address: row.target_address,
    status: row.status,
    pendingExpiresAt: row.pending_expires_at,
    expiresAt: row.lease_expires_at,
  };
}

async function authorizedLease(
  supabase: SupabaseClient,
  req: Request,
  body: Record<string, unknown>,
  requireActiveOwner = true,
) {
  const token = bearerToken(req);
  if (!token || typeof body.id !== "string") {
    throw new ApiError(401, "Unauthorized");
  }
  const tokenHash = await sha256Hex(token);
  const { data, error } = await supabase
    .from("dns_leases")
    .select(LEASE_COLUMNS)
    .eq("id", body.id)
    .eq("lease_token_hash", tokenHash)
    .maybeSingle<LeaseRow>();
  if (error) throw new Error("Lease lookup failed: " + error.message);
  if (!data) throw new ApiError(404, "Lease not found");
  if (requireActiveOwner) {
    const { data: invite, error: inviteError } = await supabase
      .from("dns_lease_invites")
      .select("enabled,expires_at")
      .eq("id", data.invite_id)
      .maybeSingle<{ enabled: boolean; expires_at: string | null }>();
    if (inviteError) {
      throw new Error("Lease owner lookup failed: " + inviteError.message);
    }
    if (
      !invite?.enabled ||
      (invite.expires_at && new Date(invite.expires_at).getTime() <= Date.now())
    ) {
      throw new ApiError(403, "Lease owner is no longer active");
    }
  }
  return data;
}

async function challengeTokenFor(row: LeaseRow) {
  if (
    !row.challenge_id ||
    !row.challenge_token_hash ||
    !row.challenge_expires_at
  ) {
    throw new Error("Lease challenge state is incomplete");
  }
  const token = await deriveChallengeToken(
    challengeSecret(),
    "lease-proof",
    row.challenge_id,
  );
  if ((await sha256Hex(token)) !== row.challenge_token_hash) {
    throw new Error(
      "DNS lease challenge secret does not match the stored challenge",
    );
  }
  return token;
}

function challengeResponse(row: LeaseRow, token: string) {
  return {
    ...publicLease(row),
    challengeToken: token,
    challengeUrl: challengeUrl(row.target_address, row.id),
    challengeExpiresAt: row.challenge_expires_at,
  };
}

async function claim(supabase: SupabaseClient, req: Request) {
  const inviteToken = bearerToken(req);
  if (!inviteToken) throw new ApiError(401, "Unauthorized");
  const body = await requestBody(req);
  if (!validIdempotencyKey(body.idempotencyKey)) {
    throw new ApiError(400, "Invalid idempotencyKey");
  }
  if (!validSecret(body.leaseToken)) {
    throw new ApiError(
      400,
      "leaseToken must contain 32 random bytes encoded as base64url",
    );
  }
  if ("challengeToken" in body) {
    throw new ApiError(400, "Challenge tokens are issued by the service");
  }

  let address: string;
  try {
    address = parsePublicDnsTarget(body.address, body.recordType);
  } catch (error) {
    throw new ApiError(
      400,
      error instanceof Error ? error.message : "Invalid address",
    );
  }

  const recordType = body.recordType as DnsRecordType;
  const pendingExpiresAt = new Date(
    Date.now() +
      envInt("DNS_LEASE_PENDING_TTL_SECONDS", 600, 60, 3_600) * 1_000,
  ).toISOString();
  const challengeId = crypto.randomUUID();
  const candidateChallenge = await deriveChallengeToken(
    challengeSecret(),
    "lease-proof",
    challengeId,
  );
  const parameters = {
    p_invite_token_hash: await sha256Hex(inviteToken),
    p_record_type: recordType,
    p_target_address: address,
    p_lease_token_hash: await sha256Hex(body.leaseToken),
    p_challenge_id: challengeId,
    p_challenge_token_hash: await sha256Hex(candidateChallenge),
    p_challenge_expires_at: pendingExpiresAt,
    p_idempotency_key: body.idempotencyKey,
    p_pending_expires_at: pendingExpiresAt,
  };

  let row: LeaseRow | undefined;
  for (let attempt = 0; attempt < 4; attempt++) {
    const { data, error } = await supabase.rpc("claim_dns_lease", {
      ...parameters,
      p_label: randomLabel(),
    });
    if (!error) {
      row = data as LeaseRow;
      break;
    }
    if (
      error.code === "23505" &&
      error.message.includes("dns_leases_label_key") &&
      attempt < 3
    ) {
      continue;
    }
    if (error.code === "23505") {
      throw new ApiError(
        409,
        "Lease token or idempotency key was already used",
      );
    }
    if (error.message.includes("invite_not_available")) {
      throw new ApiError(401, "Unauthorized");
    }
    if (error.message.includes("lease_quota_exceeded")) {
      throw new ApiError(429, "Active lease quota exceeded");
    }
    if (error.message.includes("lease_claim_limit_exceeded")) {
      throw new ApiError(429, "Invite lifetime claim limit exceeded");
    }
    if (error.message.includes("lease_global_rate_exceeded")) {
      throw new ApiError(429, "Managed DNS beta claim window is full");
    }
    if (error.message.includes("lease_policy_not_available")) {
      throw new Error("DNS lease policy is not configured");
    }
    if (error.message.includes("idempotency_key_payload_mismatch")) {
      throw new ApiError(
        409,
        "Idempotency key was already used with a different request",
      );
    }
    if (error.message.includes("idempotency_key_already_completed")) {
      throw new ApiError(
        409,
        "Idempotency key belongs to a completed or expired request",
      );
    }
    throw new Error("Lease claim failed: " + error.message);
  }
  if (!row) throw new Error("Could not allocate a unique DNS label");
  return json(201, challengeResponse(row, await challengeTokenFor(row)));
}

async function boundedStreamText(
  stream: ReadableStream<Uint8Array> | null,
  maximumBytes: number,
  tooLarge: ApiError,
) {
  if (!stream) return "";
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  let length = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    length += value.byteLength;
    if (length > maximumBytes) {
      await reader.cancel().catch(() => undefined);
      throw tooLarge;
    }
    chunks.push(value);
  }
  const body = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new TextDecoder("utf-8", { fatal: true }).decode(body);
}

async function fetchChallenge(
  row: LeaseRow,
  expectedTokenHash = row.challenge_token_hash,
  expiresAt = row.challenge_expires_at,
) {
  if (!expectedTokenHash || !expiresAt) {
    throw new ApiError(409, "Lease has no pending challenge");
  }
  if (new Date(expiresAt).getTime() <= Date.now()) {
    throw new ApiError(410, "Lease challenge expired");
  }
  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(),
    envInt("DNS_LEASE_CHALLENGE_TIMEOUT_MS", 5_000, 1_000, 15_000),
  );
  try {
    const response = await fetch(challengeUrl(row.target_address, row.id), {
      method: "GET",
      cache: "no-store",
      redirect: "error",
      signal: controller.signal,
      headers: { Accept: "text/plain" },
    });
    if (!response.ok) {
      throw new ApiError(
        409,
        "Challenge endpoint returned HTTP " + response.status,
      );
    }
    const declaredLength = Number(
      response.headers.get("content-length") ?? "0",
    );
    if (Number.isFinite(declaredLength) && declaredLength > 512) {
      throw new ApiError(409, "Challenge response is too large");
    }
    const responseBody = await boundedStreamText(
      response.body,
      512,
      new ApiError(409, "Challenge response is too large"),
    );
    if (new Date(expiresAt).getTime() <= Date.now()) {
      throw new ApiError(410, "Lease challenge expired");
    }
    if ((await sha256Hex(responseBody)) !== expectedTokenHash) {
      throw new ApiError(409, "Challenge response did not match");
    }
  } catch (error) {
    if (error instanceof ApiError) throw error;
    throw new ApiError(409, "Could not verify the address challenge");
  } finally {
    clearTimeout(timeout);
  }
}

function expectedComment(row: LeaseRow) {
  return "Peerbit managed lease " + row.id;
}

function recordHasManagedShape(row: LeaseRow, record: CloudflareDnsRecord) {
  return (
    record.name === row.domain &&
    record.type === row.record_type &&
    record.content === row.target_address &&
    record.comment === expectedComment(row) &&
    record.proxied === false
  );
}

function sameRecord(left: CloudflareDnsRecord, right: CloudflareDnsRecord) {
  return (
    left.id === right.id &&
    left.name === right.name &&
    left.type === right.type &&
    left.content === right.content &&
    left.comment === right.comment &&
    left.proxied === right.proxied
  );
}

function managedRecordsOrThrow(
  row: LeaseRow,
  records: CloudflareDnsRecord[],
) {
  if (!records.every((record) => recordHasManagedShape(row, record))) {
    throw new Error(
      "Refusing to operate on a DNS name that does not exactly match its lease",
    );
  }
  return records;
}

function assertZone(
  row: LeaseRow,
  observedZoneId: string,
  expectedZoneId: string,
) {
  if (observedZoneId !== expectedZoneId) {
    throw new Error("DNS broker changed zone identity during the operation");
  }
  if (row.cloudflare_zone_id && row.cloudflare_zone_id !== observedZoneId) {
    throw new Error("DNS lease belongs to a different provider zone");
  }
}

async function assertProvisioningOwned(
  supabase: SupabaseClient,
  row: LeaseRow,
  operationId: string,
  minimumRemainingSeconds = 0,
) {
  const minimumExpiry = new Date(
    Date.now() + minimumRemainingSeconds * 1_000,
  ).toISOString();
  const { data, error } = await supabase
    .from("dns_leases")
    .select("id")
    .eq("id", row.id)
    .eq("status", "provisioning")
    .eq("provisioning_id", operationId)
    .gt("provisioning_expires_at", minimumExpiry)
    .maybeSingle();
  if (error) {
    throw new Error(
      "Could not confirm provisioning ownership: " + error.message,
    );
  }
  if (!data) throw new ApiError(409, "Provisioning ownership expired");
}

async function persistProvisioningZone(
  supabase: SupabaseClient,
  row: LeaseRow,
  operationId: string,
  zoneId: string,
) {
  assertZone(row, zoneId, zoneId);
  const { data, error } = await supabase
    .from("dns_leases")
    .update({ cloudflare_zone_id: zoneId })
    .eq("id", row.id)
    .eq("status", "provisioning")
    .eq("provisioning_id", operationId)
    .gt(
      "provisioning_expires_at",
      new Date(
        Date.now() + PROVISIONING_MUTATION_SAFETY_SECONDS * 1_000,
      ).toISOString(),
    )
    .select("id,cloudflare_zone_id")
    .maybeSingle<{ id: string; cloudflare_zone_id: string }>();
  if (error) {
    throw new Error("Could not persist provisioning zone: " + error.message);
  }
  if (!data) throw new ApiError(409, "Provisioning ownership expired");
  row.cloudflare_zone_id = data.cloudflare_zone_id;
}

async function persistProvisioningRecord(
  supabase: SupabaseClient,
  row: LeaseRow,
  operationId: string,
  zoneId: string,
  record: CloudflareDnsRecord,
) {
  if (!recordHasManagedShape(row, record)) {
    throw new Error("Refusing to persist a mismatched managed DNS record");
  }
  const { data, error } = await supabase
    .from("dns_leases")
    .update({
      cloudflare_zone_id: zoneId,
      cloudflare_record_id: record.id,
    })
    .eq("id", row.id)
    .eq("status", "provisioning")
    .eq("provisioning_id", operationId)
    .eq("cloudflare_zone_id", zoneId)
    .gt(
      "provisioning_expires_at",
      new Date(
        Date.now() + PROVISIONING_MUTATION_SAFETY_SECONDS * 1_000,
      ).toISOString(),
    )
    .select("id,cloudflare_record_id")
    .maybeSingle<{ id: string; cloudflare_record_id: string }>();
  if (error) {
    throw new Error("Could not persist provisioned record: " + error.message);
  }
  if (!data) throw new ApiError(409, "Provisioning ownership expired");
  row.cloudflare_record_id = data.cloudflare_record_id;
}

async function verify(supabase: SupabaseClient, req: Request) {
  const body = await requestBody(req);
  const row = await authorizedLease(supabase, req, body);
  if (row.status === "active") return json(200, publicLease(row));
  if (row.status !== "pending" && row.status !== "provisioning") {
    throw new ApiError(409, "Lease cannot be verified while " + row.status);
  }
  if (
    new Date(row.pending_expires_at).getTime() <= Date.now() ||
    !row.challenge_expires_at ||
    new Date(row.challenge_expires_at).getTime() <= Date.now()
  ) {
    throw new ApiError(410, "Lease challenge expired");
  }

  const now = new Date();
  const verifyAvailableAt = new Date(
    now.getTime() +
      envInt("DNS_LEASE_VERIFY_INTERVAL_SECONDS", 30, 10, 300) * 1_000,
  ).toISOString();
  const operationId = crypto.randomUUID();
  const operationExpiresAt = new Date(
    now.getTime() + provisioningLockSeconds() * 1_000,
  ).toISOString();

  if (row.status === "pending") {
    const { data, error } = await supabase
      .from("dns_leases")
      .update({ verify_available_at: verifyAvailableAt })
      .eq("id", row.id)
      .eq("status", "pending")
      .eq("updated_at", row.updated_at)
      .lte("verify_available_at", now.toISOString())
      .select("id,updated_at")
      .maybeSingle<{ id: string; updated_at: string }>();
    if (error) {
      throw new Error(
        "Could not reserve challenge verification: " + error.message,
      );
    }
    if (!data) {
      throw new ApiError(429, "Challenge verification is rate limited");
    }
    row.updated_at = data.updated_at;
    await fetchChallenge(row);

    const { data: locked, error: lockError } = await supabase
      .from("dns_leases")
      .update({
        status: "provisioning",
        provisioning_id: operationId,
        provisioning_expires_at: operationExpiresAt,
        cleanup_available_at: operationExpiresAt,
        error_message: null,
      })
      .eq("id", row.id)
      .eq("status", "pending")
      .eq("updated_at", row.updated_at)
      .select("id,updated_at")
      .maybeSingle<{ id: string; updated_at: string }>();
    if (lockError) {
      throw new Error(
        "Could not lock lease for provisioning: " + lockError.message,
      );
    }
    if (!locked) {
      throw new ApiError(409, "Lease state changed during verification");
    }
    row.status = "provisioning";
    row.updated_at = locked.updated_at;
    row.provisioning_id = operationId;
    row.provisioning_expires_at = operationExpiresAt;
  } else {
    const { data, error } = await supabase
      .from("dns_leases")
      .update({
        provisioning_id: operationId,
        provisioning_expires_at: operationExpiresAt,
        cleanup_available_at: operationExpiresAt,
        error_message: null,
        verify_available_at: verifyAvailableAt,
      })
      .eq("id", row.id)
      .eq("status", "provisioning")
      .eq("updated_at", row.updated_at)
      .lte("provisioning_expires_at", now.toISOString())
      .lte("verify_available_at", now.toISOString())
      .select("id,updated_at")
      .maybeSingle<{ id: string; updated_at: string }>();
    if (error) {
      throw new Error(
        "Could not lock lease provisioning retry: " + error.message,
      );
    }
    if (!data) throw new ApiError(409, "Lease is already being provisioned");
    row.updated_at = data.updated_at;
    row.provisioning_id = operationId;
    row.provisioning_expires_at = operationExpiresAt;
    await fetchChallenge(row);
  }

  const cf = dnsConfig();
  let zoneId: string;
  let record: CloudflareDnsRecord;
  try {
    zoneId = await verifyDnsZone(cf);
    assertZone(row, zoneId, zoneId);
    // Persist provider identity before the first DNS read or mutation. If an
    // external response is lost, cleanup is thereby pinned to the same zone.
    await persistProvisioningZone(supabase, row, operationId, zoneId);

    let records: CloudflareDnsRecord[];
    if (row.cloudflare_record_id) {
      records = await verifiedRecordsForDeletion(row, cf, zoneId);
    } else {
      const initial = await findDnsRecords(cf, row.domain);
      assertZone(row, initial.zoneId, zoneId);
      records = managedRecordsOrThrow(row, initial.records);
    }
    if (records.length === 0) {
      await assertProvisioningOwned(
        supabase,
        row,
        operationId,
        PROVISIONING_MUTATION_SAFETY_SECONDS,
      );
      const created = await createDnsRecord(cf, {
        id: row.id,
        domain: row.domain,
        recordType: row.record_type,
        address: row.target_address,
      });
      assertZone(row, created.zoneId, zoneId);
      record = created.record;
    } else {
      record = row.cloudflare_record_id
        ? records.find(
          (candidate) => candidate.id === row.cloudflare_record_id,
        ) ?? [...records].sort((left, right) =>
          left.id.localeCompare(right.id)
        )[0]
        : [...records].sort((left, right) =>
          left.id.localeCompare(right.id)
        )[0];
    }
    // Persist the canonical immutable ID before duplicate reconciliation. All
    // retries then choose the same record, independent of provider list order.
    await persistProvisioningRecord(
      supabase,
      row,
      operationId,
      zoneId,
      record,
    );

    await assertProvisioningOwned(supabase, row, operationId);
    const confirmation = await findDnsRecords(cf, row.domain);
    assertZone(row, confirmation.zoneId, zoneId);
    records = managedRecordsOrThrow(row, confirmation.records);
    if (!records.some((candidate) => candidate.id === record.id)) {
      throw new ApiError(
        409,
        "DNS name did not remain assigned to this lease",
      );
    }
    await deleteManagedRecords(
      row,
      cf,
      zoneId,
      records.filter((duplicate) => duplicate.id !== record.id),
      () =>
        assertProvisioningOwned(
          supabase,
          row,
          operationId,
          PROVISIONING_MUTATION_SAFETY_SECONDS,
        ),
    );
    const final = await findDnsRecords(cf, row.domain);
    assertZone(row, final.zoneId, zoneId);
    const finalRecords = managedRecordsOrThrow(row, final.records);
    if (finalRecords.length !== 1 || finalRecords[0].id !== record.id) {
      throw new ApiError(
        409,
        "DNS name did not remain exclusively assigned to this lease",
      );
    }
    record = finalRecords[0];
  } catch (error) {
    await supabase
      .from("dns_leases")
      .update({
        error_message: error instanceof Error
          ? error.message.slice(0, 500)
          : "DNS provisioning failed",
      })
      .eq("id", row.id)
      .eq("status", "provisioning")
      .eq("provisioning_id", operationId);
    throw error;
  }

  const timestamp = new Date().toISOString();
  const expiresAt = new Date(
    Date.now() +
      envInt("DNS_LEASE_ACTIVE_TTL_SECONDS", 86_400, 3_600, 604_800) * 1_000,
  ).toISOString();
  const { data, error } = await supabase
    .from("dns_leases")
    .update({
      status: "active",
      cloudflare_record_id: record.id,
      cloudflare_zone_id: zoneId,
      challenge_id: null,
      challenge_token_hash: null,
      challenge_expires_at: null,
      provisioning_id: null,
      provisioning_expires_at: null,
      cleanup_available_at: timestamp,
      verify_available_at: timestamp,
      lease_expires_at: expiresAt,
      verified_at: timestamp,
      error_message: null,
    })
    .eq("id", row.id)
    .eq("status", "provisioning")
    .eq("provisioning_id", operationId)
    .gt("provisioning_expires_at", timestamp)
    .select(LEASE_COLUMNS)
    .maybeSingle<LeaseRow>();
  if (error) {
    throw new Error("Could not activate provisioned lease: " + error.message);
  }
  if (!data) {
    throw new ApiError(
      409,
      "Provisioning ownership expired; cleanup will reconcile the record",
    );
  }
  return json(200, publicLease(data));
}

async function renewChallenge(supabase: SupabaseClient, req: Request) {
  const body = await requestBody(req);
  if ("challengeToken" in body) {
    throw new ApiError(400, "Challenge tokens are issued by the service");
  }
  let row = await authorizedLease(supabase, req, body);
  if (row.status !== "active" || !row.lease_expires_at) {
    throw new ApiError(409, "Only active leases can be renewed");
  }
  const now = new Date();
  const leaseExpiry = new Date(row.lease_expires_at);
  if (leaseExpiry.getTime() <= now.getTime()) {
    throw new ApiError(410, "Lease has expired");
  }

  if (
    row.challenge_id &&
    row.challenge_token_hash &&
    row.challenge_expires_at &&
    new Date(row.challenge_expires_at).getTime() > now.getTime()
  ) {
    return json(200, challengeResponse(row, await challengeTokenFor(row)));
  }

  const challengeId = crypto.randomUUID();
  const token = await deriveChallengeToken(
    challengeSecret(),
    "lease-proof",
    challengeId,
  );
  const ttl = envInt(
    "DNS_LEASE_RENEW_CHALLENGE_TTL_SECONDS",
    600,
    60,
    1_800,
  );
  const challengeExpiryMs = Math.min(
    now.getTime() + ttl * 1_000,
    leaseExpiry.getTime(),
  );
  if (challengeExpiryMs <= now.getTime() + 10_000) {
    throw new ApiError(410, "Lease is too close to expiry to renew");
  }
  const challengeExpiresAt = new Date(challengeExpiryMs).toISOString();
  const verifyAvailableAt = new Date(
    now.getTime() +
      envInt("DNS_LEASE_RENEW_INTERVAL_SECONDS", 60, 30, 3_600) * 1_000,
  ).toISOString();
  const cleanupAvailableAt = new Date(
    challengeExpiryMs +
      envInt("DNS_LEASE_CLEANUP_LOCK_SECONDS", 180, 120, 600) * 1_000,
  ).toISOString();
  const { data, error } = await supabase
    .from("dns_leases")
    .update({
      challenge_id: challengeId,
      challenge_token_hash: await sha256Hex(token),
      challenge_expires_at: challengeExpiresAt,
      verify_available_at: verifyAvailableAt,
      cleanup_available_at: cleanupAvailableAt,
    })
    .eq("id", row.id)
    .eq("status", "active")
    .eq("updated_at", row.updated_at)
    .gt("lease_expires_at", now.toISOString())
    .lte("verify_available_at", now.toISOString())
    .select(LEASE_COLUMNS)
    .maybeSingle<LeaseRow>();
  if (error) {
    throw new Error("Could not issue renewal challenge: " + error.message);
  }
  if (data) {
    row = data;
    return json(200, challengeResponse(row, await challengeTokenFor(row)));
  }

  const { data: concurrent, error: concurrentError } = await supabase
    .from("dns_leases")
    .select(LEASE_COLUMNS)
    .eq("id", row.id)
    .eq("lease_token_hash", row.lease_token_hash)
    .maybeSingle<LeaseRow>();
  if (concurrentError) {
    throw new Error(
      "Could not recover concurrent renewal challenge: " +
        concurrentError.message,
    );
  }
  if (
    concurrent?.status === "active" &&
    concurrent.challenge_id &&
    concurrent.challenge_expires_at &&
    new Date(concurrent.challenge_expires_at).getTime() > Date.now()
  ) {
    return json(
      200,
      challengeResponse(concurrent, await challengeTokenFor(concurrent)),
    );
  }
  throw new ApiError(429, "Renewal challenge is rate limited or state changed");
}

async function verifiedRecordsForDeletion(
  row: LeaseRow,
  cf: CloudflareDnsConfig,
  zoneId: string,
) {
  const listed = await findDnsRecords(cf, row.domain);
  assertZone(row, listed.zoneId, zoneId);
  const records = managedRecordsOrThrow(row, listed.records);

  if (!row.cloudflare_record_id) return records;

  const fetched = await getDnsRecord(cf, row.cloudflare_record_id);
  assertZone(row, fetched.zoneId, zoneId);
  if (!fetched.record) {
    if (records.length === 0) return [];
    throw new Error(
      "Refusing to release a lease whose immutable record disappeared while its DNS name remains occupied",
    );
  }
  if (
    fetched.record.id !== row.cloudflare_record_id ||
    !recordHasManagedShape(row, fetched.record)
  ) {
    throw new Error(
      "Refusing to delete an immutable DNS record that no longer exactly matches its lease",
    );
  }
  const listedCopy = records.find(
    (record) => record.id === row.cloudflare_record_id,
  );
  if (!listedCopy || !sameRecord(fetched.record, listedCopy)) {
    throw new Error(
      "Refusing to delete a DNS record whose ID and name reads disagree",
    );
  }
  return records;
}

async function deleteVerifiedRecords(
  row: LeaseRow,
  cf: CloudflareDnsConfig,
  zoneId: string,
  records: CloudflareDnsRecord[],
) {
  await deleteManagedRecords(row, cf, zoneId, records);
  const remaining = await verifiedRecordsForDeletion(row, cf, zoneId);
  if (remaining.length !== 0) {
    throw new Error("Managed DNS records remained after deletion");
  }
}

async function deleteManagedRecords(
  row: LeaseRow,
  cf: CloudflareDnsConfig,
  zoneId: string,
  records: CloudflareDnsRecord[],
  beforeDelete?: () => Promise<void>,
) {
  let next = 0;
  await Promise.all(
    Array.from({ length: Math.min(5, records.length) }, async () => {
      while (next < records.length) {
        const record = records[next++];
        if (beforeDelete) await beforeDelete();
        const deletedZone = await deleteDnsRecord(cf, {
          id: record.id,
          leaseId: row.id,
          name: row.domain,
          type: row.record_type,
          address: row.target_address,
        });
        assertZone(row, deletedZone, zoneId);
      }
    }),
  );
}

async function renew(supabase: SupabaseClient, req: Request) {
  const body = await requestBody(req);
  if ("challengeToken" in body) {
    throw new ApiError(400, "Challenge tokens are issued by the service");
  }
  const row = await authorizedLease(supabase, req, body);
  if (row.status !== "active" || !row.lease_expires_at) {
    throw new ApiError(409, "Only active leases can be renewed");
  }
  if (new Date(row.lease_expires_at).getTime() <= Date.now()) {
    throw new ApiError(410, "Lease has expired");
  }
  if (
    !row.challenge_id ||
    !row.challenge_token_hash ||
    !row.challenge_expires_at ||
    new Date(row.challenge_expires_at).getTime() <= Date.now()
  ) {
    throw new ApiError(409, "Request a fresh renewal challenge first");
  }
  const consumedChallengeId = row.challenge_id;
  const consumedChallengeHash = row.challenge_token_hash;
  const consumedChallengeExpiry = row.challenge_expires_at;
  const now = new Date();
  const { data: reserved, error: reserveError } = await supabase
    .from("dns_leases")
    .update({
      challenge_id: null,
      challenge_token_hash: null,
      challenge_expires_at: null,
      cleanup_available_at: new Date(
        now.getTime() +
          envInt("DNS_LEASE_CLEANUP_LOCK_SECONDS", 180, 120, 600) * 1_000,
      ).toISOString(),
    })
    .eq("id", row.id)
    .eq("status", "active")
    .eq("updated_at", row.updated_at)
    .eq("challenge_id", consumedChallengeId)
    .gt("challenge_expires_at", now.toISOString())
    .gt("lease_expires_at", now.toISOString())
    .select("id,updated_at")
    .maybeSingle<{ id: string; updated_at: string }>();
  if (reserveError) {
    throw new Error("Could not reserve lease renewal: " + reserveError.message);
  }
  if (!reserved) {
    throw new ApiError(409, "Renewal challenge was consumed or state changed");
  }
  row.updated_at = reserved.updated_at;

  try {
    await fetchChallenge(
      row,
      consumedChallengeHash,
      consumedChallengeExpiry,
    );

    const cf = dnsConfig();
    const zoneId = await verifyDnsZone(cf);
    assertZone(row, zoneId, zoneId);
    let records: CloudflareDnsRecord[];
    try {
      records = await verifiedRecordsForDeletion(row, cf, zoneId);
    } catch (error) {
      if (error instanceof Error && error.message.startsWith("Refusing to")) {
        throw new ApiError(
          409,
          "Managed DNS record no longer matches its lease",
        );
      }
      throw new ApiError(502, "Could not verify the managed DNS record");
    }
    if (records.length === 0) {
      throw new ApiError(409, "Managed DNS record is missing");
    }
    if (records.length > 1) {
      const keep = records.find(
        (record) => record.id === row.cloudflare_record_id,
      ) ?? records[0];
      await deleteManagedRecords(
        row,
        cf,
        zoneId,
        records.filter((record) => record.id !== keep.id),
      );
      records = await verifiedRecordsForDeletion(row, cf, zoneId);
      if (records.length !== 1 || records[0].id !== keep.id) {
        throw new ApiError(
          502,
          "Could not reconcile duplicate managed records",
        );
      }
    }
  } catch (error) {
    const recovery = await recoverConsumedRenewalAvailability(supabase, {
      id: row.id,
      reservedUpdatedAt: row.updated_at,
    });
    if (recovery.error) {
      console.error("Could not recover consumed renewal availability", {
        leaseId: row.id,
        message: recovery.error.slice(0, 500),
      });
    }
    throw error;
  }

  const timestamp = new Date().toISOString();
  const expiresAt = new Date(
    Date.now() +
      envInt("DNS_LEASE_ACTIVE_TTL_SECONDS", 86_400, 3_600, 604_800) * 1_000,
  ).toISOString();
  const { data, error } = await supabase
    .from("dns_leases")
    .update({
      cleanup_available_at: timestamp,
      lease_expires_at: expiresAt,
      renewed_at: timestamp,
      error_message: null,
    })
    .eq("id", row.id)
    .eq("status", "active")
    .eq("updated_at", row.updated_at)
    .select(LEASE_COLUMNS)
    .maybeSingle<LeaseRow>();
  if (error) throw new Error("Could not renew lease: " + error.message);
  if (!data) throw new ApiError(409, "Lease state changed during renewal");
  return json(200, publicLease(data));
}

async function releasePendingLease(
  supabase: SupabaseClient,
  row: LeaseRow,
) {
  const { data, error } = await supabase
    .from("dns_leases")
    .update({
      status: "released",
      challenge_id: null,
      challenge_token_hash: null,
      challenge_expires_at: null,
      released_at: new Date().toISOString(),
      error_message: null,
    })
    .eq("id", row.id)
    .eq("status", "pending")
    .eq("updated_at", row.updated_at)
    .select(LEASE_COLUMNS)
    .maybeSingle<LeaseRow>();
  if (error) {
    throw new Error("Could not release pending lease: " + error.message);
  }
  if (!data) throw new ApiError(409, "Lease state changed; retry release");
  return data;
}

async function release(supabase: SupabaseClient, req: Request) {
  const body = await requestBody(req);
  const row = await authorizedLease(supabase, req, body, false);
  if (row.status === "released" || row.status === "expired") {
    return json(200, publicLease(row));
  }
  if (row.status === "pending") {
    return json(200, publicLease(await releasePendingLease(supabase, row)));
  }
  if (row.status === "provisioning") {
    throw new ApiError(
      409,
      "Lease is currently being provisioned; retry release shortly",
    );
  }
  if (row.status === "expiring") {
    throw new ApiError(409, "Lease is currently expiring");
  }
  if (row.status === "release_pending") {
    throw new ApiError(409, "Lease is already being released");
  }
  if (row.status === "quarantined") {
    throw new ApiError(409, "Lease requires manual DNS cleanup");
  }
  if (row.status !== "active") {
    throw new ApiError(409, "Lease cannot be released while " + row.status);
  }

  const { data: locked, error: stateError } = await supabase
    .from("dns_leases")
    .update({
      status: "release_pending",
      challenge_id: null,
      challenge_token_hash: null,
      challenge_expires_at: null,
      cleanup_available_at: new Date(
        Date.now() +
          envInt("DNS_LEASE_CLEANUP_LOCK_SECONDS", 180, 120, 600) * 1_000,
      ).toISOString(),
    })
    .eq("id", row.id)
    .eq("status", "active")
    .eq("updated_at", row.updated_at)
    .select("id,updated_at")
    .maybeSingle<{ id: string; updated_at: string }>();
  if (stateError) {
    throw new Error("Could not prepare lease release: " + stateError.message);
  }
  if (!locked) throw new ApiError(409, "Lease state changed; retry release");
  row.status = "release_pending";
  row.updated_at = locked.updated_at;

  try {
    const cf = dnsConfig();
    const zoneId = await verifyDnsZone(cf);
    assertZone(row, zoneId, zoneId);
    const records = await verifiedRecordsForDeletion(row, cf, zoneId);
    if (records.length > 0) {
      await deleteVerifiedRecords(row, cf, zoneId, records);
    }
  } catch (error) {
    await supabase
      .from("dns_leases")
      .update({
        error_message: error instanceof Error
          ? error.message.slice(0, 500)
          : "DNS release failed",
      })
      .eq("id", row.id)
      .eq("status", "release_pending")
      .eq("updated_at", row.updated_at);
    throw new ApiError(
      502,
      "Could not safely release the DNS record; cleanup will retry",
    );
  }

  const { data, error } = await supabase
    .from("dns_leases")
    .update({
      status: "released",
      released_at: new Date().toISOString(),
      error_message: null,
    })
    .eq("id", row.id)
    .eq("status", "release_pending")
    .eq("updated_at", row.updated_at)
    .select(LEASE_COLUMNS)
    .maybeSingle<LeaseRow>();
  if (error) {
    throw new Error("Could not finish lease release: " + error.message);
  }
  if (!data) throw new ApiError(409, "Lease state changed during release");
  return json(200, publicLease(data));
}

async function cleanupPending(
  supabase: SupabaseClient,
  row: LeaseRow,
) {
  const terminalStatus = row.status === "release_pending"
    ? "released"
    : "expired";
  const { data, error } = await supabase
    .from("dns_leases")
    .update({
      status: terminalStatus,
      challenge_id: null,
      challenge_token_hash: null,
      challenge_expires_at: null,
      released_at: terminalStatus === "released"
        ? new Date().toISOString()
        : null,
      cleanup_failures: 0,
      error_message: null,
    })
    .eq("id", row.id)
    .eq("status", row.status)
    .eq("updated_at", row.updated_at)
    .select("id")
    .maybeSingle();
  if (error) {
    throw new Error("Could not tombstone pending lease: " + error.message);
  }
  return data
    ? { id: row.id, status: terminalStatus }
    : { id: row.id, status: "state_changed" };
}

async function cleanupOne(
  supabase: SupabaseClient,
  row: LeaseRow,
  cf?: CloudflareDnsConfig,
  zoneId?: string,
) {
  if (row.status === "pending") {
    try {
      return await cleanupPending(supabase, row);
    } catch (error) {
      return {
        id: row.id,
        status: "error",
        error: error instanceof Error ? error.message : "Cleanup failed",
      };
    }
  }

  let cleanupLockAcquired = false;
  try {
    if (!cf || !zoneId) throw new Error("DNS broker is not available");
    const terminalStatus = row.status === "release_pending"
      ? "released"
      : "expired";
    const cleanupStatus = row.status === "release_pending"
      ? "release_pending"
      : "expiring";
    const lockUntil = new Date(
      Date.now() +
        envInt("DNS_LEASE_CLEANUP_LOCK_SECONDS", 180, 120, 600) * 1_000,
    ).toISOString();
    const { data: locked, error: lockError } = await supabase
      .from("dns_leases")
      .update({
        status: cleanupStatus,
        challenge_id: null,
        challenge_token_hash: null,
        challenge_expires_at: null,
        provisioning_id: null,
        provisioning_expires_at: null,
        cleanup_available_at: lockUntil,
        error_message: null,
      })
      .eq("id", row.id)
      .eq("status", row.status)
      .eq("updated_at", row.updated_at)
      .lte("cleanup_available_at", new Date().toISOString())
      .select("id,updated_at")
      .maybeSingle<{ id: string; updated_at: string }>();
    if (lockError) {
      throw new Error("Could not lock lease for cleanup: " + lockError.message);
    }
    if (!locked) return { id: row.id, status: "state_changed" };
    cleanupLockAcquired = true;
    row.status = cleanupStatus;
    row.updated_at = locked.updated_at;
    assertZone(row, zoneId, zoneId);
    const records = await verifiedRecordsForDeletion(row, cf, zoneId);
    if (records.length > 0) {
      await deleteVerifiedRecords(row, cf, zoneId, records);
    }
    const { data: updated, error: updateError } = await supabase
      .from("dns_leases")
      .update({
        status: terminalStatus,
        released_at: terminalStatus === "released"
          ? new Date().toISOString()
          : null,
        cleanup_failures: 0,
        challenge_id: null,
        challenge_token_hash: null,
        challenge_expires_at: null,
        provisioning_id: null,
        provisioning_expires_at: null,
        error_message: null,
      })
      .eq("id", row.id)
      .eq("status", row.status)
      .eq("updated_at", row.updated_at)
      .select("id")
      .maybeSingle();
    if (updateError) {
      throw new Error("Could not tombstone lease: " + updateError.message);
    }
    return updated
      ? { id: row.id, status: terminalStatus }
      : { id: row.id, status: "state_changed" };
  } catch (error) {
    const message = error instanceof Error
      ? error.message.slice(0, 500)
      : "Cleanup failed";
    const failures = Math.min(20, row.cleanup_failures + 1);
    const quarantineAfter = envInt(
      "DNS_LEASE_CLEANUP_QUARANTINE_FAILURES",
      12,
      3,
      20,
    );
    const quarantineRequested = failures >= quarantineAfter;
    const shouldQuarantine = cleanupLockAcquired && quarantineRequested;
    const retrySeconds = Math.min(21_600, 60 * 2 ** (failures - 1));
    console.error("DNS lease cleanup failed", {
      failures,
      leaseId: row.id,
      quarantineRequested,
      cleanupLockAcquired,
      message,
      status: row.status,
    });

    const failureUpdate: Record<string, unknown> = {
      status: shouldQuarantine ? "quarantined" : row.status,
      quarantined_at: shouldQuarantine ? new Date().toISOString() : null,
      cleanup_available_at: new Date(
        Date.now() + retrySeconds * 1_000,
      ).toISOString(),
      cleanup_failures: failures,
      error_message: message,
    };
    if (shouldQuarantine) {
      Object.assign(failureUpdate, {
        challenge_id: null,
        challenge_token_hash: null,
        challenge_expires_at: null,
        provisioning_id: null,
        provisioning_expires_at: null,
      });
    }

    let persisted:
      | { id: string; status: LeaseStatus }
      | null = null;
    try {
      const { data, error: persistError } = await supabase
        .from("dns_leases")
        .update(failureUpdate)
        .eq("id", row.id)
        .eq("status", row.status)
        .eq("updated_at", row.updated_at)
        .select("id,status")
        .maybeSingle<{ id: string; status: LeaseStatus }>();
      if (persistError) {
        console.error("Could not persist DNS lease cleanup failure", {
          leaseId: row.id,
          message: persistError.message,
        });
        return {
          id: row.id,
          status: "error",
          error: message,
          persisted: false,
        };
      }
      persisted = data;
    } catch (persistError) {
      console.error("Could not persist DNS lease cleanup failure", {
        leaseId: row.id,
        message: persistError instanceof Error
          ? persistError.message
          : "Unknown persistence failure",
      });
      return {
        id: row.id,
        status: "error",
        error: message,
        persisted: false,
      };
    }

    if (!persisted) {
      return {
        id: row.id,
        status: "state_changed",
        error: message,
        persisted: false,
      };
    }
    return {
      id: row.id,
      status: persisted.status === "quarantined" ? "quarantined" : "error",
      error: message,
      persisted: true,
    };
  }
}

async function timingSafeSecretEqual(left: string, right: string) {
  const encoder = new TextEncoder();
  const [leftHash, rightHash] = await Promise.all([
    crypto.subtle.digest("SHA-256", encoder.encode(left)),
    crypto.subtle.digest("SHA-256", encoder.encode(right)),
  ]);
  const leftBytes = new Uint8Array(leftHash);
  const rightBytes = new Uint8Array(rightHash);
  let difference = 0;
  for (let index = 0; index < leftBytes.length; index++) {
    difference |= leftBytes[index] ^ rightBytes[index];
  }
  return difference === 0;
}

async function cleanup(supabase: SupabaseClient, req: Request) {
  const expected = Deno.env.get("DNS_LEASE_CLEANUP_SECRET");
  const provided = bearerToken(req);
  if (
    !expected ||
    !provided ||
    !(await timingSafeSecretEqual(provided, expected))
  ) {
    throw new ApiError(401, "Unauthorized");
  }
  const now = new Date();
  const [pending, provisioning, active, releasing, expiring] = await Promise
    .all([
      supabase
        .from("dns_leases")
        .select(LEASE_COLUMNS)
        .eq("status", "pending")
        .lte("pending_expires_at", now.toISOString())
        .lte("cleanup_available_at", now.toISOString())
        .order("cleanup_available_at", { ascending: true })
        .limit(2),
      supabase
        .from("dns_leases")
        .select(LEASE_COLUMNS)
        .eq("status", "provisioning")
        .lte("pending_expires_at", now.toISOString())
        .lte("provisioning_expires_at", now.toISOString())
        .lte("cleanup_available_at", now.toISOString())
        .order("cleanup_available_at", { ascending: true })
        .limit(2),
      supabase
        .from("dns_leases")
        .select(LEASE_COLUMNS)
        .eq("status", "active")
        .lte("lease_expires_at", now.toISOString())
        .lte("cleanup_available_at", now.toISOString())
        .order("cleanup_available_at", { ascending: true })
        .limit(2),
      supabase
        .from("dns_leases")
        .select(LEASE_COLUMNS)
        .eq("status", "release_pending")
        .lte("cleanup_available_at", now.toISOString())
        .order("cleanup_available_at", { ascending: true })
        .limit(2),
      supabase
        .from("dns_leases")
        .select(LEASE_COLUMNS)
        .eq("status", "expiring")
        .lte("cleanup_available_at", now.toISOString())
        .order("cleanup_available_at", { ascending: true })
        .limit(2),
    ]);
  for (const result of [pending, provisioning, active, releasing, expiring]) {
    if (result.error) {
      throw new Error("Could not list stale leases: " + result.error.message);
    }
  }
  const pendingRows = (pending.data ?? []) as LeaseRow[];
  // Rows that never entered provisioning cannot have a provider record. Reap
  // them before contacting the broker so a DNS outage cannot consume quota.
  const pendingResults = await Promise.all(
    pendingRows.map((row) => cleanupOne(supabase, row)),
  );
  const stale = [releasing, expiring, active, provisioning]
    .flatMap((result) => (result.data ?? []) as LeaseRow[]);
  if (stale.length === 0) {
    return json(200, {
      processed: pendingResults.length,
      results: pendingResults,
    });
  }

  const cf = dnsConfig();
  let zoneId: string;
  try {
    zoneId = await verifyDnsZone(cf);
  } catch (error) {
    console.error("DNS lease cleanup zone preflight failed", error);
    throw new ApiError(
      502,
      "DNS broker is unavailable; pending-only cleanup still completed",
    );
  }
  const dnsResults: Array<Awaited<ReturnType<typeof cleanupOne>>> = new Array(
    stale.length,
  );
  let next = 0;
  await Promise.all(
    Array.from({ length: Math.min(5, stale.length) }, async () => {
      while (next < stale.length) {
        const index = next++;
        dnsResults[index] = await cleanupOne(
          supabase,
          stale[index],
          cf,
          zoneId,
        );
      }
    }),
  );
  const results = [...dnsResults, ...pendingResults];
  return json(200, { processed: results.length, results });
}

Deno.serve(async (req) => {
  if (req.method !== "POST") return json(405, { error: "Method not allowed" });
  const supabaseUrl = Deno.env.get("SUPABASE_URL");
  const serviceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
  if (!supabaseUrl || !serviceKey) {
    return json(500, { error: "Service is not configured" });
  }
  const supabase = createClient(supabaseUrl, serviceKey, {
    auth: { persistSession: false },
  });

  try {
    switch (actionFromUrl(new URL(req.url))) {
      case "claim":
        return await claim(supabase, req);
      case "verify":
        return await verify(supabase, req);
      case "renew-challenge":
        return await renewChallenge(supabase, req);
      case "renew":
        return await renew(supabase, req);
      case "release":
        return await release(supabase, req);
      case "cleanup":
        return await cleanup(supabase, req);
      default:
        return json(404, { error: "Not found" });
    }
  } catch (error) {
    if (error instanceof ApiError) {
      return json(error.status, { error: error.message });
    }
    console.error(error);
    return json(500, { error: "Internal service error" });
  }
});
