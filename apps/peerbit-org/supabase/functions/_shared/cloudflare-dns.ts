import type { DnsRecordType } from "./dns-lease.ts";

const CLOUDFLARE_ID_PATTERN = /^[0-9a-f]{32}$/;

class DnsBrokerRequestError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

export type CloudflareDnsRecord = {
  id: string;
  name: string;
  type: string;
  content: string;
  comment: string | null;
  proxied: boolean;
};

/** Configuration for the narrow Cloudflare Worker broker, not Cloudflare's API. */
export type CloudflareDnsConfig = {
  brokerUrl: string;
  brokerSecret: string;
  ttl: number;
  requestTimeoutMs: number;
};

type BrokerResponse = {
  zoneId?: unknown;
  record?: unknown;
  records?: unknown;
  deletedId?: unknown;
  error?: unknown;
};

async function boundedResponseJson(response: Response) {
  const maximumBytes = 65_536;
  const declaredLength = Number(response.headers.get("content-length") ?? "0");
  if (Number.isFinite(declaredLength) && declaredLength > maximumBytes) {
    throw new DnsBrokerRequestError(
      response.status,
      "DNS broker response was too large",
    );
  }
  const reader = response.body?.getReader();
  if (!reader) return {} as BrokerResponse;
  const chunks: Uint8Array[] = [];
  let length = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    length += value.byteLength;
    if (length > maximumBytes) {
      await reader.cancel().catch(() => undefined);
      throw new DnsBrokerRequestError(
        response.status,
        "DNS broker response was too large",
      );
    }
    chunks.push(value);
  }
  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  try {
    return JSON.parse(
      new TextDecoder("utf-8", { fatal: true }).decode(bytes),
    ) as BrokerResponse;
  } catch {
    throw new DnsBrokerRequestError(
      response.status,
      "DNS broker returned invalid JSON",
    );
  }
}

async function brokerRequest(
  config: CloudflareDnsConfig,
  path: string,
  payload: Record<string, unknown>,
  fetchFn: typeof fetch,
) {
  const response = await fetchFn(
    `${config.brokerUrl.replace(/\/$/, "")}${path}`,
    {
      method: "POST",
      signal: AbortSignal.timeout(config.requestTimeoutMs),
      headers: {
        Authorization: `Bearer ${config.brokerSecret}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    },
  );
  const body = await boundedResponseJson(response);
  if (!response.ok) {
    const nestedError = body.error && typeof body.error === "object"
      ? (body.error as Record<string, unknown>).message
      : undefined;
    const message = typeof body.error === "string"
      ? body.error
      : typeof nestedError === "string"
      ? nestedError
      : undefined;
    const detail = message ? `: ${message}` : "";
    throw new DnsBrokerRequestError(
      response.status,
      `DNS broker request failed (HTTP ${response.status})${detail}`,
    );
  }
  if (
    typeof body.zoneId !== "string" ||
    !CLOUDFLARE_ID_PATTERN.test(body.zoneId)
  ) {
    throw new DnsBrokerRequestError(
      response.status,
      "DNS broker returned an invalid zone identity",
    );
  }
  return body as BrokerResponse & { zoneId: string };
}

function isDnsRecord(value: unknown): value is CloudflareDnsRecord {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const record = value as Record<string, unknown>;
  return (
    typeof record.id === "string" && CLOUDFLARE_ID_PATTERN.test(record.id) &&
    typeof record.name === "string" &&
    typeof record.type === "string" &&
    typeof record.content === "string" &&
    (typeof record.comment === "string" || record.comment === null) &&
    typeof record.proxied === "boolean"
  );
}

export async function verifyDnsZone(
  config: CloudflareDnsConfig,
  fetchFn: typeof fetch = fetch,
) {
  const body = await brokerRequest(config, "/zone", {}, fetchFn);
  return body.zoneId;
}

export async function getDnsRecord(
  config: CloudflareDnsConfig,
  recordId: string,
  fetchFn: typeof fetch = fetch,
) {
  if (!CLOUDFLARE_ID_PATTERN.test(recordId)) {
    throw new Error(
      "DNS record ID must be 32 lowercase hexadecimal characters",
    );
  }
  const body = await brokerRequest(
    config,
    "/records/get",
    { recordId },
    fetchFn,
  );
  if (body.record === null) return { zoneId: body.zoneId, record: undefined };
  if (!isDnsRecord(body.record)) {
    throw new DnsBrokerRequestError(
      502,
      "DNS broker returned an invalid record",
    );
  }
  return { zoneId: body.zoneId, record: body.record };
}

export async function findDnsRecords(
  config: CloudflareDnsConfig,
  name: string,
  type?: DnsRecordType,
  fetchFn: typeof fetch = fetch,
) {
  const body = await brokerRequest(
    config,
    "/records/list",
    type ? { name, type } : { name },
    fetchFn,
  );
  if (!Array.isArray(body.records) || !body.records.every(isDnsRecord)) {
    throw new DnsBrokerRequestError(502, "DNS broker returned invalid records");
  }
  return {
    zoneId: body.zoneId,
    records: body.records as CloudflareDnsRecord[],
  };
}

export async function createDnsRecord(
  config: CloudflareDnsConfig,
  lease: {
    id: string;
    domain: string;
    recordType: DnsRecordType;
    address: string;
  },
  fetchFn: typeof fetch = fetch,
) {
  const body = await brokerRequest(
    config,
    "/records/create",
    {
      leaseId: lease.id,
      name: lease.domain,
      type: lease.recordType,
      address: lease.address,
      ttl: config.ttl,
    },
    fetchFn,
  );
  if (!isDnsRecord(body.record)) {
    throw new DnsBrokerRequestError(
      502,
      "DNS broker returned an invalid record",
    );
  }
  const comment = `Peerbit managed lease ${lease.id}`;
  if (
    body.record.name !== lease.domain ||
    body.record.type !== lease.recordType ||
    body.record.content !== lease.address ||
    body.record.comment !== comment ||
    body.record.proxied !== false
  ) {
    throw new Error(
      "DNS broker returned a record that does not match the requested lease",
    );
  }
  return { zoneId: body.zoneId, record: body.record };
}

export async function deleteDnsRecord(
  config: CloudflareDnsConfig,
  record: {
    id: string;
    leaseId: string;
    name: string;
    type: DnsRecordType;
    address: string;
  },
  fetchFn: typeof fetch = fetch,
) {
  if (!CLOUDFLARE_ID_PATTERN.test(record.id)) {
    throw new Error(
      "DNS record ID must be 32 lowercase hexadecimal characters",
    );
  }
  const body = await brokerRequest(
    config,
    "/records/delete",
    {
      recordId: record.id,
      leaseId: record.leaseId,
      name: record.name,
      type: record.type,
      address: record.address,
    },
    fetchFn,
  );
  if (body.deletedId !== record.id && body.deletedId !== null) {
    throw new Error("DNS broker confirmed deletion for a different DNS record");
  }
  return body.zoneId;
}
