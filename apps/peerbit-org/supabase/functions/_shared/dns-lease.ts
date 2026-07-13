export type DnsRecordType = "A" | "AAAA";

const IPV4_BLOCKS: ReadonlyArray<readonly [number, number]> = [
  [0x00000000, 8],
  [0x0a000000, 8],
  [0x64400000, 10],
  [0x7f000000, 8],
  [0xa9fe0000, 16],
  [0xac100000, 12],
  [0xc0000000, 24],
  [0xc0000200, 24],
  [0xc0586300, 24],
  [0xc0a80000, 16],
  [0xc6120000, 15],
  [0xc6336400, 24],
  [0xcb007100, 24],
  [0xe0000000, 4],
  [0xf0000000, 4],
];

function parseIpv4(value: string) {
  const parts = value.split(".");
  if (parts.length !== 4) return;
  const octets = parts.map((part) => {
    if (!/^(0|[1-9][0-9]{0,2})$/.test(part)) return Number.NaN;
    const octet = Number(part);
    return octet <= 255 ? octet : Number.NaN;
  });
  if (octets.some(Number.isNaN)) return;
  return octets;
}

function ipv4Number(octets: number[]) {
  return (
    (octets[0] * 0x1000000 +
      octets[1] * 0x10000 +
      octets[2] * 0x100 +
      octets[3]) >>>
    0
  );
}

function isBlockedIpv4(value: number) {
  return IPV4_BLOCKS.some(([network, prefix]) => {
    const mask = prefix === 0 ? 0 : (0xffffffff << (32 - prefix)) >>> 0;
    return (value & mask) >>> 0 === (network & mask) >>> 0;
  });
}

function parseIpv6(value: string) {
  let source = value.toLowerCase();
  if (source.includes(".")) {
    const lastColon = source.lastIndexOf(":");
    if (lastColon < 0) return;
    const ipv4 = parseIpv4(source.slice(lastColon + 1));
    if (!ipv4) return;
    source = `${source.slice(0, lastColon)}:${
      ((ipv4[0] << 8) | ipv4[1]).toString(16)
    }:${((ipv4[2] << 8) | ipv4[3]).toString(16)}`;
  }

  if (!/^[0-9a-f:]+$/.test(source) || source.split("::").length > 2) return;
  const compressed = source.includes("::");
  const [leftText, rightText = ""] = source.split("::");
  const left = leftText ? leftText.split(":") : [];
  const right = rightText ? rightText.split(":") : [];
  if (
    [...left, ...right].some((part) => !/^[0-9a-f]{1,4}$/.test(part)) ||
    (!compressed && left.length !== 8) ||
    (compressed && left.length + right.length >= 8)
  ) {
    return;
  }
  const zeros = compressed ? 8 - left.length - right.length : 0;
  return [...left, ...Array(zeros).fill("0"), ...right].map((part) =>
    Number.parseInt(part, 16)
  );
}

function normalizeIpv6(parts: number[]) {
  let bestStart = -1;
  let bestLength = 0;
  for (let index = 0; index < parts.length;) {
    if (parts[index] !== 0) {
      index++;
      continue;
    }
    let end = index;
    while (end < parts.length && parts[end] === 0) end++;
    if (end - index > bestLength && end - index >= 2) {
      bestStart = index;
      bestLength = end - index;
    }
    index = end;
  }

  if (bestStart < 0) return parts.map((part) => part.toString(16)).join(":");
  const left = parts
    .slice(0, bestStart)
    .map((part) => part.toString(16))
    .join(":");
  const right = parts
    .slice(bestStart + bestLength)
    .map((part) => part.toString(16))
    .join(":");
  return `${left}::${right}`;
}

export function parsePublicDnsTarget(value: unknown, recordType: unknown) {
  if (recordType !== "A" && recordType !== "AAAA") {
    throw new Error("recordType must be A or AAAA");
  }
  if (
    typeof value !== "string" ||
    value.length > 64 ||
    value.trim() !== value
  ) {
    throw new Error("address must be a plain IP address");
  }

  if (recordType === "A") {
    const octets = parseIpv4(value);
    if (!octets) throw new Error("address is not a valid IPv4 address");
    if (isBlockedIpv4(ipv4Number(octets))) {
      throw new Error("address must be publicly routable");
    }
    return octets.join(".");
  }

  const parts = parseIpv6(value);
  if (!parts) throw new Error("address is not a valid IPv6 address");
  // Only global-unicast addresses are accepted; conservative special-purpose and documentation
  // allocations are excluded even when they sit inside 2000::/3.
  if (
    parts[0] < 0x2000 ||
    parts[0] > 0x3fff ||
    (parts[0] === 0x2001 && parts[1] <= 0x01ff) ||
    (parts[0] === 0x2001 && parts[1] === 0x0db8) ||
    parts[0] === 0x2002 ||
    (parts[0] === 0x3fff && parts[1] <= 0x0fff)
  ) {
    throw new Error("address must be publicly routable");
  }
  return normalizeIpv6(parts);
}

export function challengeUrl(address: string, leaseId: string) {
  const host = address.includes(":") ? `[${address}]` : address;
  return `http://${host}/.well-known/peerbit-dns/${leaseId}`;
}

export function bearerToken(req: Request) {
  const header = req.headers.get("authorization") ?? "";
  if (!header.startsWith("Bearer ")) return;
  const token = header.slice("Bearer ".length);
  return token.length >= 32 && token.length <= 512 ? token : undefined;
}

export function validIdempotencyKey(value: unknown): value is string {
  return typeof value === "string" && /^[A-Za-z0-9._:-]{8,128}$/.test(value);
}

export function validSecret(value: unknown): value is string {
  // 32 random bytes in unpadded base64url form. Requiring this exact shape prevents weak caller-generated secrets.
  return typeof value === "string" && /^[A-Za-z0-9_-]{43}$/.test(value);
}

function decodeBase64Url(value: string) {
  const base64 = value.replace(/-/g, "+").replace(/_/g, "/") + "=";
  const decoded = atob(base64);
  return Uint8Array.from(decoded, (character) => character.charCodeAt(0));
}

function encodeBase64Url(value: ArrayBuffer) {
  let binary = "";
  for (const byte of new Uint8Array(value)) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(
    /=+$/,
    "",
  );
}

/**
 * Derive an opaque, recoverable proof token from server-only key material.
 * The database stores only the challenge UUID and the token hash; an
 * idempotent retry can derive the same clear token without persisting it.
 */
export async function deriveChallengeToken(
  secret: string,
  scope: string,
  challengeId: string,
) {
  if (!validSecret(secret)) {
    throw new Error(
      "DNS_LEASE_CHALLENGE_SECRET must be 32 random bytes encoded as base64url",
    );
  }
  const key = await crypto.subtle.importKey(
    "raw",
    decodeBase64Url(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const signature = await crypto.subtle.sign(
    "HMAC",
    key,
    new TextEncoder().encode(
      `peerbit-dns-challenge:v1:${scope}:${challengeId}`,
    ),
  );
  return encodeBase64Url(signature);
}
