export const MANAGED_ZONE_NAME = "peerchecker.com";
export const MANAGED_NAME_PATTERN = /^p-[0-9a-f]{20}\.nodes\.peerchecker\.com$/;

const LEASE_ID_PATTERN =
	/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const RECORD_ID_PATTERN = /^[0-9a-f]{32}$/;
const MANAGED_COMMENT_PATTERN = new RegExp(
	`^Peerbit managed lease (${LEASE_ID_PATTERN.source.slice(1, -1)})$`,
);

export type DnsRecordType = "A" | "AAAA";

export type ManagedDnsRecord = {
	id: string;
	name: string;
	type: DnsRecordType;
	content: string;
	ttl: number;
	proxied: false;
	comment: string;
};

export class ValidationError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "ValidationError";
	}
}

export class RecordConflictError extends Error {
	constructor(message = "The DNS record is not a valid Peerbit managed lease") {
		super(message);
		this.name = "RecordConflictError";
	}
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function requireObject(
	value: unknown,
	description = "JSON body",
): Record<string, unknown> {
	if (!isRecord(value)) {
		throw new ValidationError(`${description} must be an object`);
	}
	return value;
}

export function requireExactKeys(
	object: Record<string, unknown>,
	required: readonly string[],
	optional: readonly string[] = [],
): void {
	const allowed = new Set([...required, ...optional]);
	for (const key of Object.keys(object)) {
		if (!allowed.has(key)) {
			throw new ValidationError(`Unexpected JSON field: ${key}`);
		}
	}
	for (const key of required) {
		if (!Object.hasOwn(object, key)) {
			throw new ValidationError(`Missing JSON field: ${key}`);
		}
	}
}

function requireString(value: unknown, field: string): string {
	if (typeof value !== "string") {
		throw new ValidationError(`${field} must be a string`);
	}
	return value;
}

export function requireManagedName(value: unknown): string {
	const name = requireString(value, "name");
	if (!MANAGED_NAME_PATTERN.test(name)) {
		throw new ValidationError(
			"name must match p-<20 lowercase hex>.nodes.peerchecker.com",
		);
	}
	return name;
}

export function requireRecordType(value: unknown): DnsRecordType {
	if (value !== "A" && value !== "AAAA") {
		throw new ValidationError("type must be A or AAAA");
	}
	return value;
}

export function requireLeaseId(value: unknown): string {
	const id = requireString(value, "leaseId");
	if (!LEASE_ID_PATTERN.test(id)) {
		throw new ValidationError("leaseId must be a canonical UUID");
	}
	return id;
}

export function requireRecordId(value: unknown): string {
	const id = requireString(value, "recordId");
	if (!RECORD_ID_PATTERN.test(id)) {
		throw new ValidationError(
			"recordId must be 32 lowercase hexadecimal characters",
		);
	}
	return id;
}

export function managedComment(leaseId: string): string {
	return `Peerbit managed lease ${leaseId}`;
}

export function requireAllowedTtl(value: unknown): number {
	if (
		typeof value !== "number" ||
		!Number.isInteger(value) ||
		(value !== 1 && (value < 60 || value > 86_400))
	) {
		throw new ValidationError(
			"ttl must be 1 (automatic) or an integer from 60 through 86400 seconds",
		);
	}
	return value;
}

function parseIpv4(address: string): number[] | undefined {
	const parts = address.split(".");
	if (parts.length !== 4) return undefined;
	const octets: number[] = [];
	for (const part of parts) {
		if (!/^(0|[1-9][0-9]{0,2})$/.test(part)) return undefined;
		const octet = Number(part);
		if (octet > 255) return undefined;
		octets.push(octet);
	}
	return octets;
}

function ipv4Number(octets: readonly number[]): number {
	return (
		(((octets[0] << 24) >>> 0) +
			(octets[1] << 16) +
			(octets[2] << 8) +
			octets[3]) >>>
		0
	);
}

function inIpv4Cidr(value: number, base: number, bits: number): boolean {
	const mask = bits === 0 ? 0 : (0xffffffff << (32 - bits)) >>> 0;
	return (value & mask) === (base & mask);
}

function isPublicIpv4(octets: readonly number[]): boolean {
	const value = ipv4Number(octets);
	const blocked: ReadonlyArray<readonly [number, number]> = [
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
	return !blocked.some(([base, bits]) => inIpv4Cidr(value, base, bits));
}

function parseIpv6(input: string): number[] | undefined {
	if (input.length === 0 || input.includes("%")) return undefined;
	let address = input.toLowerCase();

	if (address.includes(".")) {
		const finalColon = address.lastIndexOf(":");
		if (finalColon < 0) return undefined;
		const octets = parseIpv4(address.slice(finalColon + 1));
		if (!octets) return undefined;
		const first = (octets[0] << 8) | octets[1];
		const second = (octets[2] << 8) | octets[3];
		address = `${address.slice(0, finalColon)}:${first.toString(16)}:${second.toString(16)}`;
	}

	const doubleColon = address.indexOf("::");
	if (doubleColon !== -1 && doubleColon !== address.lastIndexOf("::")) {
		return undefined;
	}

	const parseSide = (side: string): number[] | undefined => {
		if (side === "") return [];
		const groups = side.split(":");
		const words: number[] = [];
		for (const group of groups) {
			if (!/^[0-9a-f]{1,4}$/.test(group)) return undefined;
			words.push(Number.parseInt(group, 16));
		}
		return words;
	};

	if (doubleColon === -1) {
		const words = parseSide(address);
		return words?.length === 8 ? words : undefined;
	}

	const left = parseSide(address.slice(0, doubleColon));
	const right = parseSide(address.slice(doubleColon + 2));
	if (!left || !right || left.length + right.length >= 8) return undefined;
	return [
		...left,
		...Array<number>(8 - left.length - right.length).fill(0),
		...right,
	];
}

function ipv6BigInt(words: readonly number[]): bigint {
	let value = 0n;
	for (const word of words) value = (value << 16n) | BigInt(word);
	return value;
}

function ipv6Prefix(value: bigint, base: bigint, bits: number): boolean {
	return value >> BigInt(128 - bits) === base >> BigInt(128 - bits);
}

function isPublicIpv6(words: readonly number[]): boolean {
	const value = ipv6BigInt(words);
	// Public DNS leases intentionally accept only the currently allocated global
	// unicast block. This rejects unspecified, loopback, local, mapped, NAT64,
	// link-local, and multicast addresses by construction.
	if (!ipv6Prefix(value, 0x20000000000000000000000000000000n, 3)) {
		return false;
	}

	const blocked: ReadonlyArray<readonly [bigint, number]> = [
		// IETF protocol assignments, benchmarking, ORCHID, and Teredo.
		[0x20010000000000000000000000000000n, 23],
		// Documentation ranges.
		[0x20010db8000000000000000000000000n, 32],
		[0x3fff0000000000000000000000000000n, 20],
		// Deprecated 6to4 does not provide a reliable directly reachable endpoint.
		[0x20020000000000000000000000000000n, 16],
	];
	return !blocked.some(([base, bits]) => ipv6Prefix(value, base, bits));
}

function canonicalIpv6(words: readonly number[]): string {
	let bestStart = -1;
	let bestLength = 0;
	for (let start = 0; start < words.length; ) {
		if (words[start] !== 0) {
			start += 1;
			continue;
		}
		let end = start;
		while (end < words.length && words[end] === 0) end += 1;
		const length = end - start;
		if (length >= 2 && length > bestLength) {
			bestStart = start;
			bestLength = length;
		}
		start = end;
	}

	const hex = words.map((word) => word.toString(16));
	if (bestStart < 0) return hex.join(":");
	const left = hex.slice(0, bestStart).join(":");
	const right = hex.slice(bestStart + bestLength).join(":");
	if (!left && !right) return "::";
	if (!left) return `::${right}`;
	if (!right) return `${left}::`;
	return `${left}::${right}`;
}

export function requirePublicAddress(
	value: unknown,
	type: DnsRecordType,
): string {
	const address = requireString(value, "address");
	if (type === "A") {
		const octets = parseIpv4(address);
		if (!octets || !isPublicIpv4(octets)) {
			throw new ValidationError(
				"address must be a public IPv4 address for an A record",
			);
		}
		return octets.join(".");
	}

	const words = parseIpv6(address);
	if (!words || !isPublicIpv6(words)) {
		throw new ValidationError(
			"address must be a public IPv6 address for an AAAA record",
		);
	}
	return canonicalIpv6(words);
}

function conflictUnless(condition: boolean): asserts condition {
	if (!condition) throw new RecordConflictError();
}

export function parseManagedDnsRecord(value: unknown): ManagedDnsRecord {
	conflictUnless(isRecord(value));
	const id = value.id;
	const name = value.name;
	const type = value.type;
	const content = value.content;
	const ttl = value.ttl;
	const proxied = value.proxied;
	const comment = value.comment;

	conflictUnless(typeof id === "string" && RECORD_ID_PATTERN.test(id));
	conflictUnless(typeof name === "string" && MANAGED_NAME_PATTERN.test(name));
	conflictUnless(type === "A" || type === "AAAA");
	conflictUnless(typeof content === "string");
	conflictUnless(typeof ttl === "number");
	conflictUnless(proxied === false);
	conflictUnless(
		typeof comment === "string" && MANAGED_COMMENT_PATTERN.test(comment),
	);

	let canonicalContent: string;
	try {
		canonicalContent = requirePublicAddress(content, type);
		requireAllowedTtl(ttl);
	} catch {
		throw new RecordConflictError();
	}

	return {
		id,
		name,
		type,
		content: canonicalContent,
		ttl,
		proxied: false,
		comment,
	};
}

export type ExpectedLeaseRecord = {
	name: string;
	type: DnsRecordType;
	address: string;
	leaseId: string;
	ttl?: number;
};

export function recordMatchesLease(
	record: ManagedDnsRecord,
	expected: ExpectedLeaseRecord,
): boolean {
	return (
		record.name === expected.name &&
		record.type === expected.type &&
		record.content === expected.address &&
		record.comment === managedComment(expected.leaseId) &&
		(expected.ttl === undefined || record.ttl === expected.ttl)
	);
}
