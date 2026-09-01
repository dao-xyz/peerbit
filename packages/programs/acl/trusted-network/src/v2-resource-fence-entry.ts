import { equals } from "uint8arrays";
import {
	type AuthenticatedAuthorityEntryV0V2,
	authenticateAuthorityEntryV0V2,
} from "./v2-authority-entry.js";
import {
	NetworkDescriptorV2,
	ResourceFenceV2,
	TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_DIRECT_PARENTS,
	TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_ENTRY_BYTES,
	TRUSTED_NETWORK_V2_RESOURCE_FENCE_BODY_BYTES,
	copyUint8ArrayWithLengthV2,
	decodeResourceFenceV2,
	exactUint8ArrayByteLengthV2,
} from "./v2.js";

const RESOURCE_FENCE_AUTHORITY_LIMITS_V2 = Object.freeze({
	maximumEntryBytes: TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_ENTRY_BYTES,
	maximumDirectParents: TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_DIRECT_PARENTS,
	exactPayloadBytes: TRUSTED_NETWORK_V2_RESOURCE_FENCE_BODY_BYTES,
});

export type AuthenticatedResourceFenceEntryV2 = Readonly<{
	entryBytes: Uint8Array;
	entryCid: string;
	digest: Uint8Array;
	body: ResourceFenceV2;
	gid: string;
	metaData?: Uint8Array;
	directParents: ReadonlyArray<Readonly<{ cid: string; digest: Uint8Array }>>;
}>;

export type AuthenticateResourceFenceEntryV2Properties = Readonly<{
	entryBytes: Uint8Array;
	descriptor: NetworkDescriptorV2;
	expectedResourceId: Uint8Array;
	expectedGid: string;
}>;

const captureResourceIdV2 = (resourceId: Uint8Array): Uint8Array => {
	let byteLength: number;
	try {
		byteLength = exactUint8ArrayByteLengthV2(resourceId);
	} catch {
		throw new Error("Expected resource id must contain exactly 32 bytes");
	}
	if (byteLength !== 32) {
		throw new Error("Expected resource id must contain exactly 32 bytes");
	}
	return copyUint8ArrayWithLengthV2(resourceId, byteLength);
};

const toResourceFenceTokenV2 = (
	authenticated: AuthenticatedAuthorityEntryV0V2,
	body: ResourceFenceV2,
): AuthenticatedResourceFenceEntryV2 => ({
	entryBytes: authenticated.entryBytes,
	entryCid: authenticated.entryCid,
	digest: authenticated.entryDigest,
	body,
	gid: authenticated.gid,
	metaData: authenticated.metaData,
	directParents: authenticated.directParents,
});

/**
 * Authenticate a raw resource-fence EntryV0 against immutable resource
 * context. The returned token does not prove policy acceptance, causal
 * ancestry, fork freedom, freshness, or durability.
 */
export const authenticateResourceFenceEntryV2 = async ({
	entryBytes,
	descriptor,
	expectedResourceId,
	expectedGid,
}: AuthenticateResourceFenceEntryV2Properties): Promise<AuthenticatedResourceFenceEntryV2> => {
	if (typeof expectedGid !== "string") {
		throw new Error("Expected resource gid must be a string");
	}
	const capturedResourceId = captureResourceIdV2(expectedResourceId);
	const authenticated = await authenticateAuthorityEntryV0V2(
		entryBytes,
		descriptor,
		RESOURCE_FENCE_AUTHORITY_LIMITS_V2,
	);
	if (authenticated.gid !== expectedGid) {
		throw new Error("Resource fence belongs to another resource log");
	}
	const body = decodeResourceFenceV2(
		authenticated.payloadBytes,
		authenticated.descriptor,
	);
	if (!equals(body.resourceId, capturedResourceId)) {
		throw new Error("Resource fence belongs to another resource");
	}
	return toResourceFenceTokenV2(authenticated, body);
};
