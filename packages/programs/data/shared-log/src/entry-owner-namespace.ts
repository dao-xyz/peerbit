import { toHexString } from "@peerbit/crypto";

type Member = {
	protectedStore: boolean;
	leases: Map<symbol, object | undefined>;
};

type Registry = {
	version: 1;
	namespaces: Map<string, Map<object, Member>>;
};
const registryKey = Symbol.for("@peerbit/shared-log/entry-owner-namespaces");

// This process-local reservation couples lower storage and document projections.
// It is not a persistent permission check against direct/offline storage access.
// A node-bound symbol coordinates separately evaluated copies of this generation.
// Older libraries without this contract are not supported mixed-runtime owners.
export const claimEntryOwnerNamespace = (
	node: object,
	log: object,
	id: Uint8Array,
	protectedStore: boolean,
	projectionOwner?: object,
) => {
	const namespace = toHexString(id);
	const descriptor = Object.getOwnPropertyDescriptor(node, registryKey);
	let registry = descriptor?.value as Registry | undefined;
	if (
		descriptor &&
		(!("value" in descriptor) ||
			!registry ||
			typeof registry !== "object" ||
			registry.version !== 1 ||
			!(registry.namespaces instanceof Map))
	) {
		throw new Error(
			"Unsupported shared-log namespace reservation version or shape",
		);
	}
	let nodeNamespaces = registry?.namespaces;
	let members = nodeNamespaces?.get(namespace);
	if (members !== undefined && !(members instanceof Map)) {
		throw new Error(
			"Unsupported shared-log namespace reservation member shape",
		);
	}
	const existing = members?.get(log);
	for (const [memberLog, member] of members ?? []) {
		if (
			!member ||
			typeof member.protectedStore !== "boolean" ||
			!(member.leases instanceof Map)
		) {
			throw new Error(
				"Unsupported shared-log namespace reservation member shape",
			);
		}
		if (
			(protectedStore || member.protectedStore) &&
			(memberLog !== log || protectedStore !== member.protectedStore)
		) {
			throw new Error("Shared-log namespace already has an incompatible owner");
		}
	}
	if (
		protectedStore &&
		projectionOwner &&
		Array.from(existing?.leases.values() ?? []).some(
			(owner) => owner !== undefined,
		)
	) {
		throw new Error("Shared-log namespace already has a protected projection");
	}
	if (!nodeNamespaces) {
		nodeNamespaces = new Map();
		registry = { version: 1, namespaces: nodeNamespaces };
		Object.defineProperty(node, registryKey, {
			value: registry,
			configurable: true,
		});
	}
	if (!members) {
		members = new Map();
		nodeNamespaces.set(namespace, members);
	}
	const member = existing ?? { protectedStore, leases: new Map() };
	members.set(log, member);
	const token = Symbol();
	member.leases.set(token, projectionOwner);
	return {
		namespace,
		protectedStore,
		releaseAfterTerminalCleanup: () => {
			if (!member.leases.delete(token) || member.leases.size > 0) return;
			if (members.get(log) === member) members.delete(log);
			if (members.size === 0 && nodeNamespaces.get(namespace) === members) {
				nodeNamespaces.delete(namespace);
			}
			if (
				nodeNamespaces.size === 0 &&
				Object.getOwnPropertyDescriptor(node, registryKey)?.value === registry
			) {
				Reflect.deleteProperty(node, registryKey);
			}
		},
	};
};
