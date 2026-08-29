import type { Entry } from "@peerbit/log";
import { equals as bytesEqual } from "uint8arrays";

export type ReceiveSignatureVerificationFact<T> = {
	entry: Entry<T>;
	storageBytes: Uint8Array;
	verified: boolean;
};

export const receiveSignatureVerificationResult = <T>(
	factsByEntry: WeakMap<Entry<T>, ReceiveSignatureVerificationFact<T>[]>,
	entry: Entry<T>,
): boolean | undefined => {
	const facts = factsByEntry.get(entry);
	if (!facts || facts.length === 0) {
		return undefined;
	}
	let storageBytes: Uint8Array;
	try {
		storageBytes = entry.getStorageBytes();
	} catch {
		return undefined;
	}
	for (let i = facts.length - 1; i >= 0; i--) {
		const fact = facts[i]!;
		if (bytesEqual(storageBytes, fact.storageBytes)) {
			return fact.verified;
		}
	}
	return undefined;
};

export const withReceiveSignatureVerificationFacts = async <T, R>(
	factsByEntry: WeakMap<Entry<T>, ReceiveSignatureVerificationFact<T>[]>,
	facts: ReceiveSignatureVerificationFact<T>[] | undefined,
	operation: () => Promise<R>,
): Promise<R> => {
	if (!facts || facts.length === 0) {
		return operation();
	}
	for (const fact of facts) {
		const active = factsByEntry.get(fact.entry);
		if (active) {
			active.push(fact);
		} else {
			factsByEntry.set(fact.entry, [fact]);
		}
	}
	try {
		return await operation();
	} finally {
		for (const fact of facts) {
			const active = factsByEntry.get(fact.entry);
			if (!active) {
				continue;
			}
			const index = active.indexOf(fact);
			if (index >= 0) {
				active.splice(index, 1);
			}
			if (active.length === 0) {
				factsByEntry.delete(fact.entry);
			}
		}
	}
};
