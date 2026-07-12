import { deserialize, serialize } from "@dao-xyz/borsh";
import { Ed25519Keypair, fromBase64, toBase64 } from "@peerbit/crypto";
import sodium from "libsodium-wrappers";
import { v4 as uuid } from "uuid";
import { acquireWebLock, FastMutex, type WebLockLease } from "./lockstorage.ts";

const CLIENT_ID_STORAGE_KEY = "CLIENT_ID";
const ID_COUNTER_KEY = "idc/";
export const WEB_LOCK_CLIENT_ID_PREFIX = "@peerbit/react:web-lock/v1:";
const WEB_LOCK_NAME_PREFIX = "@peerbit/react:";

const getKeyId = (prefix: string, id: number) => `${prefix}/${id}`;

export const cookiesWhereClearedJustNow = () => {
	const lastPersistedAt = localStorage.getItem("lastPersistedAt");
	if (lastPersistedAt) {
		return false;
	}
	localStorage.setItem("lastPersistedAt", Date.now().toString());
	return true;
};

export const getClientId = (type: "session" | "local") => {
	const storage = type === "session" ? sessionStorage : localStorage;
	const idFromStorage = storage.getItem(CLIENT_ID_STORAGE_KEY);
	if (idFromStorage) {
		return idFromStorage;
	}
	const id = uuid();
	storage.setItem(CLIENT_ID_STORAGE_KEY, id);
	return id;
};

export const releaseKey = (path: string, lock: FastMutex) => {
	lock.release(path);
};

export const createWebLockClientId = () =>
	`${WEB_LOCK_CLIENT_ID_PREFIX}${uuid()}`;

const isWebLockClientId = (clientId: string) =>
	clientId.startsWith(WEB_LOCK_CLIENT_ID_PREFIX);

const getSelectionLockName = (id: string) =>
	`${WEB_LOCK_NAME_PREFIX}keypair-selection:${JSON.stringify(id)}`;

const getKeypairLockName = (id: string, index: number) =>
	`${WEB_LOCK_NAME_PREFIX}keypair:${JSON.stringify([id, index])}`;

const getAbortReason = (signal: AbortSignal) =>
	signal.reason ?? new DOMException("Aborted", "AbortError");

const throwIfAborted = (signal?: AbortSignal) => {
	if (signal?.aborted) {
		throw getAbortReason(signal);
	}
};

const waitForLegacyCounter = async (
	key: string,
	lock: FastMutex,
	signal?: AbortSignal,
) => {
	const startedAt = Date.now();
	while (true) {
		throwIfAborted(signal);
		const owners = lock.getLockedOwners(key);
		if (owners.length === 0) {
			return;
		}
		if (owners.every(isWebLockClientId)) {
			lock.releaseIfOwnedBy(key, isWebLockClientId);
			return;
		}
		if (Date.now() - startedAt >= lock.timeout * 2 + 50) {
			throw new Error("Timed out waiting for the legacy key counter lock");
		}
		await new Promise((resolve) => setTimeout(resolve, 10));
	}
};

const acquirePinnedKeypairLock = async ({
	id,
	index,
	lock,
	lockManager,
	lockCondition,
	signal,
}: {
	id: string;
	index: number;
	lock: FastMutex;
	lockManager: LockManager;
	lockCondition: () => boolean;
	signal?: AbortSignal;
}): Promise<WebLockLease | undefined> => {
	const webLock = await acquireWebLock(
		lockManager,
		getKeypairLockName(id, index),
		{ ifAvailable: true, signal },
	);
	if (!webLock) {
		return;
	}

	const key = getKeyId(id, index);
	const owners = lock.getLockedOwners(key);
	if (owners.some((owner) => !isWebLockClientId(owner))) {
		webLock.release();
		return;
	}
	lock.releaseIfOwnedBy(key, isWebLockClientId);

	try {
		await lock.lock(key, lockCondition);
		lock.pin(key);
	} catch (error) {
		try {
			lock.release(key);
		} finally {
			webLock.release();
		}
		throw error;
	}

	let released = false;
	const release = () => {
		if (released) return;
		released = true;
		signal?.removeEventListener("abort", release);
		try {
			lock.release(key);
		} finally {
			webLock.release();
		}
	};
	signal?.addEventListener("abort", release, { once: true });
	if (signal?.aborted) {
		release();
		throw getAbortReason(signal);
	}

	return {
		release,
		detachAbort: () => {
			signal?.removeEventListener("abort", release);
			webLock.detachAbort();
		},
	};
};

export const getFreeKeypair = async (
	id = "",
	lock: FastMutex,
	lockCondition: () => boolean = () => true,
	options?: {
		releaseLockIfSameId?: boolean;
		releaseFirstLock?: boolean;
	},
) => {
	await sodium.ready;
	const idCounterKey = ID_COUNTER_KEY + id;
	await lock.lock(idCounterKey, () => true);
	try {
		const idCounter = JSON.parse(localStorage.getItem(idCounterKey) || "0");
		for (let i = 0; i < 10000; i++) {
			const key = getKeyId(id, i);
			const lockedInfo = lock.getLockedInfo(key);
			if (lockedInfo) {
				if (
					lockedInfo === lock.clientId &&
					options?.releaseLockIfSameId
				) {
					lock.release(key);
				} else if (options?.releaseFirstLock) {
					lock.releaseIfOwnedBy(key, () => true);
				} else {
					continue;
				}
			}

			await lock.lock(key, lockCondition);
			localStorage.setItem(
				idCounterKey,
				JSON.stringify(Math.max(idCounter, i + 1)),
			);
			return {
				index: i,
				path: key,
				key: await getKeypair(key),
			};
		}
		throw new Error("Failed to resolve key");
	} finally {
		lock.release(idCounterKey);
	}
};

export const getFreeKeypairWithWebLock = async (
	id: string,
	lock: FastMutex,
	lockManager: LockManager,
	lockCondition: () => boolean = () => true,
	signal?: AbortSignal,
) => {
	if (!isWebLockClientId(lock.clientId)) {
		throw new Error(
			"Web Lock key allocation requires a tagged mutex client ID",
		);
	}
	await sodium.ready;
	throwIfAborted(signal);

	const selectionLock = await acquireWebLock(
		lockManager,
		getSelectionLockName(id),
		{ signal },
	);
	if (!selectionLock) {
		throw new Error("Failed to acquire the keypair selection lock");
	}

	const idCounterKey = ID_COUNTER_KEY + id;
	let counterLocked = false;
	try {
		await waitForLegacyCounter(idCounterKey, lock, signal);
		await lock.lock(idCounterKey, () => true);
		counterLocked = true;
		const idCounter = JSON.parse(localStorage.getItem(idCounterKey) || "0");

		for (let i = 0; i < 10000; i++) {
			throwIfAborted(signal);
			const selected = await acquirePinnedKeypairLock({
				id,
				index: i,
				lock,
				lockManager,
				lockCondition,
				signal,
			});
			if (!selected) {
				continue;
			}

			const path = getKeyId(id, i);
			try {
				const key = await getKeypair(path);
				throwIfAborted(signal);
				localStorage.setItem(
					idCounterKey,
					JSON.stringify(Math.max(idCounter, i + 1)),
				);
				selected.detachAbort();
				return { index: i, path, key, release: selected.release };
			} catch (error) {
				selected.release();
				throw error;
			}
		}
		throw new Error("Failed to resolve key");
	} finally {
		try {
			if (counterLocked) {
				lock.release(idCounterKey);
			}
		} finally {
			selectionLock.release();
		}
	}
};

export const getAllKeyPairs = async (id = "") => {
	const idCounterKey = ID_COUNTER_KEY + id;
	const counter = JSON.parse(localStorage.getItem(idCounterKey) || "0");
	const ret: Ed25519Keypair[] = [];
	for (let i = 0; i < counter; i++) {
		const key = getKeyId(id, i);
		const kp = loadKeypair(key);
		if (kp) {
			ret.push(kp);
		}
	}
	return ret;
};

let _getKeypair: Promise<Ed25519Keypair> | undefined;

export const getKeypair = async (keyName: string): Promise<Ed25519Keypair> => {
	await _getKeypair;
	const fn = async () => {
		let keypair = loadKeypair(keyName);
		if (keypair) {
			return keypair;
		}

		keypair = await Ed25519Keypair.create();
		saveKeypair(keyName, keypair);
		return keypair;
	};
	_getKeypair = fn();
	return _getKeypair;
};

const saveKeypair = (path: string, key: Ed25519Keypair) => {
	const str = toBase64(serialize(key));
	localStorage.setItem(`_keys/${path}`, str);
};

const loadKeypair = (path: string) => {
	const item = localStorage.getItem(`_keys/${path}`);
	if (!item) {
		return undefined;
	}
	return deserialize(fromBase64(item), Ed25519Keypair);
};

export const inIframe = () => {
	try {
		return window.self !== window.top;
	} catch {
		return true;
	}
};
