import { deserialize, serialize } from "@dao-xyz/borsh";
import { Ed25519Keypair, fromBase64, toBase64 } from "@peerbit/crypto";
import sodium from "libsodium-wrappers";
import { v4 as uuid } from "uuid";
import { FastMutex } from "./lockstorage.ts";

const CLIENT_ID_STORAGE_KEY = "CLIENT_ID";
const ID_COUNTER_KEY = "idc/";

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
	let idCounter = JSON.parse(localStorage.getItem(idCounterKey) || "0");
	for (let i = 0; i < 10000; i++) {
		const key = getKeyId(id, i);
		const lockedInfo = lock.getLockedInfo(key);
		if (lockedInfo) {
			if (
				(lockedInfo === lock.clientId && options?.releaseLockIfSameId) ||
				options?.releaseFirstLock
			) {
				await lock.release(key);
			} else {
				continue;
			}
		}

		await lock.lock(key, lockCondition);
		localStorage.setItem(
			idCounterKey,
			JSON.stringify(Math.max(idCounter, i + 1)),
		);
		await lock.release(idCounterKey);
		return {
			index: i,
			path: key,
			key: await getKeypair(key),
		};
	}
	throw new Error("Failed to resolve key");
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
