import * as indexerTypes from "@peerbit/indexer-interface";
import { Entry } from "@peerbit/log";
import {
	DeleteByStringKeyOperation,
	DeleteOperation,
	type Operation,
	PutOperation,
	PutWithKeyOperation,
} from "./operation.js";

/**
 * Borsh byte fields are input-buffer views. Clone only fields that can expose
 * verified operation bytes while retaining the operation's exact prototype,
 * subclass fields, symbols, and property descriptors.
 */
export const detachOperationBytes = <T extends Operation>(operation: T): T => {
	const cloneWithField = <V extends object>(
		value: V,
		key: PropertyKey,
		replacement: unknown,
	): V => {
		const descriptors = Object.getOwnPropertyDescriptors(value) as Record<
			PropertyKey,
			PropertyDescriptor
		>;
		descriptors[key] = {
			...descriptors[key],
			value: replacement,
		};
		const clone = Object.create(Object.getPrototypeOf(value), descriptors) as V;
		if (!Object.isExtensible(value)) {
			Object.preventExtensions(clone);
		}
		return clone;
	};
	if (operation instanceof PutWithKeyOperation) {
		return cloneWithField(
			operation,
			"data",
			new Uint8Array(operation.data),
		) as T;
	}
	if (operation instanceof PutOperation) {
		return cloneWithField(
			operation,
			"data",
			new Uint8Array(operation.data),
		) as T;
	}
	if (operation instanceof DeleteOperation) {
		const key = cloneWithField(
			operation.key,
			"key",
			operation.key instanceof indexerTypes.Uint8ArrayKey
				? new Uint8Array(operation.key.key)
				: operation.key.key,
		);
		return cloneWithField(operation, "key", key) as T;
	}
	if (operation instanceof DeleteByStringKeyOperation) {
		return cloneWithField(operation, "key", operation.key) as T;
	}
	return operation;
};

type LazyCallbackProperty = "operation" | "entry";
type LazyCallbackState = {
	operation?: Operation;
	entry?: Entry<any>;
	operationPending: boolean;
	entryPending: boolean;
};

const lazyCallbackStates = new WeakMap<object, LazyCallbackState>();

const isLazyCallbackProperty = (
	property: PropertyKey,
): property is LazyCallbackProperty =>
	property === "operation" || property === "entry";

const settleCallbackProperty = (
	target: object,
	state: LazyCallbackState,
	property: LazyCallbackProperty,
): void => {
	state[`${property}Pending`] = false;
	state[property] = undefined;
	if (!state.operationPending && !state.entryPending) {
		lazyCallbackStates.delete(target);
	}
};

const materializeCallbackProperty = (
	target: object,
	property: LazyCallbackProperty,
): void => {
	const state = lazyCallbackStates.get(target);
	if (!state || !state[`${property}Pending`]) {
		return;
	}
	const value =
		property === "operation"
			? detachOperationBytes(state.operation!)
			: detachEntryPayloadForCallback(state.entry!);
	Reflect.set(target, property, value, target);
	settleCallbackProperty(target, state, property);
};

const lazyCallbackHandler: ProxyHandler<any> = {
	get(target, property, receiver) {
		if (isLazyCallbackProperty(property)) {
			materializeCallbackProperty(target, property);
		}
		return Reflect.get(target, property, receiver);
	},
	set(target, property, value) {
		const updated = Reflect.set(target, property, value, target);
		if (updated && isLazyCallbackProperty(property)) {
			const state = lazyCallbackStates.get(target);
			if (state) {
				settleCallbackProperty(target, state, property);
			}
		}
		return updated;
	},
	deleteProperty(target, property) {
		const deleted = Reflect.deleteProperty(target, property);
		if (deleted && isLazyCallbackProperty(property)) {
			const state = lazyCallbackStates.get(target);
			if (state) {
				settleCallbackProperty(target, state, property);
			}
		}
		return deleted;
	},
	defineProperty(target, property, descriptor) {
		if (isLazyCallbackProperty(property)) {
			const replacesValue =
				"value" in descriptor || "get" in descriptor || "set" in descriptor;
			if (!replacesValue) {
				materializeCallbackProperty(target, property);
			}
			const defined = Reflect.defineProperty(target, property, descriptor);
			if (defined && replacesValue) {
				const state = lazyCallbackStates.get(target);
				if (state) {
					settleCallbackProperty(target, state, property);
				}
			}
			return defined;
		}
		return Reflect.defineProperty(target, property, descriptor);
	},
	getOwnPropertyDescriptor(target, property) {
		if (isLazyCallbackProperty(property)) {
			materializeCallbackProperty(target, property);
		}
		return Reflect.getOwnPropertyDescriptor(target, property);
	},
};

export const detachCanPerformCallbackProperties = <
	T extends { operation: Operation; entry: Entry<any> },
>(
	properties: T,
): T => {
	lazyCallbackStates.set(properties, {
		operation: properties.operation,
		entry: properties.entry,
		operationPending: true,
		entryPending: true,
	});
	Reflect.set(properties, "operation", undefined, properties);
	Reflect.set(properties, "entry", undefined, properties);
	return new Proxy(properties, lazyCallbackHandler);
};

/**
 * Return an Entry view whose public payload accessors cannot expose the
 * verified payload buffer to application callbacks. The byte copy is lazy:
 * callbacks that only inspect hashes, clocks, or signers allocate nothing.
 */
const detachedCallbackEntries = new WeakSet<object>();

export const detachEntryPayloadForCallback = <T>(entry: Entry<T>): Entry<T> => {
	if (detachedCallbackEntries.has(entry)) {
		return entry;
	}
	let detachedPayload: any;
	let detachedMaterialized: Entry<T> | undefined;
	let callbackEntry: Entry<T>;
	const boundMethods = new Map<
		PropertyKey,
		{ source: Function; bound: Function }
	>();
	const copyPayload = (payload: any) => {
		if (detachedPayload) {
			return detachedPayload;
		}
		const data = new Uint8Array(payload.data);
		const descriptors = Object.getOwnPropertyDescriptors(payload) as Record<
			PropertyKey,
			PropertyDescriptor
		>;
		descriptors.data = {
			...descriptors.data,
			value: data,
		};
		descriptors._value = {
			configurable: true,
			enumerable: false,
			writable: true,
			...descriptors._value,
			value: payload.isDecoded ? payload.encoding.decoder(data) : undefined,
		};
		detachedPayload = Object.create(
			Object.getPrototypeOf(payload),
			descriptors,
		);
		if (!Object.isExtensible(payload)) {
			Object.preventExtensions(detachedPayload);
		}
		return detachedPayload;
	};
	const getPayload = async () => {
		const entryWithPayloadMethod = entry as Entry<T> & {
			getPayload?: () => unknown;
		};
		if (typeof entryWithPayloadMethod.getPayload === "function") {
			return copyPayload(await entryWithPayloadMethod.getPayload.call(entry));
		}
		const materialized = entry.toMaterialized();
		if (materialized !== entry) {
			const materializedWithPayloadMethod = materialized as Entry<T> & {
				getPayload?: () => unknown;
			};
			if (typeof materializedWithPayloadMethod.getPayload === "function") {
				return copyPayload(
					await materializedWithPayloadMethod.getPayload.call(materialized),
				);
			}
			return copyPayload(materialized.payload);
		}
		return copyPayload(entry.payload);
	};
	const getPayloadValue = async () => {
		const payload = await getPayload();
		return payload.isDecoded ? payload.value : payload.getValue();
	};
	callbackEntry = new Proxy(entry, {
		get(target, property) {
			if (property === "payload") {
				return copyPayload(entry.payload);
			}
			if (
				property === "getPayload" &&
				typeof (entry as Entry<T> & { getPayload?: unknown }).getPayload ===
					"function"
			) {
				return getPayload;
			}
			if (property === "getPayloadValue") {
				return getPayloadValue;
			}
			if (property === "getStorageBytes") {
				return () => new Uint8Array(entry.getStorageBytes());
			}
			if (property === "valueOf") {
				return () => callbackEntry;
			}
			if (property === "toMaterialized") {
				return () => {
					const materialized = entry.toMaterialized();
					if (materialized === entry) {
						return callbackEntry;
					}
					return (detachedMaterialized ??=
						detachEntryPayloadForCallback(materialized));
				};
			}
			if (property === "toSignable") {
				return () => detachEntryPayloadForCallback(entry.toSignable());
			}
			if (property === "init") {
				return (properties: Parameters<Entry<T>["init"]>[0]) => {
					entry.init(properties);
					return callbackEntry;
				};
			}
			const value = Reflect.get(target, property, target);
			if (property === "constructor" || typeof value !== "function") {
				return value;
			}
			const existing = boundMethods.get(property);
			if (existing && existing.source === value) {
				return existing.bound;
			}
			const bound = value.bind(target);
			boundMethods.set(property, { source: value, bound });
			return bound;
		},
		set(target, property, value) {
			return Reflect.set(target, property, value, target);
		},
	});
	detachedCallbackEntries.add(callbackEntry);
	return callbackEntry;
};
