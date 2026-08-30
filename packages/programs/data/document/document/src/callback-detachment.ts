import type { PublicSignKey, SignatureWithKey } from "@peerbit/crypto";
import * as indexerTypes from "@peerbit/indexer-interface";
import { Entry } from "@peerbit/log";
import {
	DeleteByStringKeyOperation,
	DeleteOperation,
	type Operation,
	PutOperation,
	PutWithKeyOperation,
} from "./operation.js";

type LazyPropertyFactories = Map<PropertyKey, () => unknown>;

const isPromiseLike = (value: unknown): value is PromiseLike<unknown> =>
	!!value &&
	(typeof value === "object" || typeof value === "function") &&
	typeof (value as { then?: unknown }).then === "function";

const mapMaybePromise = (
	value: unknown,
	map: (value: any) => unknown,
): unknown => (isPromiseLike(value) ? value.then(map) : map(value));

const copyOptionalBytes = (
	bytes: Uint8Array | undefined,
): Uint8Array | undefined =>
	bytes == null ? undefined : new Uint8Array(bytes);

const preserveArrayIntegrity = <T>(source: readonly T[], copy: T[]): T[] => {
	if (Object.isFrozen(source)) {
		Object.freeze(copy);
	} else if (Object.isSealed(source)) {
		Object.seal(copy);
	} else if (!Object.isExtensible(source)) {
		Object.preventExtensions(copy);
	}
	return copy;
};

const detachArray = <T>(
	values: readonly T[],
	detach: (value: T) => T = (value) => value,
): T[] => preserveArrayIntegrity(values, values.map(detach));

const getInheritedPropertyDescriptor = (
	value: object,
	key: PropertyKey,
): PropertyDescriptor | undefined => {
	let prototype = Object.getPrototypeOf(value) as object | null;
	while (prototype) {
		const descriptor = Reflect.getOwnPropertyDescriptor(prototype, key);
		if (descriptor) {
			return descriptor;
		}
		prototype = Object.getPrototypeOf(prototype) as object | null;
	}
	return undefined;
};

/**
 * Clone an object without resolving selected nested values. For normal
 * objects the selected fields become callback-local lazy accessors. The clone
 * receives the source's frozen/sealed/non-extensible state only after those
 * accessors are installed, so a no-touch callback never resolves lazy source
 * fields.
 */
const cloneWithLazyProperties = <T extends object>(
	value: T,
	factories: LazyPropertyFactories,
): T => {
	const descriptors = Object.getOwnPropertyDescriptors(value) as Record<
		PropertyKey,
		PropertyDescriptor
	>;
	const extensible = Object.isExtensible(value);
	for (const [key, factory] of factories) {
		const ownDescriptor = descriptors[key];
		const descriptor =
			ownDescriptor ?? getInheritedPropertyDescriptor(value, key);
		let resolved = false;
		let detachedValue: unknown;
		const writable = descriptor
			? (ownDescriptor != null || extensible) &&
				("value" in descriptor
					? descriptor.writable !== false
					: descriptor.set != null)
			: extensible;
		descriptors[key] = {
			configurable: descriptor?.configurable ?? true,
			enumerable: descriptor?.enumerable ?? true,
			get() {
				if (!resolved) {
					detachedValue = factory();
					resolved = true;
				}
				return detachedValue;
			},
			...(writable
				? {
						set(nextValue: unknown) {
							detachedValue = nextValue;
							resolved = true;
						},
					}
				: {}),
		};
	}
	const clone = Object.create(Object.getPrototypeOf(value), descriptors) as T;
	if (Object.isFrozen(value)) {
		Object.freeze(clone);
	} else if (Object.isSealed(value)) {
		Object.seal(clone);
	} else if (!Object.isExtensible(value)) {
		Object.preventExtensions(clone);
	}
	return clone;
};

const addOwnByteFactories = (
	value: object,
	factories: LazyPropertyFactories,
): void => {
	const descriptors = Object.getOwnPropertyDescriptors(value) as Record<
		PropertyKey,
		PropertyDescriptor
	>;
	for (const key of Reflect.ownKeys(descriptors)) {
		const descriptor = descriptors[key];
		if (descriptor && "value" in descriptor) {
			const bytes = descriptor.value;
			if (bytes instanceof Uint8Array) {
				factories.set(key, () => new Uint8Array(bytes));
			}
		}
	}
};

const addOwnArrayFactories = (
	value: object,
	factories: LazyPropertyFactories,
): void => {
	const descriptors = Object.getOwnPropertyDescriptors(value) as Record<
		PropertyKey,
		PropertyDescriptor
	>;
	for (const key of Reflect.ownKeys(descriptors)) {
		const descriptor = descriptors[key];
		if (
			descriptor &&
			"value" in descriptor &&
			Array.isArray(descriptor.value)
		) {
			const array = descriptor.value as unknown[];
			factories.set(key, () =>
				detachArray(array, (item) =>
					item instanceof Uint8Array ? new Uint8Array(item) : item,
				),
			);
		}
	}
};

const detachedCallbackEntryLikes = new WeakSet<object>();

const createEntryDetachmentContext = () => {
	let metadata: WeakMap<object, object> | undefined;
	let clocks: WeakMap<object, object> | undefined;
	let timestamps: WeakMap<object, object> | undefined;
	let signatures: WeakMap<object, object> | undefined;
	let publicKeys: WeakMap<object, object> | undefined;
	let entryLikes: WeakMap<object, object> | undefined;
	let arrays: WeakMap<object, unknown[]> | undefined;

	const detachValues = <T>(
		values: readonly T[],
		detach: (value: T) => T = (value) => value,
	): T[] => {
		const cached = arrays?.get(values);
		if (cached) {
			return cached as T[];
		}
		const clone = detachArray(values, detach);
		(arrays ??= new WeakMap()).set(values, clone);
		return clone;
	};

	const detachTimestamp = <T>(value: T): T => {
		if (!value || typeof value !== "object") {
			return value;
		}
		const cached = timestamps?.get(value);
		if (cached) {
			return cached as T;
		}
		const factories: LazyPropertyFactories = new Map();
		addOwnByteFactories(value, factories);
		const clone = cloneWithLazyProperties(value, factories);
		(timestamps ??= new WeakMap()).set(value, clone);
		return clone;
	};

	const detachClock = <T>(value: T): T => {
		if (!value || typeof value !== "object") {
			return value;
		}
		const cached = clocks?.get(value);
		if (cached) {
			return cached as T;
		}
		const factories: LazyPropertyFactories = new Map();
		addOwnByteFactories(value, factories);
		if (Reflect.has(value, "id") && !factories.has("id")) {
			factories.set("id", () =>
				copyOptionalBytes(
					Reflect.get(value, "id", value) as Uint8Array | undefined,
				),
			);
		}
		if (Reflect.has(value, "timestamp")) {
			factories.set("timestamp", () =>
				detachTimestamp(Reflect.get(value, "timestamp", value)),
			);
		}
		const clone = cloneWithLazyProperties(value, factories);
		(clocks ??= new WeakMap()).set(value, clone);
		return clone;
	};

	const detachMeta = <T>(value: T): T => {
		if (!value || typeof value !== "object") {
			return value;
		}
		const cached = metadata?.get(value);
		if (cached) {
			return cached as T;
		}
		const factories: LazyPropertyFactories = new Map();
		addOwnByteFactories(value, factories);
		if (Reflect.has(value, "data") && !factories.has("data")) {
			factories.set("data", () =>
				copyOptionalBytes(
					Reflect.get(value, "data", value) as Uint8Array | undefined,
				),
			);
		}
		if (Reflect.has(value, "clock")) {
			factories.set("clock", () =>
				detachClock(Reflect.get(value, "clock", value)),
			);
		}
		if (Reflect.has(value, "next")) {
			factories.set("next", () => {
				const next = Reflect.get(value, "next", value);
				return Array.isArray(next) ? detachValues(next) : next;
			});
		}
		const clone = cloneWithLazyProperties(value, factories);
		(metadata ??= new WeakMap()).set(value, clone);
		return clone;
	};

	const detachPublicKey = <T>(value: T): T => {
		if (!value || typeof value !== "object") {
			return value;
		}
		const cached = publicKeys?.get(value);
		if (cached) {
			return cached as T;
		}
		const factories: LazyPropertyFactories = new Map();
		addOwnByteFactories(value, factories);
		const clone = cloneWithLazyProperties(value, factories);
		(publicKeys ??= new WeakMap()).set(value, clone);
		return clone;
	};

	const detachSignature = <T>(value: T): T => {
		if (!value || typeof value !== "object") {
			return value;
		}
		const cached = signatures?.get(value);
		if (cached) {
			return cached as T;
		}
		const factories: LazyPropertyFactories = new Map();
		addOwnByteFactories(value, factories);
		if (Reflect.has(value, "publicKey")) {
			factories.set("publicKey", () =>
				detachPublicKey(Reflect.get(value, "publicKey", value)),
			);
		}
		const clone = cloneWithLazyProperties(value, factories);
		(signatures ??= new WeakMap()).set(value, clone);
		return clone;
	};

	const detachSignatures = (
		values: readonly SignatureWithKey[],
	): SignatureWithKey[] => detachValues(values, detachSignature);

	const detachPublicKeys = (
		values: readonly PublicSignKey[],
	): PublicSignKey[] => detachValues(values, detachPublicKey);

	const detachEntryLike = <T extends object>(value: T): T => {
		if (detachedCallbackEntryLikes.has(value)) {
			return value;
		}
		const cached = entryLikes?.get(value);
		if (cached) {
			return cached as T;
		}
		const factories: LazyPropertyFactories = new Map();
		addOwnByteFactories(value, factories);
		addOwnArrayFactories(value, factories);
		if (Reflect.has(value, "meta")) {
			factories.set("meta", () =>
				detachMeta(Reflect.get(value, "meta", value)),
			);
		}
		for (const methodName of ["getMetaBytes", "getHashDigestBytes"] as const) {
			const method = Reflect.get(value, methodName, value);
			if (typeof method === "function") {
				factories.set(
					methodName,
					() =>
						(...args: unknown[]) =>
							mapMaybePromise(
								Reflect.apply(method, value, args),
								copyOptionalBytes,
							),
				);
			}
		}
		const clone = cloneWithLazyProperties(value, factories);
		(entryLikes ??= new WeakMap()).set(value, clone);
		detachedCallbackEntryLikes.add(clone);
		return clone;
	};

	return {
		detachArray: detachValues,
		detachClock,
		detachEntryLike,
		detachMeta,
		detachPublicKeys,
		detachSignatures,
	};
};

type EntryDetachmentContext = ReturnType<typeof createEntryDetachmentContext>;
type EntryDetachmentContextProvider = () => EntryDetachmentContext;

const detachPreparedAppendJoinFactsForCallback = (
	facts: object,
	canonicalEntry: Entry<any>,
	callbackEntry: Entry<any>,
	getContext: EntryDetachmentContextProvider,
): object => {
	const factories: LazyPropertyFactories = new Map();
	addOwnByteFactories(facts, factories);
	if (Reflect.has(facts, "bytes")) {
		factories.set("bytes", () =>
			copyOptionalBytes(
				Reflect.get(facts, "bytes", facts) as Uint8Array | undefined,
			),
		);
	}
	if (Reflect.has(facts, "meta")) {
		factories.set("meta", () =>
			getContext().detachMeta(Reflect.get(facts, "meta", facts)),
		);
	}
	const getShallowEntry = Reflect.get(facts, "getShallowEntry", facts);
	if (typeof getShallowEntry === "function") {
		factories.set("getShallowEntry", () => (...args: unknown[]) => {
			const shallow = Reflect.apply(getShallowEntry, facts, args) as unknown;
			return shallow && typeof shallow === "object"
				? getContext().detachEntryLike(shallow)
				: shallow;
		});
	}
	const materializeEntry = Reflect.get(facts, "materializeEntry", facts);
	if (typeof materializeEntry === "function") {
		factories.set(
			"materializeEntry",
			() =>
				(...args: unknown[]) =>
					mapMaybePromise(
						Reflect.apply(materializeEntry, facts, args),
						(materialized) =>
							materialized === canonicalEntry
								? callbackEntry
								: materialized instanceof Entry
									? detachEntryPayloadForCallbackWithContext(
											materialized,
											getContext,
										)
									: materialized && typeof materialized === "object"
										? getContext().detachEntryLike(materialized)
										: materialized,
					),
		);
	}
	return cloneWithLazyProperties(facts, factories);
};

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
 * Return an Entry view whose public accessors cannot expose verified payload,
 * metadata, signature, public-key, or hash-digest buffers to application
 * callbacks. Nested copies remain lazy, including metadata byte fields.
 */
const detachedCallbackEntries = new WeakSet<object>();

const detachEntryPayloadForCallbackWithContext = <T>(
	entry: Entry<T>,
	getContext: EntryDetachmentContextProvider,
): Entry<T> => {
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
			if (property === "meta") {
				return getContext().detachMeta(Reflect.get(target, property, target));
			}
			if (property === "next") {
				const next = Reflect.get(target, property, target);
				return Array.isArray(next) ? getContext().detachArray(next) : next;
			}
			if (property === "signatures") {
				return getContext().detachSignatures(
					Reflect.get(target, property, target),
				);
			}
			if (property === "publicKeys") {
				return getContext().detachPublicKeys(
					Reflect.get(target, property, target),
				);
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
			if (property === "getMeta") {
				const getMeta = Reflect.get(target, property, target);
				if (typeof getMeta === "function") {
					return (...args: unknown[]) =>
						mapMaybePromise(
							Reflect.apply(getMeta, target, args),
							getContext().detachMeta,
						);
				}
			}
			if (property === "getClock") {
				const getClock = Reflect.get(target, property, target);
				if (typeof getClock === "function") {
					return (...args: unknown[]) =>
						mapMaybePromise(
							Reflect.apply(getClock, target, args),
							getContext().detachClock,
						);
				}
			}
			if (property === "getNext") {
				const getNext = Reflect.get(target, property, target);
				if (typeof getNext === "function") {
					return (...args: unknown[]) =>
						mapMaybePromise(Reflect.apply(getNext, target, args), (next) =>
							Array.isArray(next) ? getContext().detachArray(next) : next,
						);
				}
			}
			if (property === "getSignatures") {
				const getSignatures = Reflect.get(target, property, target);
				if (typeof getSignatures === "function") {
					return (...args: unknown[]) =>
						mapMaybePromise(
							Reflect.apply(getSignatures, target, args),
							getContext().detachSignatures,
						);
				}
			}
			if (property === "getPublicKeys") {
				const getPublicKeys = Reflect.get(target, property, target);
				if (typeof getPublicKeys === "function") {
					return (...args: unknown[]) =>
						mapMaybePromise(
							Reflect.apply(getPublicKeys, target, args),
							getContext().detachPublicKeys,
						);
				}
			}
			if (property === "getMetaBytes" || property === "getHashDigestBytes") {
				const getBytes = Reflect.get(target, property, target);
				if (typeof getBytes === "function") {
					return (...args: unknown[]) =>
						mapMaybePromise(
							Reflect.apply(getBytes, target, args),
							copyOptionalBytes,
						);
				}
			}
			if (property === "__peerbitNext") {
				const next = Reflect.get(target, property, target);
				return Array.isArray(next) ? getContext().detachArray(next) : next;
			}
			if (property === "getStorageBytes") {
				return () => new Uint8Array(entry.getStorageBytes());
			}
			if (property === "toShallow") {
				const toShallow = Reflect.get(target, property, target);
				if (typeof toShallow === "function") {
					return (...args: unknown[]) => {
						const shallow = Reflect.apply(toShallow, target, args) as unknown;
						return shallow && typeof shallow === "object"
							? getContext().detachEntryLike(shallow)
							: shallow;
					};
				}
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
						detachEntryPayloadForCallbackWithContext(materialized, getContext));
				};
			}
			if (property === "toSignable") {
				return () =>
					detachEntryPayloadForCallbackWithContext(
						entry.toSignable(),
						getContext,
					);
			}
			if (property === "toPreparedAppendJoinFacts") {
				const toPreparedAppendJoinFacts = Reflect.get(target, property, target);
				if (typeof toPreparedAppendJoinFacts === "function") {
					return (...args: unknown[]) => {
						const facts = Reflect.apply(
							toPreparedAppendJoinFacts,
							target,
							args,
						) as unknown;
						return facts && typeof facts === "object"
							? detachPreparedAppendJoinFactsForCallback(
									facts,
									target,
									callbackEntry,
									getContext,
								)
							: facts;
					};
				}
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

export const detachEntryPayloadForCallback = <T>(entry: Entry<T>): Entry<T> => {
	let context: EntryDetachmentContext | undefined;
	return detachEntryPayloadForCallbackWithContext(
		entry,
		() => (context ??= createEntryDetachmentContext()),
	);
};

/** Detach full, shallow, and replicated entries at document callback edges. */
export const detachEntryForCallback = <T>(entry: T): T => {
	if (!entry || typeof entry !== "object") {
		return entry;
	}
	return (
		entry instanceof Entry
			? detachEntryPayloadForCallback(entry)
			: createEntryDetachmentContext().detachEntryLike(entry)
	) as T;
};

/** Give arbitrary document transforms signer objects they cannot retain/mutate. */
export const detachPublicKeysForCallback = (
	keys: readonly PublicSignKey[],
): PublicSignKey[] => createEntryDetachmentContext().detachPublicKeys(keys);
