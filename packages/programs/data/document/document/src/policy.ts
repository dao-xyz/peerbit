import type { PublicSignKey } from "@peerbit/crypto";
import type { CanPerform, CanPerformOperations } from "./program.js";

export type NativeCanPerformPolicyDescriptor =
	| {
			readonly kind: "allowAll";
	  }
	| {
			readonly kind: "and";
			readonly policies: readonly NativeCanPerformPolicyDescriptor[];
	  }
	| {
			readonly kind: "or";
			readonly policies: readonly NativeCanPerformPolicyDescriptor[];
	  }
	| {
			readonly kind: "put";
			readonly policy: NativeCanPerformPolicyDescriptor;
	  }
	| {
			readonly kind: "delete";
			readonly policy: NativeCanPerformPolicyDescriptor;
	  }
	| {
			readonly kind: "signedByPublicKey";
			readonly publicKey: Uint8Array;
	  }
	| {
			readonly kind: "signedByField";
			readonly path: string | readonly string[];
	  }
	| {
			readonly kind: "deleteSignedByExistingField";
			readonly path: string | readonly string[];
	  }
	| {
			readonly kind: "sameSignersAsPrevious";
	  };

const NATIVE_CAN_PERFORM_POLICY = Symbol.for(
	"@peerbit/document/native-can-perform-policy",
);

const copyBytes = (bytes: Uint8Array): Uint8Array => new Uint8Array(bytes);

const bytesEqual = (left: Uint8Array, right: Uint8Array): boolean => {
	if (left.byteLength !== right.byteLength) {
		return false;
	}
	for (let i = 0; i < left.byteLength; i++) {
		if (left[i] !== right[i]) {
			return false;
		}
	}
	return true;
};

const asPath = (path: string | readonly string[]): readonly string[] =>
	typeof path === "string" ? [path] : path;

type FieldValueAccessor = (value: unknown) => unknown;

const createFieldValueAccessor = (
	path: string | readonly string[],
): FieldValueAccessor => {
	const segments = asPath(path);
	if (segments.length === 1) {
		const segment = segments[0]!;
		return (value: unknown) =>
			(value as Record<string, unknown> | undefined)?.[segment];
	}
	return (value: unknown) => {
		let current = value as Record<string, unknown> | undefined;
		for (const segment of segments) {
			if (current == null) {
				return undefined;
			}
			current = current[segment] as Record<string, unknown> | undefined;
		}
		return current;
	};
};

const keyRawBytes = (publicKey: PublicSignKey): Uint8Array | undefined =>
	(publicKey as PublicSignKey & { publicKey?: Uint8Array }).publicKey;

const valueMatchesPublicKeyBytes = (
	value: unknown,
	publicKey: PublicSignKey,
	publicKeyBytes: Uint8Array,
	rawPublicKeyBytes?: Uint8Array,
): boolean => {
	if (!value) {
		return false;
	}
	if (value instanceof Uint8Array) {
		return (
			bytesEqual(value, publicKeyBytes) ||
			(rawPublicKeyBytes ? bytesEqual(value, rawPublicKeyBytes) : false)
		);
	}
	const maybePublicKey = value as Partial<PublicSignKey> & {
		publicKey?: Uint8Array;
	};
	if (typeof maybePublicKey.equals === "function") {
		return maybePublicKey.equals(publicKey);
	}
	if (maybePublicKey.bytes instanceof Uint8Array) {
		return bytesEqual(maybePublicKey.bytes, publicKeyBytes);
	}
	if (maybePublicKey.publicKey instanceof Uint8Array) {
		return (
			!!rawPublicKeyBytes &&
			bytesEqual(maybePublicKey.publicKey, rawPublicKeyBytes)
		);
	}
	return false;
};

const valueMatchesPublicKey = (value: unknown, publicKey: PublicSignKey): boolean =>
	valueMatchesPublicKeyBytes(
		value,
		publicKey,
		publicKey.bytes,
		keyRawBytes(publicKey),
	);

const createPublicKeyValueMatcher = (
	publicKey: PublicSignKey,
): ((value: unknown) => boolean) => {
	const publicKeyBytes = publicKey.bytes;
	const rawPublicKeyBytes = keyRawBytes(publicKey);
	return (value) =>
		valueMatchesPublicKeyBytes(
			value,
			publicKey,
			publicKeyBytes,
			rawPublicKeyBytes,
		);
};

const valueMatchesAnyPublicKey = (
	value: unknown,
	publicKeys: readonly PublicSignKey[],
): boolean => {
	if (!value) {
		return false;
	}
	for (const publicKey of publicKeys) {
		if (valueMatchesPublicKey(value, publicKey)) {
			return true;
		}
	}
	return false;
};

const allDescriptors = <T>(
	policies: readonly CanPerform<T>[],
): NativeCanPerformPolicyDescriptor[] | undefined => {
	const descriptors: NativeCanPerformPolicyDescriptor[] = [];
	for (const policy of policies) {
		const descriptor = getNativeCanPerformPolicyDescriptor(policy);
		if (!descriptor) {
			return;
		}
		descriptors.push(descriptor);
	}
	return descriptors;
};

type NativeCanPerformPolicyFunction<T> = CanPerform<T> & {
	readonly [NATIVE_CAN_PERFORM_POLICY]?: NativeCanPerformPolicyDescriptor;
};

const attachNativeCanPerformPolicy = <T>(
	fn: CanPerform<T>,
	descriptor: NativeCanPerformPolicyDescriptor,
): NativeCanPerformPolicyFunction<T> => {
	if ((fn as NativeCanPerformPolicyFunction<T>)[NATIVE_CAN_PERFORM_POLICY]) {
		return fn as NativeCanPerformPolicyFunction<T>;
	}
	Object.defineProperty(fn, NATIVE_CAN_PERFORM_POLICY, {
		value: Object.freeze(descriptor),
		enumerable: false,
		writable: false,
		configurable: false,
	});
	return fn as NativeCanPerformPolicyFunction<T>;
};

export const getNativeCanPerformPolicyDescriptor = <T>(
	canPerform: CanPerform<T> | undefined,
): NativeCanPerformPolicyDescriptor | undefined =>
	(canPerform as NativeCanPerformPolicyFunction<T> | undefined)?.[
		NATIVE_CAN_PERFORM_POLICY
	];

export type NativeFastPathCanPerformPolicyEvaluator = (
	document: unknown,
) => boolean;
export type NativeFastPathDeletePolicyEvaluator = (
	existingDocument: unknown,
) => boolean;

const allowFastPath = (): boolean => true;
const denyFastPath = (): boolean => false;

export const createNativeFastPathCanPerformPolicyEvaluator = (
	descriptor: NativeCanPerformPolicyDescriptor,
	localPublicKey: PublicSignKey | undefined,
): NativeFastPathCanPerformPolicyEvaluator => {
	switch (descriptor.kind) {
		case "allowAll":
			return allowFastPath;
		case "signedByPublicKey":
			return localPublicKey &&
				bytesEqual(descriptor.publicKey, localPublicKey.bytes)
				? allowFastPath
				: denyFastPath;
		case "put":
			return createNativeFastPathCanPerformPolicyEvaluator(
				descriptor.policy,
				localPublicKey,
			);
		case "delete":
			return denyFastPath;
		case "deleteSignedByExistingField":
		case "sameSignersAsPrevious":
			return denyFastPath;
		case "and":
			return descriptor.policies
				.map((policy) =>
					createNativeFastPathCanPerformPolicyEvaluator(
						policy,
						localPublicKey,
					),
				)
				.reduce<NativeFastPathCanPerformPolicyEvaluator>(
					(previous, next) => (document) =>
						previous(document) && next(document),
					allowFastPath,
				);
		case "or":
			return descriptor.policies
				.map((policy) =>
					createNativeFastPathCanPerformPolicyEvaluator(
						policy,
						localPublicKey,
					),
				)
				.reduce<NativeFastPathCanPerformPolicyEvaluator>(
					(previous, next) => (document) =>
						previous(document) || next(document),
					denyFastPath,
				);
		case "signedByField": {
			if (!localPublicKey) {
				return denyFastPath;
			}
			const getFieldValue = createFieldValueAccessor(descriptor.path);
			const matchesLocalPublicKey =
				createPublicKeyValueMatcher(localPublicKey);
			return (document) => matchesLocalPublicKey(getFieldValue(document));
		}
	}
};

export const createNativeFastPathDeletePolicyEvaluator = (
	descriptor: NativeCanPerformPolicyDescriptor,
	localPublicKey: PublicSignKey | undefined,
): NativeFastPathDeletePolicyEvaluator => {
	switch (descriptor.kind) {
		case "allowAll":
			return allowFastPath;
		case "signedByPublicKey":
			return localPublicKey &&
				bytesEqual(descriptor.publicKey, localPublicKey.bytes)
				? allowFastPath
				: denyFastPath;
		case "signedByField":
		case "sameSignersAsPrevious":
		case "put":
			return denyFastPath;
		case "delete":
			return createNativeFastPathDeletePolicyEvaluator(
				descriptor.policy,
				localPublicKey,
			);
		case "deleteSignedByExistingField": {
			if (!localPublicKey) {
				return denyFastPath;
			}
			const getFieldValue = createFieldValueAccessor(descriptor.path);
			const matchesLocalPublicKey =
				createPublicKeyValueMatcher(localPublicKey);
			return (document) => matchesLocalPublicKey(getFieldValue(document));
		}
		case "and":
			return descriptor.policies
				.map((policy) =>
					createNativeFastPathDeletePolicyEvaluator(policy, localPublicKey),
				)
				.reduce<NativeFastPathDeletePolicyEvaluator>(
					(previous, next) => (document) =>
						previous(document) && next(document),
					allowFastPath,
				);
		case "or":
			return descriptor.policies
				.map((policy) =>
					createNativeFastPathDeletePolicyEvaluator(policy, localPublicKey),
				)
				.reduce<NativeFastPathDeletePolicyEvaluator>(
					(previous, next) => (document) =>
						previous(document) || next(document),
					denyFastPath,
				);
	}
};

const nativeFastPathPutPolicyAllows = (
	descriptor: NativeCanPerformPolicyDescriptor,
	localPublicKey: PublicSignKey | undefined,
	document: unknown,
): boolean =>
	createNativeFastPathCanPerformPolicyEvaluator(
		descriptor,
		localPublicKey,
	)(document);

export const isNativeFastPathCanPerformPolicy = (
	descriptor: NativeCanPerformPolicyDescriptor | undefined,
	localPublicKey?: PublicSignKey,
	document?: unknown,
): boolean =>
	!!descriptor &&
	nativeFastPathPutPolicyAllows(descriptor, localPublicKey, document);

export const nativeCanPerformPolicyNeedsDeleteValue = (
	descriptor: NativeCanPerformPolicyDescriptor,
): boolean => {
	switch (descriptor.kind) {
		case "deleteSignedByExistingField":
			return true;
		case "and":
		case "or":
			return descriptor.policies.some(nativeCanPerformPolicyNeedsDeleteValue);
		case "put":
		case "delete":
			return nativeCanPerformPolicyNeedsDeleteValue(descriptor.policy);
		default:
			return false;
	}
};

export const nativeCanPerformPolicyNeedsPreviousEntries = (
	descriptor: NativeCanPerformPolicyDescriptor,
): boolean => {
	switch (descriptor.kind) {
		case "sameSignersAsPrevious":
			return true;
		case "and":
		case "or":
			return descriptor.policies.some(
				nativeCanPerformPolicyNeedsPreviousEntries,
			);
		case "put":
		case "delete":
			return nativeCanPerformPolicyNeedsPreviousEntries(descriptor.policy);
		default:
			return false;
	}
};

const getEntryPublicKeys = async <T>(
	properties: CanPerformOperations<T>,
): Promise<PublicSignKey[]> => {
	try {
		if (properties.entry.publicKeys.length > 0) {
			return properties.entry.publicKeys;
		}
	} catch {
		return properties.entry.getPublicKeys();
	}
	return properties.entry.getPublicKeys();
};

const entryPublicKeysInclude = async <T>(
	properties: CanPerformOperations<T>,
	publicKey: PublicSignKey,
): Promise<boolean> => {
	const publicKeys = await getEntryPublicKeys(properties);
	return publicKeys.some((key) => key.equals(publicKey));
};

const entryPublicKeysMatchValue = async <T>(
	properties: CanPerformOperations<T>,
	value: unknown,
): Promise<boolean> => {
	const publicKeys = await getEntryPublicKeys(properties);
	return valueMatchesAnyPublicKey(value, publicKeys);
};

const publicKeySetsEqual = (
	left: readonly PublicSignKey[],
	right: readonly PublicSignKey[],
): boolean =>
	left.length === right.length &&
	left.every((leftKey) => right.some((rightKey) => leftKey.equals(rightKey)));

const sameSignersAsPrevious = async <T>(
	properties: CanPerformOperations<T>,
): Promise<boolean> => {
	if (properties.type !== "put") {
		return false;
	}
	if (!properties.previousEntries?.length) {
		return true;
	}
	const currentPublicKeys = await getEntryPublicKeys(properties);
	for (const previousEntry of properties.previousEntries) {
		const previousPublicKeys = await getEntryPublicKeys({
			type: "put",
			value: properties.value,
			operation: properties.operation,
			entry: previousEntry as any,
		});
		if (!publicKeySetsEqual(currentPublicKeys, previousPublicKeys)) {
			return false;
		}
	}
	return true;
};

const andEvaluate = async <T>(
	policies: readonly CanPerform<T>[],
	properties: CanPerformOperations<T>,
): Promise<boolean> => {
	for (const policy of policies) {
		if (!(await policy(properties))) {
			return false;
		}
	}
	return true;
};

const orEvaluate = async <T>(
	policies: readonly CanPerform<T>[],
	properties: CanPerformOperations<T>,
): Promise<boolean> => {
	for (const policy of policies) {
		if (await policy(properties)) {
			return true;
		}
	}
	return false;
};

const attachIfDescribed = <T>(
	fn: CanPerform<T>,
	descriptor: NativeCanPerformPolicyDescriptor | undefined,
): CanPerform<T> => (descriptor ? attachNativeCanPerformPolicy(fn, descriptor) : fn);

export const policy = {
	allowAll: <T = unknown>(): CanPerform<T> =>
		attachNativeCanPerformPolicy<T>(() => true, { kind: "allowAll" }),
	signedByPublicKey: <T = unknown>(publicKey: PublicSignKey): CanPerform<T> =>
		attachNativeCanPerformPolicy<T>(
			(properties) => entryPublicKeysInclude(properties, publicKey),
			{
				kind: "signedByPublicKey",
				publicKey: copyBytes(publicKey.bytes),
			},
		),
	signedByField: <T = unknown>(
		path: string | readonly string[],
	): CanPerform<T> => {
		const getFieldValue = createFieldValueAccessor(path);
		return attachNativeCanPerformPolicy<T>(
			async (properties) => {
				if (properties.type !== "put") {
					return false;
				}
				const fieldValue = getFieldValue(properties.value);
				return entryPublicKeysMatchValue(properties, fieldValue);
			},
			{
				kind: "signedByField",
				path: typeof path === "string" ? path : Object.freeze([...path]),
			},
		);
	},
	deleteSignedByExistingField: <T = unknown>(
		path: string | readonly string[],
	): CanPerform<T> => {
		const getFieldValue = createFieldValueAccessor(path);
		return attachNativeCanPerformPolicy<T>(
			async (properties) => {
				if (properties.type !== "delete" || !properties.value) {
					return false;
				}
				const fieldValue = getFieldValue(properties.value);
				return entryPublicKeysMatchValue(properties, fieldValue);
			},
			{
				kind: "deleteSignedByExistingField",
				path: typeof path === "string" ? path : Object.freeze([...path]),
			},
		);
	},
	sameSignersAsPrevious: <T = unknown>(): CanPerform<T> =>
		attachNativeCanPerformPolicy<T>(sameSignersAsPrevious, {
			kind: "sameSignersAsPrevious",
		}),
	put: <T = unknown>(inner: CanPerform<T>): CanPerform<T> =>
		attachIfDescribed<T>(
			(properties) => properties.type === "put" && inner(properties),
			(() => {
				const descriptor = getNativeCanPerformPolicyDescriptor(inner);
				return descriptor ? { kind: "put", policy: descriptor } : undefined;
			})(),
		),
	delete: <T = unknown>(inner: CanPerform<T>): CanPerform<T> =>
		attachIfDescribed<T>(
			(properties) => properties.type === "delete" && inner(properties),
			(() => {
				const descriptor = getNativeCanPerformPolicyDescriptor(inner);
				return descriptor ? { kind: "delete", policy: descriptor } : undefined;
			})(),
		),
	and: <T = unknown>(...policies: readonly CanPerform<T>[]): CanPerform<T> =>
		attachIfDescribed<T>(
			(properties) => andEvaluate(policies, properties),
			(() => {
				const descriptors = allDescriptors(policies);
				return descriptors
					? { kind: "and", policies: Object.freeze(descriptors.slice()) }
					: undefined;
			})(),
		),
	or: <T = unknown>(...policies: readonly CanPerform<T>[]): CanPerform<T> =>
		attachIfDescribed<T>(
			(properties) => orEvaluate(policies, properties),
			(() => {
				const descriptors = allDescriptors(policies);
				return descriptors
					? { kind: "or", policies: Object.freeze(descriptors.slice()) }
					: undefined;
			})(),
		),
} as const;
