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

const nativeFastPathPutPolicyAllows = (
	descriptor: NativeCanPerformPolicyDescriptor,
	localPublicKey: Uint8Array | undefined,
): boolean => {
	switch (descriptor.kind) {
		case "allowAll":
			return true;
		case "signedByPublicKey":
			return !!localPublicKey && bytesEqual(descriptor.publicKey, localPublicKey);
		case "put":
			return nativeFastPathPutPolicyAllows(descriptor.policy, localPublicKey);
		case "delete":
			return false;
		case "and":
			return descriptor.policies.every((policy) =>
				nativeFastPathPutPolicyAllows(policy, localPublicKey),
			);
		case "or":
			return descriptor.policies.some((policy) =>
				nativeFastPathPutPolicyAllows(policy, localPublicKey),
			);
		case "signedByField":
			return false;
	}
};

export const isNativeFastPathCanPerformPolicy = (
	descriptor: NativeCanPerformPolicyDescriptor | undefined,
	localPublicKey?: Uint8Array,
): boolean =>
	!!descriptor && nativeFastPathPutPolicyAllows(descriptor, localPublicKey);

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
			async ({ entry }) => {
				const publicKeys = await entry.getPublicKeys();
				return publicKeys.some((key) => key.equals(publicKey));
			},
			{
				kind: "signedByPublicKey",
				publicKey: copyBytes(publicKey.bytes),
			},
		),
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
