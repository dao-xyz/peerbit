import type { CanPerform } from "./program.js";

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

export const isNativeFastPathCanPerformPolicy = (
	descriptor: NativeCanPerformPolicyDescriptor | undefined,
): boolean => descriptor?.kind === "allowAll";

export const policy = {
	allowAll: <T = unknown>(): CanPerform<T> =>
		attachNativeCanPerformPolicy<T>(() => true, { kind: "allowAll" }),
} as const;
