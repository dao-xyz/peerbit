import type { PublicSignKey } from "@peerbit/crypto";
import type { Context } from "@peerbit/document-interface";

export type DocumentTransformFacts = {
	entryPublicKeys?: readonly PublicSignKey[];
};

type NativeDocumentTransformSourceDescriptor =
	| {
			readonly kind: "field";
			readonly path: string | readonly string[];
	  }
	| {
			readonly kind: "context";
			readonly field: "created" | "modified" | "head" | "gid" | "size";
	  }
	| {
			readonly kind: "entryFirstSignerPublicKey";
	  };

export type NativeDocumentTransformDescriptor =
	| {
			readonly kind: "identity";
	  }
	| {
			readonly kind: "pick";
			readonly fields: readonly (string | readonly string[])[];
	  }
	| {
			readonly kind: "project";
			readonly fields: readonly {
				readonly target: string | readonly string[];
				readonly source: NativeDocumentTransformSourceDescriptor;
			}[];
	  };

const NATIVE_DOCUMENT_TRANSFORM = Symbol.for(
	"@peerbit/document/native-document-transform",
);

export type NativeDocumentTransformer<T, I> = ((
	obj: T,
	context: Context,
	facts?: DocumentTransformFacts,
) => I | Promise<I>) & {
	readonly [NATIVE_DOCUMENT_TRANSFORM]?: NativeDocumentTransformDescriptor;
};

type NativeDocumentTransformSource =
	| string
	| readonly string[]
	| NativeDocumentTransformSourceDescriptor;

const copyBytes = (bytes: Uint8Array): Uint8Array => new Uint8Array(bytes);

const freezePath = (
	path: string | readonly string[],
): string | readonly string[] =>
	typeof path === "string" ? path : Object.freeze([...path]);

const asPath = (path: string | readonly string[]): readonly string[] =>
	typeof path === "string" ? [path] : path;

const pathsEqual = (
	left: string | readonly string[],
	right: string | readonly string[],
): boolean => {
	const leftPath = asPath(left);
	const rightPath = asPath(right);
	return (
		leftPath.length === rightPath.length &&
		leftPath.every((segment, index) => segment === rightPath[index])
	);
};

const getFieldValue = (value: unknown, path: string | readonly string[]) => {
	let current = value as Record<string, unknown> | undefined;
	for (const segment of asPath(path)) {
		if (current == null) {
			return undefined;
		}
		current = current[segment] as Record<string, unknown> | undefined;
	}
	return current;
};

const setFieldValue = (
	value: Record<string, unknown>,
	path: string | readonly string[],
	fieldValue: unknown,
): void => {
	const segments = asPath(path);
	let current = value;
	for (let i = 0; i < segments.length - 1; i++) {
		const segment = segments[i]!;
		const next = current[segment];
		if (!next || typeof next !== "object") {
			const created: Record<string, unknown> = {};
			current[segment] = created;
			current = created;
		} else {
			current = next as Record<string, unknown>;
		}
	}
	current[segments[segments.length - 1]!] = fieldValue;
};

const sourceDescriptor = (
	source: NativeDocumentTransformSource,
): NativeDocumentTransformSourceDescriptor =>
	typeof source === "object" && !Array.isArray(source) && "kind" in source
		? source
		: { kind: "field", path: freezePath(source) };

const readSource = (
	document: unknown,
	context: Context,
	facts: DocumentTransformFacts | undefined,
	source: NativeDocumentTransformSourceDescriptor,
): unknown => {
	switch (source.kind) {
		case "field":
			return getFieldValue(document, source.path);
		case "context":
			return context[source.field];
		case "entryFirstSignerPublicKey": {
			const signer = facts?.entryPublicKeys?.[0];
			return signer ? copyBytes(signer.bytes) : undefined;
		}
	}
};

const attachNativeDocumentTransform = <T, I>(
	fn: NativeDocumentTransformer<T, I>,
	descriptor: NativeDocumentTransformDescriptor,
): NativeDocumentTransformer<T, I> => {
	Object.defineProperty(fn, NATIVE_DOCUMENT_TRANSFORM, {
		value: Object.freeze(descriptor),
		enumerable: false,
		writable: false,
		configurable: false,
	});
	return fn;
};

export const getNativeDocumentTransformDescriptor = <T, I>(
	transformer: ((obj: T, context: Context) => I | Promise<I>) | undefined,
): NativeDocumentTransformDescriptor | undefined =>
	(transformer as NativeDocumentTransformer<T, I> | undefined)?.[
		NATIVE_DOCUMENT_TRANSFORM
	];

export const canPrepareNativeDocumentTransformBeforeAppend = (
	descriptor: NativeDocumentTransformDescriptor | undefined,
): boolean => {
	if (!descriptor) {
		return false;
	}
	switch (descriptor.kind) {
		case "identity":
		case "pick":
			return true;
		case "project":
			return descriptor.fields.every(
				(field) => field.source.kind !== "context",
			);
	}
};

export const canPrepareNativeDocumentTransformWithAppendFacts = (
	descriptor: NativeDocumentTransformDescriptor | undefined,
): boolean => {
	if (!descriptor) {
		return false;
	}
	switch (descriptor.kind) {
		case "identity":
		case "pick":
			return true;
		case "project":
			return descriptor.fields.every(
				(field) =>
					field.source.kind !== "context" || field.source.field !== "head",
			);
	}
};

export const canUseNativeBackboneDocumentTransform = (
	descriptor: NativeDocumentTransformDescriptor | undefined,
): boolean => descriptor != null;

export const nativeDocumentTransformPreservesFieldPath = (
	descriptor: NativeDocumentTransformDescriptor | undefined,
	path: string | readonly string[],
): boolean => {
	if (!descriptor) {
		return false;
	}
	switch (descriptor.kind) {
		case "identity":
			return true;
		case "pick":
			return descriptor.fields.some((field) => pathsEqual(field, path));
		case "project":
			return descriptor.fields.some(
				(field) =>
					pathsEqual(field.target, path) &&
					field.source.kind === "field" &&
					pathsEqual(field.source.path, path),
			);
	}
};

export const transform = {
	identity: <T = unknown>(): NativeDocumentTransformer<T, T> =>
		attachNativeDocumentTransform((obj) => obj, { kind: "identity" }),

	pick: <
		T = Record<string, unknown>,
		I extends object = Partial<T>,
	>(
		fields: readonly (string | readonly string[])[],
	): NativeDocumentTransformer<T, I> => {
		const frozenFields = Object.freeze(fields.map(freezePath));
		return attachNativeDocumentTransform(
			(document) => {
				const out: Record<string, unknown> = {};
				for (const path of frozenFields) {
					setFieldValue(out, path, getFieldValue(document, path));
				}
				return out as I;
			},
			{ kind: "pick", fields: frozenFields },
		);
	},

	field: (
		path: string | readonly string[],
	): NativeDocumentTransformSourceDescriptor => ({
		kind: "field",
		path: freezePath(path),
	}),

	context: (
		field: "created" | "modified" | "head" | "gid" | "size",
	): NativeDocumentTransformSourceDescriptor => ({ kind: "context", field }),

	entryFirstSignerPublicKey:
		(): NativeDocumentTransformSourceDescriptor => ({
			kind: "entryFirstSignerPublicKey",
		}),

	project: <T = unknown, I extends object = Record<string, unknown>>(
		fields: Record<string, NativeDocumentTransformSource>,
	): NativeDocumentTransformer<T, I> => {
		const entries = Object.freeze(
			Object.entries(fields).map(([target, source]) =>
				Object.freeze({
					target,
					source: Object.freeze(sourceDescriptor(source)),
				}),
			),
		);
		return attachNativeDocumentTransform(
			(document, context, facts) => {
				const out: Record<string, unknown> = {};
				for (const entry of entries) {
					setFieldValue(
						out,
						entry.target,
						readSource(document, context, facts, entry.source),
					);
				}
				return out as I;
			},
			{ kind: "project", fields: entries },
		);
	},
} as const;
