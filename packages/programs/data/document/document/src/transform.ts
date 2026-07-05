import type { PublicSignKey } from "@peerbit/crypto";
import type { Context } from "@peerbit/document-interface";

export type DocumentTransformFacts = {
	entryPublicKeys?: readonly PublicSignKey[];
};

export type DocumentTransformer<T, I> = (
	obj: T,
	context: Context,
	facts?: DocumentTransformFacts,
) => I | Promise<I>;

type DocumentTransformSourceDescriptor =
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

export type DocumentTransformDescriptor =
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
				readonly source: DocumentTransformSourceDescriptor;
			}[];
	  };

const NATIVE_DOCUMENT_TRANSFORM = Symbol.for(
	"@peerbit/document/native-document-transform",
);

type DescribedDocumentTransformer<T, I> = DocumentTransformer<T, I> & {
	readonly [NATIVE_DOCUMENT_TRANSFORM]?: DocumentTransformDescriptor;
};

type DocumentTransformSource =
	| string
	| readonly string[]
	| DocumentTransformSourceDescriptor;

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
	source: DocumentTransformSource,
): DocumentTransformSourceDescriptor =>
	typeof source === "object" && !Array.isArray(source) && "kind" in source
		? source
		: { kind: "field", path: freezePath(source) };

const readSource = (
	document: unknown,
	context: Context,
	facts: DocumentTransformFacts | undefined,
	source: DocumentTransformSourceDescriptor,
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

const attachDocumentTransformDescriptor = <T, I>(
	fn: DocumentTransformer<T, I>,
	descriptor: DocumentTransformDescriptor,
): DescribedDocumentTransformer<T, I> => {
	Object.defineProperty(fn, NATIVE_DOCUMENT_TRANSFORM, {
		value: Object.freeze(descriptor),
		enumerable: false,
		writable: false,
		configurable: false,
	});
	return fn;
};

export const getDocumentTransformDescriptor = <T, I>(
	transformer: DocumentTransformer<T, I> | undefined,
): DocumentTransformDescriptor | undefined =>
	(transformer as DescribedDocumentTransformer<T, I> | undefined)?.[
		NATIVE_DOCUMENT_TRANSFORM
	];

export const canPrepareDocumentTransformBeforeAppend = (
	descriptor: DocumentTransformDescriptor | undefined,
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

export const canPrepareDocumentTransformWithAppendFacts = (
	descriptor: DocumentTransformDescriptor | undefined,
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

export const documentTransformPreservesFieldPath = (
	descriptor: DocumentTransformDescriptor | undefined,
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
	identity: <T = unknown>(): DocumentTransformer<T, T> =>
		attachDocumentTransformDescriptor((obj) => obj, { kind: "identity" }),

	pick: <
		T = Record<string, unknown>,
		I extends object = Partial<T>,
	>(
		fields: readonly (string | readonly string[])[],
	): DocumentTransformer<T, I> => {
		const frozenFields = Object.freeze(fields.map(freezePath));
		return attachDocumentTransformDescriptor(
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
	): DocumentTransformSourceDescriptor => ({
		kind: "field",
		path: freezePath(path),
	}),

	context: (
		field: "created" | "modified" | "head" | "gid" | "size",
	): DocumentTransformSourceDescriptor => ({ kind: "context", field }),

	entryFirstSignerPublicKey:
		(): DocumentTransformSourceDescriptor => ({
			kind: "entryFirstSignerPublicKey",
		}),

	project: <T = unknown, I extends object = Record<string, unknown>>(
		fields: Record<string, DocumentTransformSource>,
	): DocumentTransformer<T, I> => {
		const entries = Object.freeze(
			Object.entries(fields).map(([target, source]) =>
				Object.freeze({
					target,
					source: Object.freeze(sourceDescriptor(source)),
				}),
			),
		);
		return attachDocumentTransformDescriptor(
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
