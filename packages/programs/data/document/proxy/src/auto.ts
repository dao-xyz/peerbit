import { type AbstractType, getSchema } from "@dao-xyz/borsh";
import type { CanonicalOpenAdapter } from "@peerbit/canonical-client";
import { Documents } from "@peerbit/document";
import { openDocuments } from "./client.js";

const toHex = (bytes: Uint8Array): string => {
	let out = "";
	for (const b of bytes) out += b.toString(16).padStart(2, "0");
	return out;
};

const defaultTypeName = (type: AbstractType<any>): string => {
	const schema = getSchema(type);
	const variant = schema?.variant;
	if (!variant) {
		throw new Error("Document type is missing @variant() metadata");
	}
	return String(variant);
};

type DocumentsProxy = Awaited<ReturnType<typeof openDocuments>>;

const isDocumentsProgram = (program: any): program is Documents<any> => {
	if (!program || typeof program !== "object") return false;
	try {
		return getSchema(program.constructor)?.variant === "documents";
	} catch {
		return false;
	}
};

export const createDocumentAdapter = (options?: {
	getTypeName?: (type: AbstractType<any>) => string;
}): CanonicalOpenAdapter<Documents<any>, DocumentsProxy> => {
	const resolveTypeName = options?.getTypeName ?? defaultTypeName;

	return {
		name: "@peerbit/document",
		canOpen: isDocumentsProgram,
		getKey: (program, openOptions) => {
			const type = openOptions?.args?.type as AbstractType<any> | undefined;
			if (!type) return;
			const id = program.log?.log?.id;
			if (!id) return;
			return `${resolveTypeName(type)}:${toHex(id)}`;
		},
		open: async ({ program, options, client }) => {
			const type = options?.args?.type as AbstractType<any> | undefined;
			if (!type) {
				throw new Error(
					"Canonical Documents open requires options.args.type to be set",
				);
			}
			const id = program.log?.log?.id;
			if (!id) {
				throw new Error("Canonical Documents open requires a log id");
			}
			const address = (await program.calculateAddress()).address;
			const indexType = (options?.args as any)?.index?.type as
				| AbstractType<any>
				| undefined;
			const proxy = await openDocuments({
				client,
				id,
				typeName: resolveTypeName(type),
				type,
				indexType,
			});
			return { proxy, address };
		},
	};
};

export const documentAdapter = createDocumentAdapter();
