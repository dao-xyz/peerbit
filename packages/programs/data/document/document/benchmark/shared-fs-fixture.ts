import { field, variant, vec } from "@dao-xyz/borsh";
import { id } from "@peerbit/indexer-interface";
import { Program } from "@peerbit/program";
import { Documents, type SetupOptions } from "../src/program.js";

@variant("shared_fs_projection_input")
export class FsDocument {
	@field({ type: "string" })
	id: string;

	@field({ type: "string" })
	kind: string;

	@field({ type: Uint8Array })
	bytes: Uint8Array;

	@field({ type: vec("string") })
	chunkRefs: string[];

	@field({ type: vec("string") })
	causalRefs: string[];

	constructor(properties: {
		id: string;
		kind: string;
		bytes?: Uint8Array;
		chunkRefs?: string[];
		causalRefs?: string[];
	}) {
		this.id = properties.id;
		this.kind = properties.kind;
		this.bytes = properties.bytes ?? new Uint8Array();
		this.chunkRefs = properties.chunkRefs ?? [];
		this.causalRefs = properties.causalRefs ?? [];
	}
}

@variant("shared_fs_projection_index")
export class FsProjection {
	@id({ type: "string" })
	id: string;

	@field({ type: "string" })
	kind: string;

	// shared-fs uses repeated child-table probes for head selection and dedup.
	@field({ type: vec("string") })
	chunkRefs: string[];

	@field({ type: vec("string") })
	causalRefs: string[];

	@field({ type: "u32" })
	byteLength: number;

	constructor(document: FsDocument) {
		this.id = document.id;
		this.kind = document.kind;
		this.chunkRefs = document.chunkRefs;
		this.causalRefs = document.causalRefs;
		this.byteLength = document.bytes.byteLength;
	}
}

@variant("shared_fs_projection_store")
export class FsStore extends Program<
	Partial<SetupOptions<FsDocument, FsProjection>>
> {
	@field({ type: Uint8Array })
	id: Uint8Array;

	@field({ type: Documents })
	docs: Documents<FsDocument, FsProjection>;

	constructor(properties?: { id?: Uint8Array }) {
		super();
		this.id = properties?.id ?? crypto.getRandomValues(new Uint8Array(32));
		this.docs = new Documents<FsDocument, FsProjection>();
	}

	async open(
		options?: Partial<SetupOptions<FsDocument, FsProjection>>,
	): Promise<void> {
		await this.docs.open({
			...options,
			type: FsDocument,
			index: {
				...options?.index,
				idProperty: "id",
			},
		});
	}
}

export const projectFsDocument = async (
	document: FsDocument,
): Promise<FsProjection> => {
	await Promise.resolve();
	return new FsProjection(document);
};

export const createFsDocuments = (
	fileCount: number,
	chunkCount: number,
): FsDocument[] => {
	const documents: FsDocument[] = [];
	for (let index = 0; index < fileCount; index++) {
		documents.push(
			new FsDocument({
				id: `name:${index}`,
				kind: "name",
				causalRefs: index === 0 ? [] : [`name:${index - 1}`],
			}),
			new FsDocument({
				id: `version:${index}:0`,
				kind: "version",
				bytes: new Uint8Array(64).fill(index % 251),
				chunkRefs: [`chunk:${index % chunkCount}`],
				causalRefs: [`name:${index}`],
			}),
		);
	}
	for (let index = 0; index < chunkCount; index++) {
		documents.push(
			new FsDocument({
				id: `chunk:${index}`,
				kind: "chunk",
				bytes: new Uint8Array(1024).fill(index % 251),
			}),
		);
	}
	return documents;
};
