type DocumentCommitContextInput = {
	existingCreated?: bigint | number | string | null;
	modified: bigint | number | string;
	head: string;
	gid: string;
	size: number;
};

type DocumentCommitContextPlan = {
	created: bigint;
	modified: bigint;
	head: string;
	gid: string;
	size: number;
	contextBytes: Uint8Array;
};

export type SimpleDocumentProjectionPlan = {
	documentVariantType?: "u8" | "string";
	documentVariantValue?: string;
	documentFieldNames: string[];
	documentFieldTypes: string[];
	outputVariantType?: "u8" | "string";
	outputVariantValue?: string;
	outputFieldTypes: string[];
	sourceKinds: string[];
	sourceValues: string[];
};

export type SimpleDocumentFieldExtractionPlan = {
	documentVariantType?: "u8" | "string";
	documentVariantValue?: string;
	documentFieldNames: string[];
	documentFieldTypes: string[];
	fieldName: string;
};

export type SimpleDocumentProjectionContext = {
	created: bigint | number | string;
	modified: bigint | number | string;
	head?: string;
	gid: string;
	size: number;
	signer?: Uint8Array;
};

type DocumentRustModule = {
	initializeDocumentRust: () => Promise<void>;
	tryPlanDocumentContext: (
		input: DocumentCommitContextInput,
	) => DocumentCommitContextPlan | undefined;
	tryPlanDocumentContextBatch: (
		inputs: DocumentCommitContextInput[],
	) => DocumentCommitContextPlan[] | undefined;
	planDocumentContext: (
		input: DocumentCommitContextInput,
	) => Promise<DocumentCommitContextPlan>;
	planDocumentContextBatch: (
		inputs: DocumentCommitContextInput[],
	) => Promise<DocumentCommitContextPlan[]>;
	tryProjectDocumentIndexSimple: (
		encodedDocument: Uint8Array,
		plan: SimpleDocumentProjectionPlan,
		context: SimpleDocumentProjectionContext,
	) => Uint8Array | undefined;
	extractDocumentFieldSimple: (
		encodedDocument: Uint8Array,
		plan: SimpleDocumentFieldExtractionPlan,
	) => Promise<string | number | bigint | Uint8Array | undefined>;
};

let documentRustPromise: Promise<DocumentRustModule> | undefined;
let documentRustModule: DocumentRustModule | undefined;

const loadDocumentRust = async (): Promise<DocumentRustModule> => {
	if (!documentRustPromise) {
		documentRustPromise = import("@peerbit/document-rust").then((module) => {
			documentRustModule = module as DocumentRustModule;
			return documentRustModule;
		});
	}
	return documentRustPromise;
};

export const initializeDocumentRust = async (): Promise<void> => {
	const module = await loadDocumentRust();
	await module.initializeDocumentRust();
};

export const tryPlanDocumentContext = (
	input: DocumentCommitContextInput,
): DocumentCommitContextPlan | undefined =>
	documentRustModule?.tryPlanDocumentContext(input);

export const tryPlanDocumentContextBatch = (
	inputs: DocumentCommitContextInput[],
): DocumentCommitContextPlan[] | undefined =>
	documentRustModule?.tryPlanDocumentContextBatch(inputs);

export const planDocumentContext = async (
	input: DocumentCommitContextInput,
): Promise<DocumentCommitContextPlan> =>
	(await loadDocumentRust()).planDocumentContext(input);

export const planDocumentContextBatch = async (
	inputs: DocumentCommitContextInput[],
): Promise<DocumentCommitContextPlan[]> =>
	(await loadDocumentRust()).planDocumentContextBatch(inputs);

export const tryProjectDocumentIndexSimple = (
	encodedDocument: Uint8Array,
	plan: SimpleDocumentProjectionPlan,
	context: SimpleDocumentProjectionContext,
): Uint8Array | undefined =>
	documentRustModule?.tryProjectDocumentIndexSimple(
		encodedDocument,
		plan,
		context,
	);

export const extractDocumentFieldSimple = async (
	encodedDocument: Uint8Array,
	plan: SimpleDocumentFieldExtractionPlan,
): Promise<string | number | bigint | Uint8Array | undefined> =>
	(await loadDocumentRust()).extractDocumentFieldSimple(encodedDocument, plan);
