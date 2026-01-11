export * from "@peerbit/indexer-interface";
export * from "@peerbit/document-interface";
export * from "./program.js";
export type {
	CanRead,
	CanSearch,
	DocumentIndex,
	WithContext,
	WithIndexedContext,
	WithIndexed,
	OpenOptions,
	GetOptions,
	QueryOptions,
	RemoteQueryOptions,
	ResultsIterator,
	SearchOptions,
	TransformOptions,
	TransformerAsConstructor,
	TransformerAsFunction,
	ValueTypeFromRequest,
	UpdateCallbacks,
	UpdateMergeStrategy,
	UpdateOptions,
	UpdateReason,
	WaitBehavior,
	WaitPolicy,
	ReachScope,
	PrefetchOptions,
	LateResultsEvent,
	JoiningOnMissedResults,
	JoiningTargets,
	JoiningTimeoutPolicy,
} from "./search.js";
export { coerceWithContext, coerceWithIndexed } from "./search.js";
export * from "./operation.js";
export { MAX_BATCH_SIZE as MAX_DOCUMENT_SIZE } from "./constants.js";
export { ClosedError } from "@peerbit/program";
export {
	type CustomDocumentDomain,
	createDocumentDomain,
	createDocumentDomainFromProperty,
} from "./domain.js";
export * from "./events.js";
export type {
	DocumentsLike,
	DocumentsLikeCountOptions,
	DocumentsLikeIndex,
	DocumentsLikeQuery,
	DocumentsLikeWaitForOptions,
} from "./like.js";
