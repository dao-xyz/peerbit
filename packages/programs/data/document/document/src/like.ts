import type {
	Context,
	IterationRequest,
	ResultIndexedValue,
	ResultValue,
	Results,
	SearchRequest,
	SearchRequestIndexed,
} from "@peerbit/document-interface";
import type * as indexerTypes from "@peerbit/indexer-interface";
import type { Program } from "@peerbit/program";
import type { SharedLogLike } from "@peerbit/shared-log";
import type { PeerRefs } from "@peerbit/stream-interface";
import type {
	GetOptions,
	QueryOptions,
	ReachScope,
	ResultsIterator,
	SearchOptions,
	ValueTypeFromRequest,
	WithContext,
	WithIndexedContext,
} from "./search.js";

export type DocumentsLikeQuery =
	| SearchRequest
	| SearchRequestIndexed
	| IterationRequest
	| {
			query?: indexerTypes.QueryLike | indexerTypes.Query[];
			sort?: indexerTypes.SortLike | indexerTypes.Sort | indexerTypes.Sort[];
	  };

export type DocumentsLikeWaitForOptions = {
	seek?: "any" | "present";
	signal?: AbortSignal;
	timeout?: number;
};

export type DocumentsLikeCountOptions = {
	query?: indexerTypes.Query | indexerTypes.QueryLike;
	approximate?: boolean | { scope?: ReachScope };
};

export type DocumentsLikeIndex<T, I, D = any> = {
	get: <Resolve extends boolean | undefined = true>(
		id: indexerTypes.Ideable | indexerTypes.IdKey,
		options?: GetOptions<T, I, D, Resolve>,
	) => Promise<ValueTypeFromRequest<Resolve, T, I> | undefined>;
	getDetailed: <Resolve extends boolean | undefined = true>(
		id: indexerTypes.IdKey | indexerTypes.IdPrimitive,
		options?: QueryOptions<T, I, D, Resolve>,
	) => Promise<
		| Results<
				Resolve extends false
					? ResultIndexedValue<WithContext<I>>
					: ResultValue<WithIndexedContext<T, I>>
		  >[]
		| undefined
	>;
	resolveId: (value: any) => indexerTypes.IdKey;
	iterate: <Resolve extends boolean | undefined = true>(
		query?: DocumentsLikeQuery,
		options?: QueryOptions<T, I, D, Resolve>,
	) => ResultsIterator<ValueTypeFromRequest<Resolve, T, I>>;
	search: <Resolve extends boolean | undefined = true>(
		query: DocumentsLikeQuery,
		options?: SearchOptions<T, I, D, Resolve>,
	) => Promise<ValueTypeFromRequest<Resolve, T, I>[]>;
	getSize: () => Promise<number> | number;
	waitFor: (
		peers: PeerRefs,
		options?: DocumentsLikeWaitForOptions,
	) => Promise<string[]>;
	wrappedIndexedType?: new (value: I, context: Context) => WithContext<I>;
	index?: {
		count?: (options?: indexerTypes.CountOptions) => Promise<number> | number;
		getSize?: () => Promise<number> | number;
		get?: (
			id: indexerTypes.IdKey,
			options?: { shape: indexerTypes.Shape },
		) =>
			| Promise<indexerTypes.IndexedResult<WithContext<I>> | undefined>
			| indexerTypes.IndexedResult<WithContext<I>>
			| undefined;
		iterate?: (
			request?: indexerTypes.IterateOptions,
			options?: { shape?: indexerTypes.Shape; reference?: boolean },
		) => indexerTypes.IndexIterator<
			WithContext<I>,
			indexerTypes.Shape | undefined
		>;
		put?: (value: WithContext<I>) => Promise<void> | void;
	};
};

export type DocumentsLike<T, I, D = any> = {
	closed?: boolean;
	events: EventTarget;
	changes: EventTarget;
	index: DocumentsLikeIndex<T, I, D>;
	log: SharedLogLike<any>;
	put(doc: T, options?: unknown): Promise<unknown>;
	get: (
		id: indexerTypes.Ideable | indexerTypes.IdKey,
		options?: Omit<GetOptions<T, I, D, true | undefined>, "resolve">,
	) => Promise<T | undefined>;
	del(
		id: indexerTypes.Ideable | indexerTypes.IdKey,
		options?: unknown,
	): Promise<unknown>;
	count: (options?: DocumentsLikeCountOptions) => Promise<number>;
	waitFor: (
		peers: PeerRefs,
		options?: DocumentsLikeWaitForOptions,
	) => Promise<string[]>;
	recover: () => Promise<void>;
	close: (from?: Program<any, any>) => Promise<boolean | void>;
};
