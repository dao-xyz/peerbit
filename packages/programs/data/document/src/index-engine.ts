import { PublicSignKey } from "@peerbit/crypto";
import {
	CloseIteratorRequest,
	CollectNextRequest,
	Context,
	SearchRequest
} from "./query.js";
import { IndexKey, IndexKeyPrimitiveType } from "./types";
import { AbstractType } from "@dao-xyz/borsh";

export interface IndexedResult {
	key: IndexKey;
	indexed: Record<string, any>;
	context: Context;
}
export interface IndexedResults {
	results: IndexedResult[];
	kept: number;
}
export interface IndexedValue<T = Record<string, any>> {
	key: IndexKey;
	indexed: T;
	context: Context;
	size: number;
}
export type IDocumentStore<T> = AbstractType<{
	index: { search: (query: SearchRequest) => Promise<T[]> };
}>;
export type IndexEngineInitProperties = {
	indexBy: string | string[];
	nestedType: IDocumentStore<any>;
};

export interface IndexEngine {
	init(properties: IndexEngineInitProperties): Promise<void> | void;
	start?(): Promise<void> | void;
	stop?(): Promise<void> | void;
	get(
		id: IndexKeyPrimitiveType
	): Promise<IndexedResult | undefined> | IndexedResult | undefined;
	put(id: IndexKeyPrimitiveType, value: IndexedValue): Promise<void> | void;
	del(id: IndexKeyPrimitiveType): Promise<void> | void;
	query(query: SearchRequest, from: PublicSignKey): Promise<IndexedResults>;
	next(query: CollectNextRequest, from: PublicSignKey): Promise<IndexedResults>;
	close(query: CloseIteratorRequest, from: PublicSignKey): Promise<void> | void;
	iterator(): IterableIterator<[IndexKeyPrimitiveType, IndexedValue]>;
	get size(): number;
}
