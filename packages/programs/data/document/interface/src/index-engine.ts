import { PublicSignKey } from "@peerbit/crypto";
import {
	CloseIteratorRequest,
	CollectNextRequest,
	Context,
	SearchRequest
} from "./query.js";
import { IdKey, IdPrimitive } from "./id.js";
import { Constructor } from "@dao-xyz/borsh";

export interface IndexedResult {
	id: IdKey;
	indexed: Record<string, any>;
	context: Context;
}

export interface IndexedResults {
	results: IndexedResult[];
	kept: number;
}

export interface IndexedValue<T = Record<string, any>> {
	id: IdKey;
	indexed: T;
	context: Context;
	size: number;
}

export type NestedProperties<T> = {
	match: (obj: any) => obj is T;
	query: (nested: T, query: SearchRequest) => Promise<Record<string, any>[]>;
};

export type IndexEngineInitProperties<N> = {
	indexBy: string | string[];
	nested?: NestedProperties<N>;
	maxBatchSize: number;
	schema?: Constructor<any>;
};

export interface IndexEngine<N = any> {
	init(properties: IndexEngineInitProperties<N>): Promise<void> | void;
	start?(): Promise<void> | void;
	stop?(): Promise<void> | void;
	get(
		id: IdKey
	): Promise<IndexedResult | undefined> | IndexedResult | undefined;
	put(value: IndexedValue): Promise<void> | void;
	del(id: IdKey): Promise<void> | void;
	query(query: SearchRequest, from: PublicSignKey): Promise<IndexedResults>;
	next(query: CollectNextRequest, from: PublicSignKey): Promise<IndexedResults>;
	close(query: CloseIteratorRequest, from: PublicSignKey): Promise<void> | void;
	iterator(): IterableIterator<[IdPrimitive, IndexedValue]>;
	getSize(): number | Promise<number>;
	getPending(cursorId: string): number | undefined;
	get cursorCount(): number;
}
