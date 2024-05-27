import { PublicSignKey } from "@peerbit/crypto";
import {
	CloseIteratorRequest,
	CollectNextRequest,
	CountRequest,
	SearchRequest,
	SumRequest
} from "./query.js";
import { IdKey } from "./id.js";
import type { Constructor } from "@dao-xyz/borsh";
type MaybePromise<T = void> = Promise<T> | T;

export interface IndexedResult<T extends Record<string, any> = Record<string, any>> {
	id: IdKey;
	value: T;
}

export interface IndexedResults<T extends Record<string, any> = Record<string, any>> {
	results: IndexedResult<T>[];
	kept: number;
}

export interface IndexedValue<T = Record<string, any>> {
	id: IdKey;
	value: T;
}

export type NestedProperties<T> = {
	match: (obj: any) => obj is T;
	query: (nested: T, query: SearchRequest) => Promise<Record<string, any>[]>;
};


export type IteratorBatchProperties = { maxSize: number, sizeProperty: string[] }
export type IndexEngineInitProperties<T, N> = {
	indexBy?: string[];
	nested?: NestedProperties<N>;
	schema: Constructor<T>
	/* iterator: { batch: IteratorBatchProperties }; */
};

export interface Index<T extends Record<string, any>, NestedType = any> {
	init(properties: IndexEngineInitProperties<T, NestedType>): MaybePromise<Index<T, NestedType>>
	drop(): MaybePromise<void>;
	get(id: IdKey): MaybePromise<IndexedResult<T> | undefined>;
	put(value: T, id?: IdKey): MaybePromise<void>;
	del(id: IdKey): MaybePromise<void>;
	sum(query: SumRequest): MaybePromise<number>;
	count(query: CountRequest): MaybePromise<number>;
	query(query: SearchRequest, from: PublicSignKey): MaybePromise<IndexedResults<T>>;
	next(query: CollectNextRequest, from: PublicSignKey): MaybePromise<IndexedResults<T>>;
	close(query: CloseIteratorRequest, from: PublicSignKey): MaybePromise<void>;
	getSize(): MaybePromise<number>;
	getPending(cursorId: string): number | undefined;
	start(): MaybePromise<void>;
	stop(): MaybePromise<void>;
	get cursorCount(): number;
}

export interface Indices {
	init<T extends Record<string, any>, NestedType>(properties: IndexEngineInitProperties<T, NestedType>): MaybePromise<Index<T, NestedType>>
	scope(name: string): MaybePromise<Indices>;
	start(): MaybePromise<void>;
	stop(): MaybePromise<void>;
	drop(): MaybePromise<void>;
}


/* export interface IndexEngine<T extends Record<string, any> = Record<string, any>, NestedType = any> {
	init(properties: IndexEngineInitProperties<NestedType>): MaybePromise<void>
	start?(): MaybePromise<void>;
	stop?(): MaybePromise<void>;
	clear(): MaybePromise<void>;
	scope: <TS extends Record<string, any> = Record<string, any>>(name: string) => MaybePromise<IndexEngine<TS>>;
	get(
		id: IdKey
	): MaybePromise<IndexedResult<T> | undefined>;
	put(value: IndexedValue<T>): MaybePromise<void>;
	del(id: IdKey): MaybePromise<void>;
	sum(query: SumRequest): MaybePromise<number>;
	count(query: CountRequest): MaybePromise<number>;
	query(query: SearchRequest, from: PublicSignKey): MaybePromise<IndexedResults<T>>;
	next(query: CollectNextRequest, from: PublicSignKey): MaybePromise<IndexedResults<T>>;
	close(query: CloseIteratorRequest, from: PublicSignKey): MaybePromise<void>;
	getSize(): MaybePromise<number>;
	getPending(cursorId: string): number | undefined;
	get cursorCount(): number;
}

export type IndexEngineConstuctor<T extends Record<string, any>> = (directory?: string) => MaybePromise<IndexEngine<T, any>>; */