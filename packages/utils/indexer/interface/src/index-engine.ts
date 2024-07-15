import {
	CloseIteratorRequest,
	CollectNextRequest,
	CountRequest,
	DeleteRequest,
	SearchRequest,
	SumRequest
} from "./query.js";
import { IdKey } from "./id.js";
import type { AbstractType } from "@dao-xyz/borsh";
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
	schema: AbstractType<T>
	/* iterator: { batch: IteratorBatchProperties }; */
};

/* type ResolveOptions = 'match' | 'full';
export type QueryOptions = { resolve: ResolveOptions };
export type ReturnTypeFromQueryOptions<T, O> = O extends { resolve: 'full' } ? T : Partial<T>;
 */

export type Shape = { [key: string]: true | Shape }
export interface Index<T extends Record<string, any>, NestedType = any> {
	init(properties: IndexEngineInitProperties<T, NestedType>): MaybePromise<Index<T, NestedType>>
	drop(): MaybePromise<void>;
	get(id: IdKey, options?: { shape: Shape }): MaybePromise<IndexedResult<T> | undefined>;
	put(value: T, id?: IdKey): MaybePromise<void>;
	del(query: DeleteRequest): MaybePromise<IdKey[]>;
	sum(query: SumRequest): MaybePromise<bigint | number>;
	count(query: CountRequest): MaybePromise<number>;
	query(query: SearchRequest, options?: { shape?: Shape, reference?: boolean }): MaybePromise<IndexedResults<T>>;
	next(query: CollectNextRequest, options?: { shape?: Shape }): MaybePromise<IndexedResults<T>>;
	close(query: CloseIteratorRequest): MaybePromise<void>;

	/*
	query<O extends QueryOptions>(query: SearchRequest, options?: O): MaybePromise<IndexedResults<ReturnTypeFromQueryOptions<T, O>>>;
	next<O extends QueryOptions>(query: CollectNextRequest, options?: O): MaybePromise<IndexedResults<ReturnTypeFromQueryOptions<T, O>>>;
	*/

	getSize(): MaybePromise<number>;
	getPending(cursorId: string): number | undefined;
	start(): MaybePromise<void>;
	stop(): MaybePromise<void>;
	get cursorCount(): number;
}

export type IndexIterator<T> = ReturnType<typeof iterate<T>>;
export const iterate = <T>(index: Index<T, any>, query: SearchRequest) => {

	let isDone = false;
	let fetchedOnce = false;
	const next = async (count: number, options?: { shape?: Shape }) => {
		let res: IndexedResults<T>;
		if (!fetchedOnce) {
			fetchedOnce = true;
			query.fetch = count;
			res = await index.query(query, options);
		} else {
			res = await index.next(
				new CollectNextRequest({ id: query.id, amount: count }),
				options
			);
		}
		isDone = res.kept === 0;
		return res;
	}
	const done = () => isDone;
	const close = () => {
		return index.close(
			new CloseIteratorRequest({ id: query.id })
		);
	}
	return {
		next,
		done,
		close,
		all: async (options?: { shape: Shape }) => {
			const results: IndexedResult<T>[] = [];
			while (!done()) {
				for (const element of (await next(100, options)).results) {
					results.push(element);
				}
			}
			await close()
			return results
		}
	};
}


export type ResultsIterator<T> = ReturnType<typeof iterate<T>>

export const iteratorInSeries = <T>(...iterators: IndexIterator<T>[]) => {
	let i = 0;
	const done = () => i >= iterators.length;
	let current = iterators[i];
	const next = async (count: number) => {

		let acc: IndexedResults<T> = {
			kept: 0,
			results: []
		}

		while (current.done() === false && i < iterators.length) {

			const next = await current.next(count);
			acc.kept += next.kept;
			acc.results.push(...next.results);

			if (current.done()) {
				i++;
				if (i >= iterators.length) {
					break;
				}
				current = iterators[i];
			}

			if (acc.results.length >= count) {
				break;
			}
		}

		return acc;
	}
	return {
		next,
		done,
		close: async () => {
			for (const iterator of iterators) {
				await iterator.close();
			}
		},
		all: async () => {
			const results = [];
			while (!done()) {
				for (const element of (await next(100)).results) {
					results.push(element);
				}
			}
			return results;
		}
	};
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