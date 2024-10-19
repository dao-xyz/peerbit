import type { AbstractType } from "@dao-xyz/borsh";
import { type IdKey } from "./id.js";
import type { QueryLike, Sort } from "./query.js";

type MaybePromise<T = void> = Promise<T> | T;

export interface IndexedResult<
	T extends Record<string, any> = Record<string, any>,
> {
	id: IdKey;
	value: T;
}

export type IndexedResults<
	T extends Record<string, any> = Record<string, any>,
> = IndexedResult<T>[];

export interface IndexedValue<T = Record<string, any>> {
	id: IdKey;
	value: T;
}

export interface IterateOptions {
	query?: QueryLike;
	sort?: Sort | Sort[];
}

export interface DeleteOptions {
	query: QueryLike;
}

export interface SumOptions {
	query?: QueryLike;
	key: string | string[];
}

export interface CountOptions {
	query?: QueryLike;
}

export type NestedProperties<T> = {
	match: (obj: any) => obj is T;
	iterate: (nested: T, query: IterateOptions) => Promise<Record<string, any>[]>;
};

export type IteratorBatchProperties = {
	maxSize: number;
	sizeProperty: string[];
};
export type IndexEngineInitProperties<T, N> = {
	indexBy?: string[];
	nested?: NestedProperties<N>;
	schema: AbstractType<T>;
	/* iterator: { batch: IteratorBatchProperties }; */
};

/* type ResolveOptions = 'match' | 'full';
export type QueryOptions = { resolve: ResolveOptions };
export type ReturnTypeFromQueryOptions<T, O> = O extends { resolve: 'full' } ? T : Partial<T>;
 */

/* export type Shape = { [key: string]: true | Shape }; */
/* export type ShapeReturnType<T extends Shape> = {
	[K in keyof T]: T[K] extends true ? any : T[K] extends Shape ? ShapeReturnType<T[K]> : never;
}; */

export type Shape = { [key: string]: true | Shape | Shape[] };

export type ShapeReturnType<T> = T extends true
	? any
	: T extends Shape[]
		? ShapeReturnType<T[number]>[]
		: T extends Shape
			? { [K in keyof T]: ShapeReturnType<T[K]> }
			: never;

export type ReturnTypeFromShape<T, S> = S extends Shape
	? ShapeReturnType<S>
	: T;

export type IndexIterator<
	T extends Record<string, any>,
	S extends Shape | undefined,
> = {
	next: (
		amount: number,
	) => MaybePromise<IndexedResults<ReturnTypeFromShape<T, S>>>;
	all: () => MaybePromise<IndexedResults<ReturnTypeFromShape<T, S>>>;
	done: () => boolean | undefined;
	pending: () => MaybePromise<number>;
	close: () => MaybePromise<void>;
};

export interface Index<T extends Record<string, any>, NestedType = any> {
	init(
		properties: IndexEngineInitProperties<T, NestedType>,
	): MaybePromise<Index<T, NestedType>>;
	drop(): MaybePromise<void>;
	get(
		id: IdKey,
		options?: { shape: Shape },
	): MaybePromise<IndexedResult<T> | undefined>;
	put(value: T, id?: IdKey): MaybePromise<void>;
	del(query: DeleteOptions): MaybePromise<IdKey[]>;
	sum(query: SumOptions): MaybePromise<bigint | number>;
	count(query?: CountOptions): MaybePromise<number>;
	iterate<S extends Shape | undefined = undefined>(
		request?: IterateOptions,
		options?: { shape?: S; reference?: boolean },
	): IndexIterator<T, S>;

	getSize(): MaybePromise<number>;
	start(): MaybePromise<void>;
	stop(): MaybePromise<void>;
}

export const iteratorInSeries = <
	T extends Record<string, any>,
	S extends Shape | undefined,
>(
	...iterators: IndexIterator<T, S>[]
): IndexIterator<T, S> => {
	let i = 0;
	const done = () => i >= iterators.length;
	let current = iterators[i];
	const next = async (count: number) => {
		let acc: IndexedResults<ReturnTypeFromShape<T, S>> = [];

		while (!current.done() && i < iterators.length) {
			const next = await current.next(count);
			acc.push(...next);

			if (current.done()) {
				i++;
				if (i >= iterators.length) {
					break;
				}
				current = iterators[i];
			}

			if (acc.length >= count) {
				break;
			}
		}

		return acc;
	};
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
				for (const element of await next(100)) {
					results.push(element);
				}
			}
			return results;
		},
		pending: async () => {
			let allPendings = await Promise.all(
				iterators.map((iterator) => iterator.pending()),
			);
			return allPendings.reduce((acc, pending) => acc + pending, 0);
		},
	};
};

export interface Indices {
	init<T extends Record<string, any>, NestedType>(
		properties: IndexEngineInitProperties<T, NestedType>,
	): MaybePromise<Index<T, NestedType>>;
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
