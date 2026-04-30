import type { AbstractType } from "@dao-xyz/borsh";
import * as types from "@peerbit/document-interface";

const isObject = (value: unknown): value is Record<string, unknown> =>
	value != null && typeof value === "object";

const isResultLike = (
	value: unknown,
): value is {
	context: unknown;
	entries?: unknown;
	init: (type: unknown) => void;
} =>
	isObject(value) &&
	"_source" in value &&
	"context" in value &&
	typeof value.init === "function";

export const isResultIndexedValue = <I>(
	value: unknown,
): value is types.ResultIndexedValue<I> =>
	value instanceof types.ResultIndexedValue ||
	(isResultLike(value) && Array.isArray(value.entries));

export const isResultValue = <T, I = Record<string, any>>(
	value: unknown,
): value is types.ResultValue<T, I> =>
	value instanceof types.ResultValue ||
	(isResultLike(value) && !Array.isArray(value.entries));

export const isResults = <R extends types.Result>(
	value: unknown,
): value is types.Results<R> =>
	value instanceof types.Results ||
	(isObject(value) && Array.isArray(value.results) && "kept" in value);

export const initializeResultType = <T, I>(
	result: unknown,
	documentType: AbstractType<T>,
	indexedType: AbstractType<I>,
) => {
	if (isResultValue(result)) {
		result.init(documentType);
		return;
	}
	(result as types.ResultIndexedValue<I>).init(indexedType);
};
