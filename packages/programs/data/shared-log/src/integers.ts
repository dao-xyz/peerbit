import { BinaryReader } from "@dao-xyz/borsh";

export type u32 = number;
export type u64 = bigint;
export type NumberFromType<U extends "u32" | "u64"> = U extends "u32"
	? number
	: bigint;
export const MAX_U32 = 4294967295;
export const MAX_U64 = 18446744073709551615n;
export const HALF_MAX_U32 = 2147483647; // rounded down
export const HALF_MAX_U64 = 9223372036854775807n; // rounded down

export const denormalizer = <R extends "u32" | "u64">(
	resolution: R,
): ((number: number) => NumberFromType<R>) => {
	if (resolution === "u32") {
		return ((value: number) => {
			const result = Math.round(value * MAX_U32);
			return result > MAX_U32 ? MAX_U32 : result;
		}) as (number: number) => NumberFromType<R>;
	}
	return ((value: number) => {
		let result = BigInt(Math.round(value * Number(MAX_U64)));
		return result > MAX_U64 ? MAX_U64 : result;
	}) as (number: number) => NumberFromType<R>;
};

export const bytesToNumber = <R extends "u32" | "u64">(
	resolution: R,
): ((arr: Uint8Array) => NumberFromType<R>) => {
	if (resolution === "u32") {
		return ((arr: Uint8Array): number => {
			const seedNumber = new BinaryReader(arr).u32();
			return seedNumber;
		}) as (arr: Uint8Array) => NumberFromType<R>;
	}
	return ((arr: Uint8Array): bigint => {
		const seedNumber = new BinaryReader(arr).u64();
		return seedNumber;
	}) as (arr: Uint8Array) => NumberFromType<R>;
};

export interface Numbers<T extends "u32" | "u64"> {
	zero: NumberFromType<T>;
	maxValue: NumberFromType<T>;
	random: () => NumberFromType<T>;
	getGrid: (from: NumberFromType<T>, count: number) => NumberFromType<T>[];
	divRound: (a: NumberFromType<T>, b: number | bigint) => NumberFromType<T>;
	abs: (a: NumberFromType<T>) => NumberFromType<T>;
	min: (a: NumberFromType<T>, b: NumberFromType<T>) => NumberFromType<T>;
	max: (a: NumberFromType<T>, b: NumberFromType<T>) => NumberFromType<T>;
	denormalize: (value: number) => NumberFromType<T>;
	bytesToNumber: (bytes: Uint8Array) => NumberFromType<T>;
}

const getEvenlySpacedU32 = (from: number, count: number): number[] => {
	let ret: number[] = new Array(count);
	for (let i = 0; i < count; i++) {
		ret[i] = Math.round(from + (i * MAX_U32) / count) % MAX_U32;
	}
	return ret;
};

const getEvenlySpacedU64 = (from: bigint, count: number): bigint[] => {
	let ret: bigint[] = new Array(count);
	for (let i = 0; i < count; i++) {
		ret[i] = (from + (BigInt(i) * MAX_U64) / BigInt(count)) % MAX_U64;
	}
	return ret;
};

export const createNumbers = <N extends "u32" | "u64">(
	resolution: N,
): Numbers<N> => {
	const denormalizerFn = denormalizer(resolution);
	if (resolution === "u32") {
		return {
			random: () => denormalizerFn(Math.random()),
			zero: 0,
			maxValue: MAX_U32,
			getGrid: getEvenlySpacedU32 as any, // TODO fix this,
			divRound: (a, b) => Math.round(a / Number(b)) as any,
			abs: (a) => Math.abs(a as number),
			min: (a, b) => Math.min(a as number, b as number),
			max: (a, b) => Math.max(a as number, b as number),
			denormalize: denormalizerFn,
			bytesToNumber: bytesToNumber(resolution),
		} as Numbers<N>;
	} else if (resolution === "u64") {
		return {
			random: () => denormalizerFn(Math.random()),
			zero: 0n,
			maxValue: MAX_U64,
			getGrid: getEvenlySpacedU64 as any, // TODO fix this
			divRound: (a, b) => (a as bigint) / BigInt(b),
			abs: (a) => (a < 0n ? -a : a),
			min: (a, b) => (a < b ? a : b),
			max: (a, b) => (a > b ? a : b),
			denormalize: denormalizerFn,
			bytesToNumber: bytesToNumber(resolution),
		} as Numbers<N>;
	} else {
		throw new Error("Unsupported resolution");
	}
};
