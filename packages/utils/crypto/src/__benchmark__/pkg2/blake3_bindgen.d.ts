/* tslint:disable */
/* eslint-disable */
/**
 * @param {Uint8Array} data
 * @returns {Uint8Array}
 */
export function hash(data: Uint8Array): Uint8Array;
/**
 * @param {Uint8Array} data
 * @param {Uint8Array} out
 */
export function hash_mut(data: Uint8Array, out: Uint8Array): void;
/**
 * @param {number} ptr
 * @param {number} input_length
 * @returns {Uint8Array}
 */
export function hash_unsafe(ptr: number, input_length: number): Uint8Array;
/**
 * Allocate memory into the module's linear memory
 * and return the offset to the start of the block.
 * @param {number} len
 * @returns {number}
 */
export function alloc(len: number): number;

export type InitInput =
	| RequestInfo
	| URL
	| Response
	| BufferSource
	| WebAssembly.Module;

export interface InitOutput {
	readonly memory: WebAssembly.Memory;
	readonly hash: (a: number, b: number, c: number) => void;
	readonly hash_mut: (
		a: number,
		b: number,
		c: number,
		d: number,
		e: number,
		f: number
	) => void;
	readonly hash_unsafe: (a: number, b: number, c: number) => void;
	readonly alloc: (a: number) => number;
	readonly __wbindgen_add_to_stack_pointer: (a: number) => number;
	readonly __wbindgen_malloc: (a: number) => number;
	readonly __wbindgen_free: (a: number, b: number) => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;
/**
 * Instantiates the given `module`, which can either be bytes or
 * a precompiled `WebAssembly.Module`.
 *
 * @param {SyncInitInput} module
 *
 * @returns {InitOutput}
 */
export function initSync(module: SyncInitInput): InitOutput;

/**
 * If `module_or_path` is {RequestInfo} or {URL}, makes a request and
 * for everything else, calls `WebAssembly.instantiate` directly.
 *
 * @param {InitInput | Promise<InitInput>} module_or_path
 *
 * @returns {Promise<InitOutput>}
 */
export default function init(
	module_or_path?: InitInput | Promise<InitInput>
): Promise<InitOutput>;
