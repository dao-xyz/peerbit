let wasm;

let cachedUint8Memory0 = null;

function getUint8Memory0() {
	if (cachedUint8Memory0 === null || cachedUint8Memory0.byteLength === 0) {
		cachedUint8Memory0 = new Uint8Array(wasm.memory.buffer);
	}
	return cachedUint8Memory0;
}

function getArrayU8FromWasm0(ptr, len) {
	return getUint8Memory0().subarray(ptr / 1, ptr / 1 + len);
}

const heap = new Array(128).fill(undefined);

heap.push(undefined, null, true, false);

function getObject(idx) {
	return heap[idx];
}

const cachedTextDecoder = new TextDecoder("utf-8", {
	ignoreBOM: true,
	fatal: true,
});

cachedTextDecoder.decode();

function getStringFromWasm0(ptr, len) {
	return cachedTextDecoder.decode(getUint8Memory0().subarray(ptr, ptr + len));
}

let heap_next = heap.length;

function addHeapObject(obj) {
	if (heap_next === heap.length) heap.push(heap.length + 1);
	const idx = heap_next;
	heap_next = heap[idx];

	heap[idx] = obj;
	return idx;
}

function dropObject(idx) {
	if (idx < 132) return;
	heap[idx] = heap_next;
	heap_next = idx;
}

function takeObject(idx) {
	const ret = getObject(idx);
	dropObject(idx);
	return ret;
}

let WASM_VECTOR_LEN = 0;

function passArray8ToWasm0(arg, malloc) {
	const ptr = malloc(arg.length * 1);
	getUint8Memory0().set(arg, ptr / 1);
	WASM_VECTOR_LEN = arg.length;
	return ptr;
}

let cachedInt32Memory0 = null;

function getInt32Memory0() {
	if (cachedInt32Memory0 === null || cachedInt32Memory0.byteLength === 0) {
		cachedInt32Memory0 = new Int32Array(wasm.memory.buffer);
	}
	return cachedInt32Memory0;
}
/**
 * @param {Uint8Array} data
 * @returns {Uint8Array}
 */
export function hash(data) {
	try {
		const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
		const ptr0 = passArray8ToWasm0(data, wasm.__wbindgen_malloc);
		const len0 = WASM_VECTOR_LEN;
		wasm.hash(retptr, ptr0, len0);
		var r0 = getInt32Memory0()[retptr / 4 + 0];
		var r1 = getInt32Memory0()[retptr / 4 + 1];
		var v1 = getArrayU8FromWasm0(r0, r1).slice();
		wasm.__wbindgen_free(r0, r1 * 1);
		return v1;
	} finally {
		wasm.__wbindgen_add_to_stack_pointer(16);
	}
}

/**
 * @param {Uint8Array} data
 * @param {Uint8Array} out
 */
export function hash_mut(data, out) {
	try {
		const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
		const ptr0 = passArray8ToWasm0(data, wasm.__wbindgen_malloc);
		const len0 = WASM_VECTOR_LEN;
		var ptr1 = passArray8ToWasm0(out, wasm.__wbindgen_malloc);
		var len1 = WASM_VECTOR_LEN;
		wasm.hash_mut(retptr, ptr0, len0, ptr1, len1, addHeapObject(out));
		var r0 = getInt32Memory0()[retptr / 4 + 0];
		var r1 = getInt32Memory0()[retptr / 4 + 1];
		if (r1) {
			throw takeObject(r0);
		}
	} finally {
		wasm.__wbindgen_add_to_stack_pointer(16);
	}
}

/**
 * @param {number} input_offset
 * @param {number} input_length
 * @returns {Uint8Array}
 */
export function hash_unsafe(input_offset, input_length) {
	try {
		const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
		wasm.hash_unsafe(retptr, input_offset, input_length);
		var r0 = getInt32Memory0()[retptr / 4 + 0];
		var r1 = getInt32Memory0()[retptr / 4 + 1];
		var v0 = getArrayU8FromWasm0(r0, r1).slice();
		wasm.__wbindgen_free(r0, r1 * 1);
		return v0;
	} finally {
		wasm.__wbindgen_add_to_stack_pointer(16);
	}
}

/**
 * @param {number} len
 * @returns {number}
 */
export function alloc(len) {
	const ret = wasm.alloc(len);
	return ret;
}

async function load(module, imports) {
	if (typeof Response === "function" && module instanceof Response) {
		if (typeof WebAssembly.instantiateStreaming === "function") {
			try {
				return await WebAssembly.instantiateStreaming(module, imports);
			} catch (e) {
				if (module.headers.get("Content-Type") != "application/wasm") {
					console.warn(
						"`WebAssembly.instantiateStreaming` failed because your server does not serve wasm with `application/wasm` MIME type. Falling back to `WebAssembly.instantiate` which is slower. Original error:\n",
						e
					);
				} else {
					throw e;
				}
			}
		}

		const bytes = await module.arrayBuffer();
		return await WebAssembly.instantiate(bytes, imports);
	} else {
		const instance = await WebAssembly.instantiate(module, imports);

		if (instance instanceof WebAssembly.Instance) {
			return { instance, module };
		} else {
			return instance;
		}
	}
}

function getImports() {
	const imports = {};
	imports.wbg = {};
	imports.wbg.__wbindgen_copy_to_typed_array = function (arg0, arg1, arg2) {
		new Uint8Array(
			getObject(arg2).buffer,
			getObject(arg2).byteOffset,
			getObject(arg2).byteLength
		).set(getArrayU8FromWasm0(arg0, arg1));
	};
	imports.wbg.__wbindgen_string_new = function (arg0, arg1) {
		const ret = getStringFromWasm0(arg0, arg1);
		return addHeapObject(ret);
	};
	imports.wbg.__wbindgen_object_drop_ref = function (arg0) {
		takeObject(arg0);
	};

	return imports;
}

function initMemory(imports, maybe_memory) {}

function finalizeInit(instance, module) {
	wasm = instance.exports;
	init.__wbindgen_wasm_module = module;
	cachedInt32Memory0 = null;
	cachedUint8Memory0 = null;

	return wasm;
}

function initSync(module) {
	const imports = getImports();

	initMemory(imports);

	if (!(module instanceof WebAssembly.Module)) {
		module = new WebAssembly.Module(module);
	}

	const instance = new WebAssembly.Instance(module, imports);

	return finalizeInit(instance, module);
}

async function init(input) {
	if (typeof input === "undefined") {
		input = new URL("blake3_bindgen_bg.wasm", import.meta.url);
	}
	const imports = getImports();

	try {
		input = await fetch(input);
	} catch (e) {
		if (!(e instanceof TypeError)) {
			throw e;
		}
		input = await (await import("node:fs/promises")).readFile(input);
	}
	initMemory(imports);

	const { instance, module } = await load(await input, imports);

	return finalizeInit(instance, module);
}

export { initSync };
export default init;
