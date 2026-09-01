const typedArrayPrototype = Object.getPrototypeOf(Uint8Array.prototype);
const typedArrayByteLength = Object.getOwnPropertyDescriptor(
	typedArrayPrototype,
	"byteLength",
)!.get!;
const typedArrayTag = Object.getOwnPropertyDescriptor(
	typedArrayPrototype,
	Symbol.toStringTag,
)!.get!;
const typedArraySet = Uint8Array.prototype.set;

/** Reads a real Uint8Array length without consulting shadowed properties. */
export const getExactUint8ArrayByteLength = (
	value: Uint8Array,
	name: string,
): number => {
	let byteLength: number;
	let tag: string | undefined;
	try {
		byteLength = typedArrayByteLength.call(value);
		tag = typedArrayTag.call(value);
	} catch {
		throw new TypeError(`${name} must be a genuine Uint8Array`);
	}
	if (tag !== "Uint8Array") {
		throw new TypeError(`${name} must be a Uint8Array`);
	}
	if (!Number.isSafeInteger(byteLength) || byteLength < 0) {
		throw new RangeError(`${name} has an invalid byte length`);
	}
	return byteLength;
};

/** Captures a previously bounded exact length without caller-controlled hooks. */
export const copyExactUint8Array = (
	value: Uint8Array,
	name: string,
	expectedByteLength: number,
): Uint8Array => {
	const byteLength = getExactUint8ArrayByteLength(value, name);
	if (byteLength !== expectedByteLength) {
		throw new RangeError(`${name} changed byte length while being captured`);
	}
	const copy = new Uint8Array(expectedByteLength);
	try {
		typedArraySet.call(copy, value);
	} catch {
		throw new TypeError(`${name} must not be detached`);
	}
	return copy;
};
