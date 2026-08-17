const typedArrayPrototype = Object.getPrototypeOf(
	Uint8Array.prototype,
) as object;
const byteLengthGetter = Object.getOwnPropertyDescriptor(
	typedArrayPrototype,
	"byteLength",
)?.get;
const typedArrayTagGetter = Object.getOwnPropertyDescriptor(
	typedArrayPrototype,
	Symbol.toStringTag,
)?.get;
const typedArraySet = Uint8Array.prototype.set;

if (!byteLengthGetter || !typedArrayTagGetter) {
	throw new Error("Uint8Array byte-length intrinsic is unavailable");
}

/**
 * Copy a real Uint8Array using only typed-array intrinsics. This rejects Proxy
 * lookalikes and ignores shadowed byteLength/iterator/index properties before
 * allocating an exact bounded destination.
 */
export const captureBoundedUint8Array = (
	value: unknown,
	minimum: number,
	maximum: number,
	name: string,
): Uint8Array => {
	let length: number;
	try {
		if (typedArrayTagGetter.call(value) !== "Uint8Array") {
			throw new Error("Wrong typed-array element kind");
		}
		length = byteLengthGetter.call(value) as number;
	} catch (error) {
		throw new Error(`Invalid ${name}`, { cause: error });
	}
	if (
		!Number.isSafeInteger(minimum) ||
		!Number.isSafeInteger(maximum) ||
		minimum < 0 ||
		maximum < minimum ||
		length < minimum ||
		length > maximum
	) {
		throw new Error(`Invalid ${name}`);
	}
	const captured = new Uint8Array(length);
	typedArraySet.call(captured, value as Uint8Array);
	return captured;
};
