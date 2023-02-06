import { equals as uequals, compare } from "uint8arrays";
export const equals = (array1?: Uint8Array, array2?: Uint8Array) => {
	if (!!array1 != !!array2) return false;
	if (!array1 || !array2) {
		return false;
	}
	return uequals(array1, array2);
};

export const startsWith = (array: Uint8Array, withArray: Uint8Array) => {
	if (!array || array.length < withArray.length) {
		return false;
	}

	for (let i = 0; i < withArray.length; i++) {
		if (array[i] !== withArray[i]) {
			return false;
		}
	}
	return true;
};

export { compare };
