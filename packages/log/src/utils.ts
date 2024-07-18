import { equals as uequals } from "uint8arrays";

export const equals = (array1?: Uint8Array, array2?: Uint8Array) => {
	if (!!array1 !== !!array2) return false;
	if (!array1 || !array2) {
		return false;
	}
	return uequals(array1, array2);
};

export const max = <T>(...args: T[]) => args.reduce((m, e) => (e > m ? e : m));
export const min = <T>(...args: T[]) => args.reduce((m, e) => (e < m ? e : m));
