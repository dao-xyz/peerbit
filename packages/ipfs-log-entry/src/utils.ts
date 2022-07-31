export const arraysEqual = (array1?: any[] | Uint8Array, array2?: any[] | Uint8Array) => {
    if (!!array1 != !!array2)
        return false;
    return array1.length === array2.length && array1.every(function (value, index) { return value === array2[index] });
}

export const arraysCompare = (array1: Uint8Array, array2: Uint8Array): number => {
    const minLength = Math.min(array1.length, array2.length);
    for (let i = 0; i < minLength; i++) {
        if (array1[i] === array2[i]) {
            return 0;
        }
        if (array1[i] < array2[i]) {
            return -1;
        }
        return 1;

    }
    if (minLength === array1.length) {
        return 0;
    }

    if (array1.length < array2.length) {
        return -1;
    }
    return 1;
}