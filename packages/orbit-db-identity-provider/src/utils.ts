export const joinUint8Arrays = (arrays: Uint8Array[]) => {
    const flatNumberArray = arrays.reduce((acc, curr) => {
        acc.push(...curr);
        return acc;
    }, []);
    return new Uint8Array(flatNumberArray);
};
export const arraysEqual = (array1?: Uint8Array, array2?: Uint8Array) => {
    if (!!array1 != !!array2)
        return false;
    return array1.length === array2.length && array1.every(function (value, index) { return value === array2[index] });
}