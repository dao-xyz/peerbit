export const joinUint8Arrays = (arrays: Uint8Array[]) => {
    const flatNumberArray = arrays.reduce((acc, curr) => {
        acc.push(...curr);
        return acc;
    }, []);
    return new Uint8Array(flatNumberArray);
};
