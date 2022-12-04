import { BinaryReader, BinaryWriter } from "@dao-xyz/borsh";

export const StringSetSerializer = {
    deserialize: (reader: BinaryReader) => {
        const len = reader.u32();
        const resp = new Set();
        for (let i = 0; i < len; i++) {
            resp.add(reader.string());
        }
    },
    serialize: (arg: Set<string>, writer: BinaryWriter) => {
        writer.u32(arg.size);
        arg.forEach((s) => {
            writer.string(s);
        });
    },
};
export const fixedUint8Array = (length: number) => {
    return {
        serialize: (obj: Uint8Array, writer: BinaryWriter) => {
            if (length !== obj.length) {
                throw new Error("Unexpected length");
            }
            for (let i = 0; i < length; i++) {
                writer.u8(obj[i]);
            }
        },
        deserialize: (reader: BinaryReader) => {
            const arr = new Uint8Array(length);
            for (let i = 0; i < length; i++) {
                arr[i] = reader.u8();
            }
            return arr;
        },
    };
};

export const arraysEqual = (
    array1?: any[] | Uint8Array,
    array2?: any[] | Uint8Array
) => {
    if (!!array1 != !!array2) return false;
    if (!array1 || !array2) {
        return false;
    }
    return (
        array1.length === array2.length &&
        array1.every(function (value, index) {
            return value === array2[index];
        })
    );
};

export const arraysCompare = (
    array1: Uint8Array,
    array2: Uint8Array
): number => {
    if (array1 < array2) return -1;
    if (array1 > array2) return 1;
    return 0;
};

export const joinUint8Arrays = (arrays: Uint8Array[]) => {
    const flatNumberArray = arrays.reduce((acc: number[], curr) => {
        acc.push(...curr);
        return acc;
    }, []);
    return new Uint8Array(flatNumberArray);
};
