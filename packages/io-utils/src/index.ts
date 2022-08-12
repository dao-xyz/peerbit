import { BinaryReader, BinaryWriter } from "@dao-xyz/borsh";

export const U8IntArraySerializer = {
    serialize: (obj: Uint8Array, writer) => {
        writer.writeU32(obj.length);
        for (let i = 0; i < obj.length; i++) {
            writer.writeU8(obj[i])
        }
    },
    deserialize: (reader) => {
        const len = reader.readU32();
        const arr = new Uint8Array(len);
        for (let i = 0; i < len; i++) {
            arr[i] = reader.readU8();
        }
        return arr;
    }
};

export const U64Serializer = {
    serialize: (value: number, writer: BinaryWriter) => {
        writer.writeU64(value);
    },
    deserialize: (reader: BinaryReader) => {
        return reader.readU64().toNumber();
    }
}

export const U8IntArraySerializerOptional = {
    serialize: (obj: Uint8Array, writer) => {
        if (!obj) {
            writer.writeU8(0);
            return;
        }

        writer.writeU8(1);

        writer.writeU32(obj.length);
        for (let i = 0; i < obj.length; i++) {
            writer.writeU8(obj[i])
        }
    },
    deserialize: (reader) => {
        const exist = reader.readU8() === 1;
        if (!exist) {
            return null;
        }

        const len = reader.readU32();
        const arr = new Uint8Array(len);
        for (let i = 0; i < len; i++) {
            arr[i] = reader.readU8();
        }
        return arr;
    }
};

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

export const joinUint8Arrays = (arrays: Uint8Array[]) => {
    const flatNumberArray = arrays.reduce((acc, curr) => {
        acc.push(...curr);
        return acc;
    }, []);
    return new Uint8Array(flatNumberArray);
};
