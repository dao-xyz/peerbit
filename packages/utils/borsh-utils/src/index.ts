import { BinaryReader, BinaryWriter, Constructor } from "@dao-xyz/borsh";

export const UInt8ArraySerializer = {
    serialize: (obj: Uint8Array, writer: BinaryWriter) => {
        writer.writeU32(obj.length);
        for (let i = 0; i < obj.length; i++) {
            writer.writeU8(obj[i]);
        }
    },
    deserialize: (reader: BinaryReader) => {
        const len = reader.readU32();
        const arr = new Uint8Array(len);
        for (let i = 0; i < len; i++) {
            arr[i] = reader.readU8();
        }
        return arr;
    },
};

export const StringSetSerializer = {
    deserialize: (reader: BinaryReader) => {
        const len = reader.readU32();
        const resp = new Set();
        for (let i = 0; i < len; i++) {
            resp.add(reader.readString());
        }
    },
    serialize: (arg: Set<string>, writer: BinaryWriter) => {
        writer.writeU32(arg.size);
        arg.forEach((s) => {
            writer.writeString(s);
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
                writer.writeU8(obj[i]);
            }
        },
        deserialize: (reader: BinaryReader) => {
            const arr = new Uint8Array(length);
            for (let i = 0; i < length; i++) {
                arr[i] = reader.readU8();
            }
            return arr;
        },
    };
};

export const fixedString = (length: number) => {
    return {
        serialize: (obj: string, writer: BinaryWriter) => {
            if (length !== obj.length) {
                throw new Error("Unexpected length");
            }
            for (let i = 0; i < length; i++) {
                writer.writeString(obj[i]);
            }
        },
        deserialize: (reader: BinaryReader) => {
            const arr = new Uint8Array(length);
            for (let i = 0; i < length; i++) {
                arr[i] = reader.readU8();
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

export type GetBuffer = {
    getBuffer(): Buffer;
};
export const bufferSerializer = (clazz: Constructor<GetBuffer>) => {
    return {
        serialize: (obj: GetBuffer, writer: BinaryWriter) => {
            const buffer = obj.getBuffer();
            writer.writeU32(buffer.length);
            for (let i = 0; i < buffer.length; i++) {
                writer.writeU8(buffer[i]);
            }
        },
        deserialize: (reader: BinaryReader) => {
            const len = reader.readU32();
            const arr = new Uint8Array(len);
            for (let i = 0; i < len; i++) {
                arr[i] = reader.readU8();
            }
            return new clazz(Buffer.from(arr));
        },
    };
};
