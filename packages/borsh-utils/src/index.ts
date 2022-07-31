
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