import fs from "fs";
import { MemoryLevel } from "memory-level";
import { AbstractLevel } from "abstract-level";

const prefixPath = "./keystore-test/";
export const createStore = (
    name = "keystore"
): AbstractLevel<any, string, Uint8Array> => {
    if (fs && fs.mkdirSync) {
        fs.mkdirSync(prefixPath + name, { recursive: true });
    }
    return new MemoryLevel({ valueEncoding: "view" });
};
