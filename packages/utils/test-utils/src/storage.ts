import { MemoryLevel } from "memory-level";
import { AbstractLevel } from "abstract-level";
export const createStore = (): AbstractLevel<any, string, Uint8Array> => {
    return new MemoryLevel({ valueEncoding: "view" });
};
