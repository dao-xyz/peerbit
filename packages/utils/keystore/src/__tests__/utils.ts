import { AbstractLevel } from "abstract-level";
import { Level } from "level";

export const createStore = async (
    path = "./keystore"
): Promise<AbstractLevel<any, string, Uint8Array>> => {
    const fs = await import("fs");
    if (fs && fs.mkdirSync) {
        fs.mkdirSync(path, { recursive: true });
    }
    return new Level(path, { valueEncoding: "view" });
};
