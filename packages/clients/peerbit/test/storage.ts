import fs from "fs";
import { Level } from "level";

const prefixPath = "packages/client/src/__tests__/keystore-test/";
export const createStore = (name = ".tmp/keystore"): Level => {
	if (fs?.mkdirSync) {
		fs.mkdirSync(prefixPath + name, { recursive: true });
	}
	return new Level(prefixPath + name, { valueEncoding: "view" });
};
