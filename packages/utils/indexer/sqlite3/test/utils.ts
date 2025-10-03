import {
	type Index,
	type IndexEngineInitProperties,
	type Indices,
	getIdProperty,
} from "@peerbit/indexer-interface";
import { create as defaultCreate } from "../src/index.js";

const pendingCleanups: Array<() => Promise<void>> = [];

export const registerCleanup = (cleanup: () => Promise<void>) => {
	pendingCleanups.push(cleanup);
};

if (typeof afterEach === "function") {
	afterEach(async () => {
		while (pendingCleanups.length > 0) {
			const cleanup = pendingCleanups.pop();
			if (!cleanup) continue;
			try {
				await cleanup();
			} catch (error) {
				console.warn("sqlite3 test cleanup failed", error);
			}
		}
	});
}

export const setup = async <T extends Record<string, any>>(
	properties: Partial<IndexEngineInitProperties<T, any>> & { schema: any },
	createIndicies: (
		directory?: string,
	) => Indices | Promise<Indices> = defaultCreate,
): Promise<{ indices: Indices; store: Index<T, any>; directory?: string }> => {
	const indices = await createIndicies();
	await indices.start();
	const indexProps: IndexEngineInitProperties<T, any> = {
		...{
			indexBy: getIdProperty(properties.schema) || ["id"],
		},
		...properties,
	};
	const store = await indices.init(indexProps);

	registerCleanup(async () => {
		await store.stop?.();
		await indices.stop?.();
		await indices.drop?.();
	});
	return { indices, store };
};
