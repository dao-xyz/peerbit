import {
	type Index,
	type IndexEngineInitProperties,
	type Indices,
	getIdProperty,
} from "@peerbit/indexer-interface";

export const setup = async <T extends Record<string, any>>(
	properties: Partial<IndexEngineInitProperties<T, any>> & { schema: any },
	createIndicies: (directory?: string) => Indices | Promise<Indices>,
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
	return { indices, store };
};
