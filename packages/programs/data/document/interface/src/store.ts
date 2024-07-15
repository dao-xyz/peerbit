
export type IDocumentStore<T> = {
	index: { search: (query: any) => Promise<T[]> };
};
