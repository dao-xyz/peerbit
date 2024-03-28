import { SearchRequest } from "./query";

export type IDocumentStore<T> = {
	index: { search: (query: SearchRequest) => Promise<T[]> };
};
