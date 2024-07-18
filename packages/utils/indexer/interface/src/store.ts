import { type SearchRequest } from "./query.js";

export type IDocumentStore<T> = {
	index: { search: (query: SearchRequest) => Promise<T[]> };
};
