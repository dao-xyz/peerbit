import type { WithIndexedContext } from "./search";

export interface DocumentsChange<T, I> {
	added: WithIndexedContext<T, I>[];
	removed: WithIndexedContext<T, I>[];
}
export interface DocumentEvents<T, I> {
	change: CustomEvent<DocumentsChange<T, I>>;
}
