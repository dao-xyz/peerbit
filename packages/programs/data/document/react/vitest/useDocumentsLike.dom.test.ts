import type {
	CountEstimate,
	DocumentsLike,
	ResultsIterator,
} from "@peerbit/document";
import * as indexerTypes from "@peerbit/indexer-interface";
import type { SharedLogLike } from "@peerbit/shared-log";
import { act, render, waitFor } from "@testing-library/react";
import * as React from "react";
import { describe, expect, it } from "vitest";
import { useCount } from "../src/useCount.js";
import { useLocal } from "../src/useLocal.js";
import { useQuery } from "../src/useQuery.js";

type Doc = { id: string; text: string };

const createIterator = <T>(values: T[]): ResultsIterator<T> => {
	let index = 0;
	let done = false;

	const next = async (amount: number) => {
		if (done) return [];
		const batch = values.slice(index, index + amount);
		index += batch.length;
		if (index >= values.length) {
			done = true;
		}
		return batch;
	};

	return {
		next,
		done: () => done,
		pending: async () => (done ? 0 : values.length - index),
		all: async () => {
			if (done) return [];
			const out = values.slice(index);
			index = values.length;
			done = true;
			return out;
		},
		first: async () => {
			const batch = await next(1);
			return batch[0];
		},
		close: async () => {
			done = true;
		},
		[Symbol.asyncIterator]: () => ({
			next: async () => {
				const batch = await next(1);
				if (!batch.length) return { done: true, value: undefined };
				return { done: false, value: batch[0] };
			},
			return: async () => {
				done = true;
				return { done: true, value: undefined };
			},
		}),
	};
};

const createDocumentsLike = (initial: Doc[]) => {
	let items = [...initial];
	const events = new EventTarget();
	const logEvents = new EventTarget();

	const toCountEstimate = (estimate: number): CountEstimate => ({
		estimate,
		errorMargin: undefined,
	});

	async function count(options?: {
		query?: unknown;
		approximate?: false | undefined;
	}): Promise<number>;
	async function count(options: {
		query?: unknown;
		approximate: true | { scope?: unknown };
	}): Promise<CountEstimate>;
	async function count(options?: {
		query?: unknown;
		approximate?: boolean | { scope?: unknown };
	}): Promise<number | CountEstimate> {
		if (options?.approximate) {
			return toCountEstimate(items.length);
		}
		return items.length;
	}

	const emitChange = () => {
		events.dispatchEvent(new CustomEvent("change"));
	};

	const log: SharedLogLike = {
		events: logEvents,
		log: {
			length: 0,
			get: async () => undefined,
			has: async () => false,
			getHeads: () => createIterator([]) as any,
			toArray: async () => [],
			blocks: { has: async () => false },
		},
		replicationIndex: {
			iterate: () =>
				({
					next: async () => [],
					done: () => true,
					all: async () => [],
					pending: async () => 0,
					close: async () => {},
				}) as any,
			count: async () => 0,
			getSize: async () => 0,
		},
		getReplicators: async () => new Set<string>(),
		waitForReplicator: async () => {},
		waitForReplicators: async () => {},
		replicate: async () => {},
		unreplicate: async () => {},
		calculateCoverage: async () => 0,
		getMyReplicationSegments: async () => [],
		getAllReplicationSegments: async () => [],
		close: async () => {},
	};

	const docs: DocumentsLike<Doc, Doc> & {
		closed?: boolean;
		address?: string;
		rootAddress?: string;
	} = {
		events,
		changes: events,
		log,
		index: {
			get: async <_Resolve extends boolean | undefined = true>(
				id: indexerTypes.Ideable | indexerTypes.IdKey,
				_options?: unknown,
			) => {
				const key = id instanceof indexerTypes.IdKey ? id.primitive : id;
				return items.find((item) => item.id === String(key)) as any;
			},
			getDetailed: async () => undefined,
			resolveId: (value) => indexerTypes.toId((value as Doc).id),
			iterate: <_Resolve extends boolean | undefined = true>() =>
				createIterator(items as any) as any,
			search: async <_Resolve extends boolean | undefined = true>() =>
				items as any,
			getSize: async () => items.length,
			waitFor: async () => [],
			index: {
				count: async () => items.length,
				getSize: async () => items.length,
			},
		},
		put: async (doc) => {
			items.push(doc);
			emitChange();
		},
		get: async (id) => {
			const key = id instanceof indexerTypes.IdKey ? id.primitive : id;
			return items.find((item) => item.id === String(key));
		},
		del: async (id) => {
			const key = id instanceof indexerTypes.IdKey ? id.primitive : id;
			items = items.filter((item) => item.id !== String(key));
			emitChange();
		},
		count,
		waitFor: async () => [],
		recover: async () => {},
		close: async () => {
			docs.closed = true;
			return true;
		},
	};
	docs.address = "docs-like-address";
	return { docs, add: docs.put };
};

describe("DocumentsLike hooks", () => {
	it("useCount works with a DocumentsLike stub", async () => {
		const { docs, add } = createDocumentsLike([{ id: "a", text: "one" }]);
		const result: { current?: number } = {};

		function Hook() {
			const count = useCount(docs, { query: {}, debounce: 0 });
			React.useEffect(() => {
				result.current = count;
			}, [count]);
			return null;
		}

		render(React.createElement(Hook));
		await waitFor(() => expect(result.current).toBe(1));

		await act(async () => {
			await add({ id: "b", text: "two" });
		});
		await waitFor(() => expect(result.current).toBe(2));
	});

	it("useLocal works with a DocumentsLike stub", async () => {
		const { docs, add } = createDocumentsLike([{ id: "a", text: "one" }]);
		const result: { current?: Doc[] } = {};

		function Hook() {
			const items = useLocal(docs, {
				query: {},
				id: "local",
				debounce: 0,
			});
			React.useEffect(() => {
				result.current = items;
			}, [items]);
			return null;
		}

		render(React.createElement(Hook));
		await waitFor(() => expect(result.current?.length).toBe(1));

		await act(async () => {
			await add({ id: "b", text: "two" });
		});
		await waitFor(() => expect(result.current?.length).toBe(2));
	});

	it("useQuery works with a DocumentsLike stub", async () => {
		const { docs } = createDocumentsLike([
			{ id: "a", text: "one" },
			{ id: "b", text: "two" },
		]);
		const result: { current?: ReturnType<typeof useQuery<Doc, Doc>> } = {};

		function Hook() {
			const hook = useQuery(docs, {
				query: {},
				resolve: true,
				prefetch: false,
				batchSize: 2,
			});
			React.useEffect(() => {
				result.current = hook;
			}, [hook]);
			return null;
		}

		render(React.createElement(Hook));
		await waitFor(() => expect(result.current).toBeTruthy());

		await act(async () => {
			await result.current!.loadMore(2);
		});
		await waitFor(() => expect(result.current!.items.length).toBe(2));
	});
});
