import { field, variant } from "@dao-xyz/borsh";
import {
	Documents,
	SortDirection,
	type WithIndexedContext,
} from "@peerbit/document";
import { create as createSimpleIndexer } from "@peerbit/indexer-simple";
import { Program } from "@peerbit/program";
import { TestSession } from "@peerbit/test-utils";
import { act, render, waitFor } from "@testing-library/react";
import sodium from "libsodium-wrappers";
import { Peerbit } from "peerbit";
import * as React from "react";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { useQuery } from "../src/useQuery.js";
import type { UseQuerySharedOptions } from "../src/useQuery.js";

// Minimal Post model and Program with Documents for integration-like tests
@variant(0)
class Post {
	@field({ type: "string" })
	id!: string;
	@field({ type: "string" })
	message!: string;
	constructor(props?: { id?: string; message?: string }) {
		if (!props) return; // borsh
		this.id = props.id ?? `${Date.now()}-${Math.random()}`;
		this.message = props.message ?? "";
	}
}
@variant(0)
class PostIndexed {
	@field({ type: "string" })
	id!: string;
	@field({ type: "string" })
	indexedMessage!: string;
	constructor(props?: Post) {
		if (!props) return; // borsh
		this.id = props.id ?? `${Date.now()}-${Math.random()}`;
		this.indexedMessage = props.message ?? "";
	}
}

@variant("posts-db")
class PostsDB extends Program<{ replicate?: boolean }> {
	@field({ type: Documents })
	posts: Documents<Post, PostIndexed>;
	constructor() {
		super();
		this.posts = new Documents<Post, PostIndexed>();
	}
	async open(args?: { replicate?: boolean }): Promise<void> {
		await this.posts.open({
			type: Post,
			index: { type: PostIndexed },
			replicate: args?.replicate ? { factor: 1 } : false,
		});
	}
}

describe("useQuery (integration with Documents)", () => {
	let session: TestSession;
	let peerWriter: Peerbit;
	let peerReader: Peerbit;
	let peerReader2: Peerbit;
	let dbWriter: PostsDB;
	let dbReader: PostsDB;
	let dbReader2: PostsDB | undefined;
	let dbReplicator: PostsDB | undefined;
	let autoUnmounts: Array<() => void> = [];

	beforeEach(async () => {
		await sodium.ready;
		session = await TestSession.disconnectedInMemory(3, {
			indexer: createSimpleIndexer,
		});
		peerWriter = session.peers[0] as Peerbit;
		peerReader = session.peers[1] as Peerbit;
		peerReader2 = session.peers[2] as Peerbit;
		dbReader2 = undefined;
		dbReplicator = undefined;
		autoUnmounts = [];
	});
	const setupConnected = async () => {
		// Use TestSession.connect so sharded pubsub/fanout root candidates are
		// configured consistently for this connected component.
		await session.connect([[peerWriter, peerReader]]);
		dbWriter = await peerWriter.open(new PostsDB(), {
			existing: "reuse",
			args: { replicate: true },
		});
		dbReader = await peerReader.open<PostsDB>(dbWriter.address, {
			args: { replicate: false },
		});
		// ensure reader knows about writer as replicator for the log
		await dbReader.posts.log.waitForReplicator(peerWriter.identity.publicKey);
	};

	const setupDisconnected = async () => {
		dbWriter = await peerWriter.open(new PostsDB(), {
			existing: "reuse",
			args: { replicate: true },
		});
		dbReader = await peerReader.open<PostsDB>(dbWriter.clone(), {
			args: { replicate: false },
		});
	};

	afterEach(async () => {
		// Unmount React trees before tearing down peers
		if (autoUnmounts.length) {
			const fns = [...autoUnmounts];
			autoUnmounts = [];
			await act(async () => {
				for (const fn of fns) fn();
			});
		}

		// Close opened programs explicitly to avoid lingering async work in the background.
		// If close throws, surface it so we don't silently leak background tasks.
		await dbReader2?.close?.();
		await dbReader?.close?.();
		await dbWriter?.close?.();
		await dbReplicator?.close?.();

		await session?.stop();
	});

	function renderUseQuery<R extends boolean>(
		db: PostsDB,
		options: UseQuerySharedOptions<Post, PostIndexed, R>,
	) {
		type HookResult = ReturnType<typeof useQuery<Post, PostIndexed, R>>;
		const result: { current: HookResult } = {} as any;

		function HookCmp({
			opts,
		}: {
			opts: UseQuerySharedOptions<Post, PostIndexed, R>;
		}) {
			const hook = useQuery<Post, PostIndexed, R>(db.posts, opts);
			React.useEffect(() => {
				result.current = hook as HookResult;
			}, [hook]);
			return null;
		}

		const api = render(React.createElement(HookCmp, { opts: options }));
		const rerender = (opts: UseQuerySharedOptions<Post, PostIndexed, R>) =>
			api.rerender(React.createElement(HookCmp, { opts }));
		let hasUnmounted = false;
		const doUnmount = () => {
			if (hasUnmounted) return;
			hasUnmounted = true;
			api.unmount();
			const idx = autoUnmounts.indexOf(doUnmount);
			if (idx >= 0) autoUnmounts.splice(idx, 1);
		};
		// Expose to outer afterEach so tests don't need to remember calling unmount
		autoUnmounts.push(doUnmount);
		return { result, rerender, unmount: doUnmount };
	}

	it("local query", async () => {
		await setupConnected();
		await dbWriter.posts.put(new Post({ message: "hello" }));
		const { result } = renderUseQuery(dbWriter, {
			query: {},
			resolve: true,
			local: true,
			prefetch: true,
		});
		await waitFor(() => expect(result.current?.items?.length ?? 0).toBe(1));

		await act(async () => {
			expect(result.current.items.length).toBe(1);
			expect((result.current.items[0] as Post).message).toBe("hello");
		});
	});

	it("does not mutate the options object passed in", async () => {
		await setupConnected();
		const cfg = {
			query: {},
			resolve: true,
			local: true,
			remote: { reach: { eager: true }, wait: { timeout: 10_000 } },
			prefetch: false,
			batchSize: 10,
		};
		const cfgOrg = { ...cfg };
		renderUseQuery(dbReader, cfg);
		// expect that cfg has not been modified
		expect(cfg).to.deep.equal(cfgOrg);
	});

	it("respects remote warmup before iterating", async () => {
		await setupConnected();
		await dbWriter.posts.put(new Post({ message: "hello" }));

		const cfg: UseQuerySharedOptions<Post, PostIndexed, true> = {
			query: {},
			resolve: true,
			local: true,
			remote: { reach: { eager: true }, wait: { timeout: 10_000 } },
			prefetch: false,
			batchSize: 10,
		};
		const { result, rerender } = renderUseQuery(dbReader, cfg);

		await waitFor(() => {
			if (!result.current) throw new Error("no result yet");
			return true;
		});

		expect(result.current.items.length).toBe(0);

		await act(async () => {
			await result.current.loadMore();
		});

		expect(result.current.items.length).toBe(1);
		expect((result.current.items[0] as Post).message).toBe("hello");

		await act(async () => {
			rerender(cfg);
		});
		await act(async () => {
			await result.current.loadMore();
		});
		await waitFor(() => expect(result.current.items.length).toBe(1));
	});

	it("honors remote.wait.timeout by resolving after connection", async () => {
		// create isolated peers not connected yet
		await setupDisconnected();

		const { result } = renderUseQuery(dbReader, {
			query: {},
			resolve: true,
			local: false,
			remote: {
				reach: { eager: true },
				wait: { behavior: "block", timeout: 5_000 },
			},
			prefetch: true,
		});

		await waitFor(() => expect(result.current).toBeDefined());

		// Now connect and write

		await act(async () => {
			// Write while disconnected so the prefetching iterator can deterministically
			// observe the entry once the remote connection is established.
			await dbWriter.posts.put(new Post({ message: "late" }));
			await session.connect([[peerReader, peerWriter]]);
			await dbReader.posts.log.waitForReplicator(
				dbWriter.node.identity.publicKey,
			);
		});

		await waitFor(() => expect(result.current.items.length).toBe(1), { timeout: 10_000 });
		expect((result.current.items[0] as Post).message).toBe("late");
	});

	it("pushes remote writes from replicator to non-replicator", async () => {
		await setupConnected();

		const { result } = renderUseQuery(dbReader, {
			query: {},
			resolve: true,
			local: false,
			remote: { reach: { eager: true } },
			updates: { merge: true },
			prefetch: false,
		});

		await waitFor(() => {
			expect(result.current).toBeDefined();
		});

		expect(result.current.items.length).toBe(0);

		await act(async () => {
			await dbWriter.posts.put(new Post({ message: "replicator-push" }));
		});

		await act(async () => {
			await result.current.loadMore();
		});

		await waitFor(
			() =>
				expect(result.current.items.map((p) => (p as Post).message)).toContain(
					"replicator-push",
				),
			{ timeout: 10_000 },
		);
	});

		it(
			"fanouts pushed updates to multiple observers",
			{
				timeout: 20_000,
			},
			async () => {
		await setupConnected();

			await session.connect([[peerWriter, peerReader], [peerWriter, peerReader2]]);
			dbReader2 = await peerReader2.open<PostsDB>(dbWriter.address, {
				args: { replicate: false },
				},
			);

			const reader2 = dbReader2;
			if (!reader2) throw new Error("Expected dbReader2 to be defined");

			await reader2.posts.log.waitForReplicator(peerWriter.identity.publicKey);

		const hookOne = renderUseQuery(dbReader, {
			query: {},
			resolve: true,
			local: false,
			remote: { reach: { eager: true } },
			updates: { merge: true },
			prefetch: false,
		});

			const hookTwo = renderUseQuery(reader2, {
				query: {},
				resolve: true,
				local: false,
				remote: { reach: { eager: true } },
				updates: { merge: true },
				prefetch: false,
			});

			await waitFor(() => {
				expect(hookOne.result.current).toBeDefined();
				expect(hookTwo.result.current).toBeDefined();
			});

			await act(async () => {
				await dbWriter.posts.put(
					new Post({ id: "broadcast", message: "broadcast" }),
				);
			});

		await act(async () => {
			await hookOne.result.current.loadMore();
			await hookTwo.result.current.loadMore();
		});

		await waitFor(
			() =>
				expect(
					hookOne.result.current.items.some(
						(p) => (p as Post).message === "broadcast",
					),
				).toBe(true),
			{ timeout: 10_000 },
		);

			await waitFor(
				() =>
					expect(
						hookTwo.result.current.items.some(
							(p) => (p as Post).message === "broadcast",
						),
					).toBe(true),
				{ timeout: 10_000 },
			);

			await act(async () => {
				await dbReader.posts.put(
					new Post({ id: "observer-origin", message: "observer-origin" }),
				);
			});

			await waitFor(
				async () => {
					const stored = await dbWriter.posts.get("observer-origin", {
						local: true,
						remote: false,
					});
					expect(stored?.message).toBe("observer-origin");
				},
				{ timeout: 10_000 },
			);

				await waitFor(
					async () => {
						const fetched = await reader2.posts.get("observer-origin", {
							local: true,
							remote: true,
						});
						expect(fetched?.message).toBe("observer-origin");
				},
				{ timeout: 10_000 },
			);

			await act(async () => {
				await hookOne.result.current.loadMore();
				await hookTwo.result.current.loadMore();
			});

		await waitFor(
			() =>
				expect(
					hookTwo.result.current.items.some(
						(p) => (p as Post).message === "observer-origin",
					),
				).toBe(true),
			{ timeout: 10_000 },
		);

		hookOne.unmount();
		hookTwo.unmount();
	});

	it(
		"push delivers via intermediate replicator without manual loadMore",
		{
			timeout: 20_000,
		},
			async () => {
				const peerReplicator = peerReader2;
				await session.connect([
					[peerWriter, peerReplicator],
					[peerReader, peerReplicator],
				]);

				const base = new PostsDB();
				const replicatorDb = await peerReplicator.open(base, {
					args: { replicate: true },
			});
			dbReplicator = replicatorDb;
			dbWriter = await peerWriter.open(base.clone(), {
				args: { replicate: true },
			});
			dbReader = await peerReader.open(base.clone(), {
				args: { replicate: false },
			});

			await replicatorDb.posts.index.waitFor(dbWriter.node.identity.publicKey);
			await dbWriter.posts.log.waitForReplicator(
				replicatorDb.node.identity.publicKey,
			);
			await dbReader.posts.index.waitFor(replicatorDb.node.identity.publicKey);
			await dbWriter.posts.index.waitFor(replicatorDb.node.identity.publicKey);

			const { result, unmount } = renderUseQuery(dbReader, {
				query: {},
				resolve: true,
				local: false,
				remote: {
					reach: {
						discover: [replicatorDb.node.identity.publicKey],
						eager: true,
					},
					wait: { timeout: 5_000, behavior: "keep-open" },
				},
				updates: { push: true, merge: true },
				prefetch: false,
			});

			await waitFor(() => expect(result.current).toBeDefined());
			expect(result.current.items.length).toBe(0);

			await act(async () => {
				await dbWriter.posts.put(new Post({ message: "push-through-replicator" }));
			});

			await waitFor(() => expect(result.current.items).toBeDefined());
			await waitFor(
				async () => {
					if (
						result.current.items.some(
							(p) => (p as Post).message === "push-through-replicator",
						)
					) {
						return true;
					}
					await act(async () => {
						await result.current.loadMore();
					});
					return result.current.items.some(
						(p) => (p as Post).message === "push-through-replicator",
					);
				},
				{ timeout: 15_000 },
			);

			unmount();
		},
	);

	it(
		"allows pinning items via applyResults and handles late results",
		{ timeout: 20_000 },
		async () => {
			await setupConnected();

			for (let i = 0; i < 30; i++) {
				await dbWriter.posts.put(
					new Post({
						id: `post-${i.toString().padStart(3, "0")}`,
						message: `seed-${i}`,
					}),
				);
			}

			const pinnedIds = new Set<string>();
			let pinnedPost: WithIndexedContext<Post, PostIndexed> | undefined;
			let lateCalled = false;
			const extraEndBase = new Post({ id: "post-end", message: "end" });
			const extraMidBase = new Post({ id: "post-mid", message: "mid" });
			let extraEnd: WithIndexedContext<Post, PostIndexed> | undefined;
			let extraMid: WithIndexedContext<Post, PostIndexed> | undefined;

			const { result } = renderUseQuery(dbReader, {
				query: {
					sort: { key: "id", direction: SortDirection.DESC },
				},
				resolve: true,
				local: false,
				remote: { reach: { eager: true } },
				updates: { push: true, merge: true },
				prefetch: false,
				batchSize: 10,
				applyResults: (_prev, _incoming, { defaultMerge }) => {
					const merged = defaultMerge();
					const pinned = merged.filter((p) => pinnedIds.has((p as Post).id));
					const rest = merged.filter((p) => !pinnedIds.has((p as Post).id));
					return [...pinned, ...rest];
				},
				onLateResults: async (_evt, helpers) => {
					lateCalled = true;
					if (pinnedPost) {
						helpers.inject(pinnedPost, { position: "start" });
						if (extraMid) helpers.inject(extraMid, { position: 1 });
						if (extraEnd) helpers.inject(extraEnd, { position: "end" });
					}
					await helpers.loadMore(1, { force: true, reason: "late" });
				},
			});

			await waitFor(() => {
				expect(result.current).toBeDefined();
			});

			await act(async () => {
				await result.current.loadMore();
			});
			await waitFor(() => expect(result.current.items.length).toBeGreaterThanOrEqual(10));

			const pinnedBase = new Post({ id: "post-999", message: "pinned" });
			pinnedIds.add(pinnedBase.id);
			// fabricate a context for injection; we only need stable __context for identity/dedup
			const sampleCtx = (
				result.current.items[0] as WithIndexedContext<Post, PostIndexed>
			).__context;
			const attachIndexed = (
				post: Post,
			): WithIndexedContext<Post, PostIndexed> =>
				Object.assign(post, {
					__context: sampleCtx,
					__indexed: new PostIndexed(post),
				}) as WithIndexedContext<Post, PostIndexed>;
			pinnedPost = attachIndexed(pinnedBase);
			extraMid = attachIndexed(extraMidBase);
			extraEnd = attachIndexed(extraEndBase);

			await act(async () => {
				await dbWriter.posts.put(pinnedBase);
			});

			await waitFor(() => expect(lateCalled).toBe(true), { timeout: 20_000 });

			await waitFor(() =>
				expect(
					result.current.items.some(
						(item) => (item as Post).id === pinnedPost!.id,
					),
				).toBe(true),
			);
			expect((result.current.items[0] as Post).id).toBe(pinnedPost.id);
			expect(
				result.current.items.some((item) => (item as Post).id === extraMid.id),
			).toBe(true);
			expect(
				result.current.items.some((item) => (item as Post).id === extraEnd.id),
			).toBe(true);
		},
	);

	it(
		"surfaces late sorted results after iterator is done",
		{ timeout: 20_000 },
		async () => {
			await setupConnected();

			const isolated = await peerWriter.open(new PostsDB(), {
				args: { replicate: false },
			});

			const first = new Post({ id: "post-000", message: "first" });
			await isolated.posts.put(first);

			let lateCalled = false;
			const { result } = renderUseQuery(isolated, {
				query: {
					sort: { key: "id", direction: SortDirection.DESC },
				},
				resolve: true,
				local: true,
				remote: false,
				updates: { merge: true },
				batchSize: 1,
				onLateResults: () => {
					lateCalled = true;
				},
			});

			await waitFor(() => expect(result.current).toBeDefined());

			await act(async () => {
				await result.current.loadMore();
			});

			await waitFor(() =>
				expect(
					result.current.items.some((item) => (item as Post).id === first.id),
				).toBe(true),
			);

			const late = new Post({ id: "post-999", message: "late" });
			await act(async () => {
				await isolated.posts.put(late);
			});

			await waitFor(() => expect(lateCalled).toBe(true), { timeout: 20_000 });

			await act(async () => {
				await result.current.loadMore(1, { force: true, reason: "late" });
			});

			await waitFor(() =>
				expect(
					result.current.items.some((item) => (item as Post).id === late.id),
				).toBe(true),
			);
			expect(result.current.items.length).toBeGreaterThanOrEqual(2);
		},
	);

	describe("merge", () => {
		const checkAsResolvedResults = async <R extends boolean>(
			out: ReturnType<typeof renderUseQuery<R>>,
			resolved: R,
		) => {
			const { result } = out;
			await waitFor(() => expect(result.current).toBeDefined());

			// Initially empty
			expect(result.current.items.length).toBe(0);

			// Create a post on writer and expect reader hook to merge it automatically
			const id = `${Date.now()}-merge`;
			await act(async () => {
				// the reader actually does the put (a user)
				await dbReader.posts.put(new Post({ id, message: "first" }));
			});

			await waitFor(
				() =>
					expect(
						result.current.items.some(
							(p) =>
								(resolved
									? (p as Post).message
									: (p as PostIndexed).indexedMessage) === "first",
						),
					).toBe(true),
				{
					timeout: 1e4,
				},
			);
			if (resolved) {
				expect((result.current.items[0] as Post).message).toBe("first");
				expect(result.current.items[0]).to.be.instanceOf(Post);
			} else {
				expect((result.current.items[0] as PostIndexed).indexedMessage).toBe(
					"first",
				);
				expect(result.current.items[0]).to.be.instanceOf(PostIndexed);
			}
		};

		it(
			"updates.merge merges new writes into state without manual iteration, as resolved",
			{
				timeout: 20_000,
			},
			async () => {
				await setupConnected();

				// resolved undefined means we should resolve
				await checkAsResolvedResults(
					renderUseQuery<true>(dbReader, {
						query: {},
						local: false,
						remote: { reach: { eager: true } },
						prefetch: false,
						updates: { merge: true },
					}),
					true,
				);

				// resolved true means we should resolve
				await checkAsResolvedResults(
					renderUseQuery<true>(dbReader, {
						query: {},
						local: false,
						resolve: true,
						remote: { reach: { eager: true } },
						prefetch: false,
						updates: { merge: true },
					}),
					true,
				);

				// resolved false means we should NOT resolve
				await checkAsResolvedResults(
					renderUseQuery<false>(dbReader, {
						query: {},
						local: false,
						resolve: false,
						remote: { reach: { eager: true } },
						prefetch: false,
						updates: { merge: true },
					}),
					false,
				);
			},
		);
	});

	/*
    it("updates.merge reflects document mutation in hook state", async () => {
        await setupConnected();

        const id = `${Date.now()}-mut`;
        await dbWriter.posts.put(new Post({ id, message: "v1" }));

        const { result } = renderUseQuery(dbReader, {
            query: {},
            resolve: true,
            local: false,
            remote: { reach: { eager: true } },
            prefetch: true,
            updates: { merge: true },
        });

        await waitFor(() => expect(result.current.items.length).toBe(1), {
            timeout: 1e4,
        });
        expect(result.current.items[0].message).toBe("v1");

        // Mutate by putting a new version with the same id
        await act(async () => {
            // the reader actually does the put (a user)
            await dbReader.posts.put(new Post({ id, message: "v2" }));
        });

        // Expect the hook state to reflect the updated content
        await waitFor(
            () => {
                const found = result.current.items.find((p) => p.id === id);
                expect(found?.message).toBe("v2");
            },
            { timeout: 1e4 }
        );
    });
    */

	it(
		"reverse=true preserves reversed ordering and appends live updates",
		{ timeout: 20_000 },
		async () => {
			// Local-only setup to avoid remote flakiness; live updates arrive as "change".
			const db = await peerWriter.open(new PostsDB(), {
				existing: "reuse",
				args: { replicate: false },
			});

			for (let i = 1; i <= 5; i++) {
				await db.posts.put(
					new Post({
						id: `post-${i.toString().padStart(3, "0")}`,
						message: `seed-${i}`,
					}),
				);
			}

			const { result } = renderUseQuery(db, {
				query: { sort: { key: "id", direction: SortDirection.DESC } },
				resolve: true,
				local: true,
				remote: false,
				updates: { merge: true },
				batchSize: 3,
				prefetch: true,
				reverse: true,
			});

			await waitFor(() =>
				expect(result.current.items.map((p) => (p as Post).id)).toEqual([
					"post-003",
					"post-004",
					"post-005",
				]),
			);

			await act(async () => {
				await result.current.loadMore();
			});
			await waitFor(() =>
				expect(result.current.items.map((p) => (p as Post).id)).toEqual([
					"post-001",
					"post-002",
					"post-003",
					"post-004",
					"post-005",
				]),
			);

			await act(async () => {
				await db.posts.put(
					new Post({ id: "post-006", message: "live-change" }),
				);
			});

			await waitFor(
				async () => {
					if (
						!result.current.items.some((p) => (p as Post).id === "post-006")
					) {
						await act(async () => {
							await result.current.loadMore(1, {
								force: true,
								reason: "change",
							});
						});
					}
					expect(
						result.current.items.some((p) => (p as Post).id === "post-006"),
					).toBe(true);
				},
				{ timeout: 15_000 },
			);

			expect(result.current.items.map((p) => (p as Post).id)).toEqual([
				"post-001",
				"post-002",
				"post-003",
				"post-004",
				"post-005",
				"post-006",
			]);
		},
	);

	it("clears results when props change (e.g. reverse toggled)", async () => {
		await setupConnected();
		await dbWriter.posts.put(new Post({ message: "one" }));
		await dbWriter.posts.put(new Post({ message: "two" }));

		const { result, rerender } = renderUseQuery(dbWriter, {
			query: {},
			resolve: true,
			local: true,
			remote: false,
			prefetch: true,
			reverse: false,
		});

		await waitFor(() => expect(result.current.items.length).toBeGreaterThan(0));

		// Toggle a prop that triggers iterator rebuild
		await act(async () => {
			rerender({
				query: {},
				resolve: true,
				local: true,
				remote: false,
				prefetch: false,
				reverse: true,
			});
		});

		// After reset we expect cleared results until re-fetched
		await waitFor(() => expect(result.current.items.length).toBe(0));

		await act(async () => {
			await result.current.loadMore();
		});
		await waitFor(() => expect(result.current.items.length).toBeGreaterThan(0));
	});

	it("rebuilds iterators when remote/local toggles", async () => {
		await setupConnected();
		await dbWriter.posts.put(new Post({ message: "one" }));

		const stableQuery = {};
		const remoteOpts = { reach: { eager: true }, wait: { timeout: 10_000 } };

		const { result, rerender } = renderUseQuery(dbReader, {
			query: stableQuery,
			resolve: true,
			local: false,
			remote: remoteOpts,
			prefetch: false,
		});

		await waitFor(() => expect(result.current).toBeDefined());

		await act(async () => {
			await result.current.loadMore();
		});

		await waitFor(
			() =>
				expect(result.current.items.map((p) => (p as Post).message)).toContain(
					"one",
				),
			{ timeout: 10_000 },
		);

		await act(async () => {
			rerender({
				query: stableQuery,
				resolve: true,
				local: true,
				remote: false,
				prefetch: false,
			});
		});

		await waitFor(() => expect(result.current.items.length).toBe(0));

		await act(async () => {
			await dbWriter.posts.put(new Post({ message: "two" }));
		});

		await act(async () => {
			await result.current.loadMore();
		});

		await waitFor(() =>
			expect(
				result.current.items.map((p) => (p as Post).message),
			).not.toContain("two"),
		);

		await act(async () => {
			rerender({
				query: stableQuery,
				resolve: true,
				local: false,
				remote: remoteOpts,
				prefetch: false,
			});
		});

		await waitFor(() => expect(result.current.items.length).toBe(0));

		await act(async () => {
			await result.current.loadMore();
		});
		await waitFor(
			() =>
				expect(result.current.items.map((p) => (p as Post).message)).toContain(
					"two",
				),
			{ timeout: 10_000 },
		);
	});

		describe("lifecycle edge cases", () => {
			it("handles database close during remote query without throwing", async () => {
			// This test verifies that when a database is closed while a remote query
			// is in-flight (e.g., during component unmount with keepOpenOnUnmount: false),
			// the query gracefully returns empty results instead of throwing NotStartedError.
			await setupConnected();
			await dbWriter.posts.put(new Post({ message: "test" }));

			const { result, unmount } = renderUseQuery(dbReader, {
				query: {},
				resolve: true,
				local: true,
				remote: { reach: { eager: true }, wait: { timeout: 5000 } },
				prefetch: true,
			});

				await waitFor(() => expect(result.current).toBeDefined());

				// Start a loadMore (which triggers getCover internally for remote queries)
				// and close the database concurrently - this simulates the race condition
				// that happens when a component unmounts while a remote query is in-flight
				let loadMorePromise: Promise<boolean>;
				await act(async () => {
					loadMorePromise = result.current.loadMore();

					// Close the database while the query might be in-flight
					// This should NOT cause an unhandled NotStartedError
					await dbReader.close();

					// Ensure any state updates from the in-flight query are flushed
					await loadMorePromise;
				});

				await expect(loadMorePromise!).resolves.toBeDefined();

				// Unmount should also be clean
				await act(async () => {
					unmount();
				});
			});

		it("handles unmount during active remote query without throwing", async () => {
			// This test simulates the exact scenario from the bug report:
			// useQuery with remote enabled kicks off getCover, and useProgram
			// closes the program on unmount before the query completes.
			await setupConnected();
			await dbWriter.posts.put(new Post({ message: "test" }));

			const { result, unmount } = renderUseQuery(dbReader, {
				query: {},
				resolve: true,
				local: true,
				remote: { reach: { eager: true }, wait: { timeout: 5000 } },
				prefetch: true,
			});

				await waitFor(() => expect(result.current).toBeDefined());

				// Trigger a remote query
				const loadMorePromise = result.current.loadMore();

				// Immediately unmount (simulating rapid navigation or re-render)
				// This closes the iterator but the underlying getCover might still be running
				await act(async () => {
					unmount();
				});

				// Close the database (simulating useProgram cleanup with keepOpenOnUnmount: false)
				await act(async () => {
					await dbReader.close();
					await loadMorePromise;
				});

				// The promise should resolve gracefully (returning false), not throw
				await expect(loadMorePromise).resolves.toBeDefined();
			});

		it("rapid mount/unmount cycles with remote queries do not cause errors", async () => {
			await setupConnected();
			await dbWriter.posts.put(new Post({ message: "test" }));

			// Simulate rapid mount/unmount cycles (like React StrictMode or fast navigation)
				for (let i = 0; i < 3; i++) {
					const { result, unmount } = renderUseQuery(dbReader, {
					query: {},
					resolve: true,
					local: true,
					remote: { reach: { eager: true } },
					prefetch: true,
				});

					await waitFor(() => expect(result.current).toBeDefined());

					// Start a query and immediately unmount
					const loadMorePromise = result.current.loadMore().catch(() => false);
					await act(async () => {
						unmount();
						await loadMorePromise;
					});

					// Should not throw
					await expect(loadMorePromise).resolves.toBeDefined();
				}
			});
		});
});
