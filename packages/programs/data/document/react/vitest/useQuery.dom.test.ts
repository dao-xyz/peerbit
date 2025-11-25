import { field, variant } from "@dao-xyz/borsh";
import { Documents } from "@peerbit/document";
import { Program } from "@peerbit/program";
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
	let peerWriter: Peerbit;
	let peerReader: Peerbit;
	let peerReader2: Peerbit | undefined;
	let dbWriter: PostsDB;
	let dbReader: PostsDB;
	let dbReader2: PostsDB | undefined;
	let autoUnmount: undefined | (() => void);

	beforeEach(async () => {
		await sodium.ready;
		peerWriter = await Peerbit.create();
		peerReader = await Peerbit.create();
		peerReader2 = undefined;
		dbReader2 = undefined;
	});
	const setupConnected = async () => {
		await peerWriter.dial(peerReader);
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
		autoUnmount?.();
		autoUnmount = undefined;
		await peerWriter?.stop();
		await peerReader?.stop();
		await peerReader2?.stop();
	});

	function renderUseQuery<R extends boolean>(
		db: PostsDB,
		options: UseQuerySharedOptions<Post, PostIndexed, R>,
	) {
		const result: {
			current: ReturnType<typeof useQuery<Post, PostIndexed, R>>;
		} = {} as any;

		function HookCmp({
			opts,
		}: {
			opts: UseQuerySharedOptions<Post, PostIndexed, R>;
		}) {
			const hook = useQuery<Post, PostIndexed, R>(db.posts, opts);
			React.useEffect(() => {
				result.current = hook;
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
			if (autoUnmount === doUnmount) {
				// clear outer reference if we still own it
				autoUnmount = undefined;
			}
		};
		// Expose to outer afterEach so tests don't need to remember calling unmount
		autoUnmount = doUnmount;
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
			expect(result.current.items[0].message).toBe("hello");
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
		expect(result.current.items[0].message).toBe("hello");

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
			await dbReader.node.dial(dbWriter.node.getMultiaddrs());
			await dbWriter.posts.put(new Post({ message: "late" }));
			await dbReader.posts.log.waitForReplicator(
				dbWriter.node.identity.publicKey,
			);
		});

		await waitFor(() => expect(result.current.items.length).toBe(1));
		expect(result.current.items[0].message).toBe("late");
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

	it("fanouts pushed updates to multiple observers", async () => {
		await setupConnected();

		peerReader2 = await Peerbit.create();
		await peerReader2.dial(peerWriter);
		dbReader2 = await peerReader2.open<PostsDB>(dbWriter.address, {
			args: { replicate: false },
		});

		await dbReader2.posts.log.waitForReplicator(peerWriter.identity.publicKey);

		const hookOne = renderUseQuery(dbReader, {
			query: {},
			resolve: true,
			local: false,
			remote: { reach: { eager: true } },
			updates: { merge: true },
			prefetch: false,
		});

		const hookTwo = renderUseQuery(dbReader2, {
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
			await dbWriter.posts.put(new Post({ message: "broadcast" }));
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
			await dbReader.posts.put(new Post({ message: "observer-origin" }));
		});

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
			const peerReplicator = await Peerbit.create();
			try {
				await peerWriter.dial(peerReplicator);
				await peerReader.dial(peerReplicator);

				const base = new PostsDB();
				const replicatorDb = await peerReplicator.open(base, {
					args: { replicate: true },
				});
				dbWriter = await peerWriter.open(base.clone(), {
					args: { replicate: true },
				});
				dbReader = await peerReader.open(base.clone(), {
					args: { replicate: false },
				});

				await replicatorDb.posts.index.waitFor(
					dbWriter.node.identity.publicKey,
				);
				await dbWriter.posts.log.waitForReplicator(
					replicatorDb.node.identity.publicKey,
				);
				await dbReader.posts.index.waitFor(
					replicatorDb.node.identity.publicKey,
				);
				await dbWriter.posts.index.waitFor(
					replicatorDb.node.identity.publicKey,
				);

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
					await dbWriter.posts.put(
						new Post({ message: "push-through-replicator" }),
					);
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
			} finally {
				await peerReplicator.stop();
			}
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
	it("clears results when props change (e.g. reverse toggled)", async () => {
		await setupConnected();
		await dbWriter.posts.put(new Post({ message: "one" }));
		await dbWriter.posts.put(new Post({ message: "two" }));

		const { result, rerender } = renderUseQuery(dbReader, {
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
});
