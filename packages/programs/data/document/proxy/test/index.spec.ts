import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import {
	LoopbackPair,
	bindService,
	createProxyFromService,
} from "@dao-xyz/borsh-rpc";
import { Ed25519PublicKey } from "@peerbit/crypto";
import {
	IterationRequest,
	SearchRequest,
	SearchRequestIndexed,
} from "@peerbit/document-interface";
import { Context } from "@peerbit/document-interface";
import * as indexerTypes from "@peerbit/indexer-interface";
import { Sort, SortDirection } from "@peerbit/indexer-interface";
import {
	SharedLogEntriesIteratorService,
	SharedLogReplicationIteratorService,
	SharedLogService,
} from "@peerbit/shared-log-proxy";
import { expect } from "chai";
import { openDocuments } from "../src/client.js";
import {
	Bytes,
	DocumentsChange,
	DocumentsGetRequest,
	DocumentsIndexResult,
	DocumentsIterateRequest,
	DocumentsIteratorBatch,
	DocumentsIteratorService,
	DocumentsService,
} from "../src/index.js";

@variant("binary_document")
class BinaryDocument {
	@field({ type: "string" })
	id: string;

	@field({ type: Uint8Array })
	bytes: Uint8Array;

	constructor(properties?: { id?: string; bytes?: Uint8Array }) {
		this.id = properties?.id ?? "";
		this.bytes = properties?.bytes ?? new Uint8Array();
	}
}

const ensureCustomEvent = () => {
	if (typeof (globalThis as any).CustomEvent === "function") {
		return;
	}

	class CustomEventPolyfill<T = any> extends Event {
		detail: T;
		constructor(type: string, params?: CustomEventInit<T>) {
			super(type, params);
			this.detail = params?.detail as T;
		}
	}

	(globalThis as any).CustomEvent = CustomEventPolyfill;
};

const waitFor = async (fn: () => boolean, timeoutMs = 1000) => {
	const start = Date.now();
	while (true) {
		if (fn()) return;
		if (Date.now() - start > timeoutMs) {
			throw new Error("Timed out");
		}
		await new Promise((r) => setTimeout(r, 10));
	}
};

const buildContext = (head: string) =>
	new Context({
		created: 0n,
		modified: 0n,
		head,
		gid: "gid",
		size: 0,
	});

describe("@peerbit/document-proxy", () => {
	it("serializes SortLike values for document-interface requests", () => {
		const sortLike = [{ key: ["timestamp"], direction: SortDirection.ASC }];

		const req = new IterationRequest({ sort: sortLike as any });
		const reqBytes = serialize(req);
		const decoded = deserialize(reqBytes, IterationRequest) as IterationRequest;
		expect(decoded.sort).to.have.length(1);
		expect(decoded.sort[0]).to.be.instanceOf(Sort);

		const search = new SearchRequest({ sort: sortLike as any });
		expect(() => serialize(search)).to.not.throw();

		const searchIndexed = new SearchRequestIndexed({ sort: sortLike as any });
		expect(() => serialize(searchIndexed)).to.not.throw();
	});

	it("coerces sort wrappers for index.iterate over RPC", async () => {
		const createChannelPair = () => {
			const aHandlers = new Set<(data: Uint8Array) => void>();
			const bHandlers = new Set<(data: Uint8Array) => void>();
			const aCloseHandlers = new Set<() => void>();
			const bCloseHandlers = new Set<() => void>();
			let closed = false;

			const closeAll = () => {
				if (closed) return;
				closed = true;
				for (const handler of aCloseHandlers) handler();
				for (const handler of bCloseHandlers) handler();
				aHandlers.clear();
				bHandlers.clear();
				aCloseHandlers.clear();
				bCloseHandlers.clear();
			};

			const a = {
				send: (data: Uint8Array) => {
					if (closed) return;
					for (const handler of bHandlers) handler(data);
				},
				onMessage: (handler: (data: Uint8Array) => void) => {
					aHandlers.add(handler);
					return () => aHandlers.delete(handler);
				},
				close: closeAll,
				onClose: (handler: () => void) => {
					aCloseHandlers.add(handler);
					return () => aCloseHandlers.delete(handler);
				},
			};

			const b = {
				send: (data: Uint8Array) => {
					if (closed) return;
					for (const handler of aHandlers) handler(data);
				},
				onMessage: (handler: (data: Uint8Array) => void) => {
					bHandlers.add(handler);
					return () => bHandlers.delete(handler);
				},
				close: closeAll,
				onClose: (handler: () => void) => {
					bCloseHandlers.add(handler);
					return () => bCloseHandlers.delete(handler);
				},
			};

			return { a, b, close: closeAll };
		};

		const { a: clientChannel, b: serverChannel, close } = createChannelPair();

		const iterateRequests: DocumentsIterateRequest[] = [];

		const publicKey = new Ed25519PublicKey({
			publicKey: new Uint8Array(32).fill(1),
		});

		const sharedLogService = new SharedLogService({
			logGet: async () => undefined,
			logHas: async () => false,
			logToArray: async () => [],
			logGetHeads: async () =>
				new SharedLogEntriesIteratorService({
					next: async () => ({ entries: [], done: true }) as any,
					pending: async () => undefined,
					done: async () => true,
					close: async () => {},
				}),
			logLength: async () => 0n,
			replicationIterate: async () =>
				new SharedLogReplicationIteratorService({
					next: async () => ({ items: [], done: true }) as any,
					pending: async () => undefined,
					done: async () => true,
					close: async () => {},
				}),
			replicationCount: async () => 0n,
			getReplicators: async () => [],
			waitForReplicator: async () => {},
			waitForReplicators: async () => {},
			replicate: async () => {},
			unreplicate: async () => {},
			calculateCoverage: async () => 0,
			getMyReplicationSegments: async () => [],
			getAllReplicationSegments: async () => [],
			resolution: async () => "u32",
			publicKey: async () => publicKey,
			close: async () => {},
		});

		const server = new DocumentsService({
			put: async () => {},
			get: async () => undefined,
			del: async () => {},
			iterate: async (request: DocumentsIterateRequest) => {
				iterateRequests.push(request);
				return new DocumentsIteratorService({
					next: async () =>
						new DocumentsIteratorBatch({ results: [], done: true }),
					pending: async () => undefined,
					done: async () => true,
					close: async () => {},
				});
			},
			openLog: async () => sharedLogService,
			close: async () => {},
		});

		const unbind = bindService(DocumentsService, serverChannel as any, server);

		const docs = await openDocuments({
			client: {
				openPort: async () => clientChannel as any,
			} as any,
			id: new Uint8Array(32),
			typeName: "binary_document",
			type: BinaryDocument,
		});

		const iterator = docs.index.iterate(
			{
				sort: [{ key: ["timestamp"], direction: SortDirection.ASC }],
			},
			{ local: true, remote: false, fetch: 1 } as any,
		);

		await iterator.close();

		const iteratorWithQuery = docs.index.iterate(
			{
				query: { id: "doc-1" },
				sort: [{ key: ["timestamp"], direction: SortDirection.ASC }],
			},
			{ local: true, remote: false, fetch: 1 } as any,
		);

		await iteratorWithQuery.close();
		await docs.close();

		unbind();
		close();

		expect(iterateRequests).to.have.length(2);

		const sortOnlyRequest = iterateRequests[0]!;
		expect(sortOnlyRequest.request.query).to.deep.equal([]);
		expect(sortOnlyRequest.request.sort).to.have.length(1);
		expect(sortOnlyRequest.request.sort[0]).to.be.instanceOf(Sort);
		expect(sortOnlyRequest.request.sort[0]!.key).to.deep.equal(["timestamp"]);
		expect(sortOnlyRequest.request.sort[0]!.direction).to.equal(
			SortDirection.ASC,
		);

		const queryAndSortRequest = iterateRequests[1]!;
		expect(queryAndSortRequest.request.query).to.have.length(1);
		expect(queryAndSortRequest.request.query[0]).to.be.instanceOf(
			indexerTypes.StringMatch,
		);
		expect((queryAndSortRequest.request.query[0] as any).key).to.deep.equal([
			"id",
		]);
		expect((queryAndSortRequest.request.query[0] as any).value).to.equal(
			"doc-1",
		);
		expect(queryAndSortRequest.request.sort).to.have.length(1);
		expect(queryAndSortRequest.request.sort[0]).to.be.instanceOf(Sort);
	});

	it("streams change events over borsh-rpc", async () => {
		ensureCustomEvent();

		const loop = new LoopbackPair();
		const store = new Map<string, Uint8Array>();

		let svc: DocumentsService | undefined;
		const server = new DocumentsService({
			put: async (doc: Bytes) => {
				const decoded = deserialize(doc.value, BinaryDocument);
				store.set(decoded.id, doc.value);
				svc!.changes.dispatchEvent(
					new CustomEvent("change", {
						detail: new DocumentsChange({
							added: [
								new DocumentsIndexResult({
									context: buildContext(decoded.id),
									value: doc.value,
								}),
							],
							removed: [],
						}),
					}),
				);
			},
			get: async (request: DocumentsGetRequest) => {
				const key = String(request.id.primitive);
				const bytes = store.get(key);
				return bytes
					? new DocumentsIndexResult({
							context: buildContext(key),
							value: bytes,
						})
					: undefined;
			},
			del: async (id: indexerTypes.IdKey) => {
				const key = String(id.primitive);
				const bytes = store.get(key);
				if (!bytes) return;
				store.delete(key);
				svc!.changes.dispatchEvent(
					new CustomEvent("change", {
						detail: new DocumentsChange({
							added: [],
							removed: [
								new DocumentsIndexResult({
									context: buildContext(key),
									value: bytes,
								}),
							],
						}),
					}),
				);
			},
			iterate: async () => {
				return new DocumentsIteratorService();
			},
			close: async () => {},
		});
		svc = server;

		const unbind = bindService(DocumentsService, loop.a, server);
		const client = createProxyFromService(
			DocumentsService,
			loop.b,
		) as unknown as DocumentsService;

		const changes: DocumentsChange[] = [];
		client.changes.addEventListener("change", (e: any) => {
			changes.push(e.detail as DocumentsChange);
		});

		await client.put(
			new Bytes({
				value: serialize(
					new BinaryDocument({ id: "a", bytes: new Uint8Array([1]) }),
				),
			}),
		);
		await waitFor(() => changes.length === 1);
		expect(
			deserialize(changes[0]!.added[0]!.value!, BinaryDocument).id,
		).to.equal("a");
		expect(changes[0]!.added[0]!.context.head).to.equal("a");

		const got = await client.get(
			new DocumentsGetRequest({ id: indexerTypes.toId("a") }),
		);
		expect(got).to.not.equal(undefined);
		expect(deserialize(got!.value!, BinaryDocument).bytes).to.deep.equal(
			new Uint8Array([1]),
		);
		expect(got!.context.head).to.equal("a");

		await client.del(indexerTypes.toId("a"));
		await waitFor(() => changes.length === 2);
		expect(
			deserialize(changes[1]!.removed[0]!.value!, BinaryDocument).id,
		).to.equal("a");
		expect(changes[1]!.removed[0]!.context.head).to.equal("a");

		await client.close();
		unbind();
	});
});
