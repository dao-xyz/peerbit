// Regression coverage for native-backbone entries with `meta.next` chains
// (the file-share ready-manifest pattern: put v1, then put v2 with
// meta.next = [v1.entry]).
//
// Two defects are pinned here, both only reproducible on peers created with
// `createRustPeerbitOptions()` (TestSession does not activate the backbone):
//
// 1. The chained put itself crashed with "Missing data": prepared native
//    entries are hollow in JS (payload/signature bytes stay in the native
//    store) and the document index commit read `entry.publicKeys` eagerly.
// 2. A replicate:false observer syncing the chained head with
//    remote:{replicate:true} got "sync OK" while nothing persisted: the
//    shared-log opened its log on the raw native block store, which drops
//    the remote options joins need to resolve the head's parents, and
//    Log.join treats the failed resolve as recoverable and silently skips.
import { field, option, variant } from "@dao-xyz/borsh";
import { expect } from "chai";
import { Peerbit } from "peerbit";
import { createRustPeerbitOptions } from "peerbit/rust";
import { Documents } from "../src/program.js";
import { Document, TestStore } from "./data.js";

@variant("native_replicate_sync_indexable")
class ChainedIndexable {
	@field({ type: "string" })
	id: string;

	@field({ type: option("string") })
	name?: string;

	constructor(doc?: Document) {
		this.id = doc?.id ?? "";
		this.name = doc?.name;
	}
}

describe("native replicate sync", function () {
	this.timeout(120_000);

	let writer: Peerbit;
	let reader: Peerbit;
	let writerStore: TestStore<ChainedIndexable>;
	let readerStore: TestStore<ChainedIndexable>;

	const indexArgs = () => ({
		type: ChainedIndexable,
	});

	beforeEach(async () => {
		writer = await Peerbit.create({ ...createRustPeerbitOptions() });
		reader = await Peerbit.create({ ...createRustPeerbitOptions() });
		await writer.dial(reader);
	});

	afterEach(async () => {
		await writerStore?.close();
		await readerStore?.close();
		await writer?.stop();
		await reader?.stop();
	});

	const openStores = async (options?: { canPerform?: boolean }) => {
		writerStore = new TestStore<ChainedIndexable>({
			docs: new Documents<Document, ChainedIndexable>(),
		});
		readerStore = writerStore.clone();
		await writer.open(writerStore, {
			args: {
				replicate: { factor: 1 },
				index: indexArgs(),
				...(options?.canPerform
					? {
							canPerform: async (operation: any) => {
								for (const key of await operation.entry.getPublicKeys()) {
									if (key.equals(writer.identity.publicKey)) {
										return true;
									}
								}
								return false;
							},
						}
					: {}),
			},
		});
		await reader.open(readerStore, {
			args: {
				replicate: false,
				index: indexArgs(),
			},
		});
		expect((writerStore.docs.log as any)._nativeBackbone, "writer backbone")
			.to.exist;
		expect((readerStore.docs.log as any)._nativeBackbone, "reader backbone")
			.to.exist;
		await readerStore.docs.log.waitForReplicator(writer.identity.publicKey);
	};

	const putChained = async (id: string) => {
		const pending = await writerStore.docs.put(
			new Document({ id, name: "v1" }),
		);
		const appended = await writerStore.docs.put(
			new Document({ id, name: "v2" }),
			{
				meta: { next: [pending.entry] },
			},
		);
		return appended.entry.hash;
	};

	it("puts a chained head natively without crashing on hollow entries", async () => {
		await openStores();
		await putChained("chained-put");
		const local = await writerStore.docs.index.get("chained-put", {
			local: true,
			remote: false,
		});
		expect(local?.name).to.equal("v2");
	});

	it("persists a chained head on the reader for remote replicate get", async () => {
		await openStores({ canPerform: true });
		const headHash = await putChained("chained-sync");

		const resolved = await readerStore.docs.index.get("chained-sync", {
			local: true,
			remote: { timeout: 10_000, replicate: true },
		});
		expect(resolved?.name, "resolved document").to.equal("v2");

		// The synced head must actually be materializable from the reader's
		// own log afterwards — "sync OK" without persistence regresses this.
		const entry = await readerStore.docs.log.log.get(headHash);
		expect(entry, `reader entry for head ${headHash}`).to.exist;
	});

	it("resolves a chained head with remote replicate without canPerform", async () => {
		await openStores();
		await putChained("chained-sync-no-policy");

		const resolved = await readerStore.docs.index.get(
			"chained-sync-no-policy",
			{
				local: true,
				remote: { timeout: 10_000, replicate: true },
			},
		);
		expect(resolved?.name).to.equal("v2");
	});
});
