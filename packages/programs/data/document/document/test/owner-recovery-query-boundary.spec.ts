import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import { toId } from "@peerbit/indexer-interface";
import { ClosedError, Program } from "@peerbit/program";
import { SharedLog } from "@peerbit/shared-log";
import { expect } from "chai";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import pDefer from "p-defer";
import { Peerbit } from "peerbit";
import sinon from "sinon";
import {
	BORSH_ENCODING_OPERATION,
	type Operation,
	PutOperation,
} from "../src/operation.js";
import { Documents } from "../src/program.js";
import { DocumentIndex, type OpenOptions } from "../src/search.js";
import { Document } from "./data.js";

type Recovery = (owner: RecoveryOwner, signal: AbortSignal) => Promise<void>;

// This fixture owns projection explicitly. It is not the Documents open path or
// a proof of delete semantics, crash atomicity, or recovery from a killed process.
@variant("test_paired_owner_recovery_query_boundary")
class RecoveryOwner extends Program<{ recover?: Recovery }> {
	@field({ type: SharedLog })
	log = new SharedLog<Operation>();

	@field({ type: DocumentIndex })
	index = new DocumentIndex<Document, Document, any>();

	async open(options?: { recover?: Recovery }) {
		const properties: OpenOptions<Document, Document, any> = {
			log: this.log,
			documentType: Document,
			dbType: Documents,
			documentEvents: new EventTarget() as any,
			replicate: async () => {},
			maybeOpen: async (value) => value,
			prefetch: true,
		};
		const finish = await (
			this.index as unknown as {
				openWithDeferredQuery(
					args: typeof properties,
				): Promise<() => Promise<void>>;
			}
		).openWithDeferredQuery(properties);
		const logOptions = {
			replicate: false as const,
			encoding: BORSH_ENCODING_OPERATION,
			nativeGraph: false as const,
			nativeBackbone: false as const,
			nativeRangePlanner: false as const,
		};
		await (
			this.log as unknown as {
				openWithRecovery(
					options: typeof logOptions,
					recover: (signal: AbortSignal) => Promise<void>,
				): Promise<void>;
			}
		).openWithRecovery(logOptions, async (signal) => {
			await options?.recover?.(this, signal);
		});
		await finish();
	}
}

describe("paired owner recovery and deferred document queries", () => {
	it("replays a persisted lower entry into a private projection before opening queries", async () => {
		const directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-paired-owner-recovery-"),
		);
		const entered = pDefer();
		const release = pDefer();
		let peer: Peerbit | undefined;
		let opening: Promise<RecoveryOwner> | undefined;
		try {
			peer = await Peerbit.create({ directory });
			const initial = await peer.open(new RecoveryOwner());
			const document = new Document({ id: "recovered", name: "disk entry" });
			const { entry } = await initial.log.append(
				new PutOperation({ data: serialize(document) }),
			);
			// The fixture deliberately leaves this local projection unapplied.
			expect(await initial.index.getSize()).to.equal(0);
			const address = initial.address;
			const reopened = initial.clone();
			await peer.stop();
			peer = await Peerbit.create({ directory });
			const queryOpen = sinon.spy(reopened.index._query, "open");
			const queryListeners = sinon.spy(
				reopened.index._query.events,
				"addEventListener",
			);
			const logRpcOpen = sinon.spy(reopened.log.rpc, "open");
			opening = peer.open(reopened, {
				args: {
					recover: async (owner, signal) => {
						expect(signal.aborted).to.equal(false);
						expect(owner.log.log.closed).to.equal(false);
						expect(await owner.index.index.getSize()).to.equal(0);
						const persisted = await owner.log.log.get(entry.hash);
						expect(persisted?.hash).to.equal(entry.hash);
						const operation = await persisted!.getPayloadValue();
						expect(operation).to.be.instanceOf(PutOperation);
						const value = deserialize(
							(operation as PutOperation).data,
							Document,
						);
						await owner.index.put(value, toId(value.id), persisted!, null);
						entered.resolve();
						await release.promise;
					},
				},
			});
			await Promise.race([
				entered.promise,
				opening.then(() => {
					throw new Error("Owner recovery completed without pausing");
				}),
			]);
			expect(await reopened.index.index.getSize()).to.equal(1);
			expect(() => reopened.index.getSize()).to.throw(ClosedError);
			await expect(
				reopened.index.get(document.id, { remote: false }),
			).to.be.rejectedWith(ClosedError);
			expect(queryOpen.called).to.equal(false);
			expect(
				queryListeners.getCalls().filter((call) => call.args[0] === "join"),
			).to.have.length(0);
			expect(logRpcOpen.called).to.equal(false);
			expect(reopened.log.syncronizer).to.equal(undefined);
			release.resolve();
			await opening;
			expect(reopened.address).to.equal(address);
			expect(queryOpen.calledOnce).to.equal(true);
			expect(logRpcOpen.calledOnce).to.equal(true);
			expect(await reopened.index.getSize()).to.equal(1);
			expect(
				(await reopened.index.get(document.id, { remote: false }))?.name,
			).to.equal(document.name);
			expect(
				(await reopened.index.index.get(toId(document.id)))?.value.__context
					.head,
			).to.equal(entry.hash);
		} finally {
			release.resolve();
			await opening?.catch(() => undefined);
			await peer?.stop();
			await fs.rm(directory, { recursive: true, force: true });
		}
	});
});
