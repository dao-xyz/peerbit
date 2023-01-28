import { deserialize, field, serialize, variant, vec } from "@dao-xyz/borsh";
import { Documents, Operation, PutOperation } from "@dao-xyz/peerbit-document";
import { Entry } from "@dao-xyz/peerbit-log";
import { LogIndex, LogQueryRequest } from "@dao-xyz/peerbit-logindex";
import { PeerIdAddress, PublicSignKey } from "@dao-xyz/peerbit-crypto";
import { DeleteOperation } from "@dao-xyz/peerbit-document";
import {
	IdentityRelation,
	createIdentityGraphStore,
	getPathGenerator,
	hasPath,
	getFromByTo,
	getToByFrom,
	getRelation,
	AbstractRelation,
} from "./identity-graph.js";
import type { PeerId } from "@libp2p/interface-peer-id";
import { Program } from "@dao-xyz/peerbit-program";
import { CanRead, RPC } from "@dao-xyz/peerbit-rpc";
import { waitFor } from "@dao-xyz/peerbit-time";
import { AddOperationOptions } from "@dao-xyz/peerbit-store";
import sodium from "libsodium-wrappers";
await sodium.ready;

const canAppendByRelation = async (
	entry: Entry<Operation<IdentityRelation>>,
	isTrusted?: (key: PublicSignKey) => Promise<boolean>
): Promise<boolean> => {
	// verify the payload
	const operation = await entry.getPayloadValue();
	if (
		operation instanceof PutOperation ||
		operation instanceof DeleteOperation
	) {
		/*  const relation: Relation = operation.value || deserialize(operation.data, Relation); */

		const keys = await entry.getPublicKeys();
		const checkKey = async (key: PublicSignKey): Promise<boolean> => {
			if (operation instanceof PutOperation) {
				// TODO, this clause is only applicable when we modify the identityGraph, but it does not make sense that the canAppend method does not know what the payload will
				// be, upon deserialization. There should be known in the `canAppend` method whether we are appending to the identityGraph.

				const relation: AbstractRelation =
					operation._value || deserialize(operation.data, AbstractRelation);
				operation._value = relation;

				if (relation instanceof IdentityRelation) {
					if (!relation.from.equals(key)) {
						return false;
					}
				}

				// else assume the payload is accepted
			}
			if (isTrusted) {
				const trusted = await isTrusted(key);
				return trusted;
			} else {
				return true;
			}
		};
		for (const key of keys) {
			const result = await checkKey(key);
			if (result) {
				return true;
			}
		}
		return false;
	} else {
		return false;
	}
};

@variant("relations")
export class IdentityGraph extends Program {
	@field({ type: Documents })
	relationGraph: Documents<IdentityRelation>;

	constructor(props?: { id?: string }) {
		super(props);
		if (props) {
			this.relationGraph = createIdentityGraphStore({
				...props,
				id: this.id,
			});
		}
	}

	async canAppend(entry: Entry<Operation<IdentityRelation>>): Promise<boolean> {
		return canAppendByRelation(entry);
	}

	async setup(options?: { canRead?: CanRead }) {
		await this.relationGraph.setup({
			type: IdentityRelation,
			canAppend: this.canAppend.bind(this),
			canRead: options?.canRead,
		}); // self referencing access controller
	}

	async addRelation(
		to: PublicSignKey,
		options?: AddOperationOptions<Operation<IdentityRelation>>
	) {
		/*  trustee = PublicKey.from(trustee); */
		await this.relationGraph.put(
			new IdentityRelation({
				to: to,
				from:
					options?.identity?.publicKey ||
					this.relationGraph.store.identity.publicKey,
			}),
			options
		);
	}
}

/**
 * Not shardeable since we can not query trusted relations, because this would lead to a recursive problem where we then need to determine whether the responder is trusted or not
 */

@variant("trusted_network")
export class TrustedNetwork extends Program {
	@field({ type: PublicSignKey })
	rootTrust: PublicSignKey;

	@field({ type: Documents })
	trustGraph: Documents<IdentityRelation>;

	@field({ type: LogIndex })
	logIndex: LogIndex;

	constructor(props?: {
		id?: string;
		rootTrust: PublicSignKey;
		logIndex?: LogIndex;
	}) {
		super(props);
		if (props) {
			this.trustGraph = createIdentityGraphStore({
				...props,
				id: this.id,
			});
			this.rootTrust = props.rootTrust;
			this.logIndex = props.logIndex || new LogIndex({ query: new RPC() });
		}
	}

	async setup() {
		await this.trustGraph.setup({
			type: IdentityRelation,
			canAppend: this.canAppend.bind(this),
			canRead: this.canRead.bind(this),
		}); // self referencing access controller
		return this.logIndex.setup({
			store: this.trustGraph.store,
			context: this,
		});
	}

	async canAppend(entry: Entry<Operation<IdentityRelation>>): Promise<boolean> {
		return canAppendByRelation(entry, (key) => this.isTrusted(key));
	}

	async canRead(key?: PublicSignKey): Promise<boolean> {
		if (!key) {
			return false;
		}
		return this.isTrusted(key);
	}

	async add(
		trustee: PublicSignKey | PeerIdAddress | PeerId
	): Promise<IdentityRelation | undefined> {
		let key: PublicSignKey | PeerIdAddress;
		if (
			trustee instanceof PublicSignKey === false &&
			trustee instanceof PeerIdAddress === false
		) {
			key = new PeerIdAddress({ address: trustee.toString() });
		} else {
			key = trustee as PublicSignKey | PeerIdAddress;
		}

		const existingRelation = this.getRelation(
			key,
			this.trustGraph.store.identity.publicKey
		);
		if (!existingRelation) {
			const relation = new IdentityRelation({
				to: key,
				from: this.trustGraph.store.identity.publicKey,
			});
			await this.trustGraph.put(relation);
			return relation;
		}
		return existingRelation.value;
	}

	hasRelation(trustee: PublicSignKey, truster = this.rootTrust) {
		return !!this.getRelation(trustee, truster);
	}
	getRelation(
		trustee: PublicSignKey | PeerIdAddress,
		truster = this.rootTrust
	) {
		return getRelation(
			truster,
			trustee instanceof PublicSignKey
				? trustee
				: new PeerIdAddress({ address: trustee.toString() }),
			this.trustGraph
		);
	}

	/**
	 * Follow trust path back to trust root.
	 * Trust root is always trusted.
	 * Hence if
	 * Root trust A trust B trust C
	 * C is trusted by Root
	 * @param trustee
	 * @param truster, the truster "root", if undefined defaults to the root trust
	 * @returns true, if trusted
	 */
	async isTrusted(
		trustee: PublicSignKey | PeerIdAddress,
		truster: PublicSignKey = this.rootTrust,
		options?: { timeout: number }
	): Promise<boolean> {
		if (trustee.equals(this.rootTrust)) {
			return true;
		}
		if (this.trustGraph.replicate) {
			return this._isTrustedLocal(trustee, truster);
		} else {
			let trusted = false;
			let stopper: (() => any) | any;
			this.logIndex.query.send(
				new LogQueryRequest({ queries: [] }),
				async (heads, from) => {
					if (!from) {
						return;
					}

					await this.trustGraph.store.sync(heads.heads, {
						canAppend: () => Promise.resolve(true),
						save: true,
					});

					const isTrustedSender = await this._isTrustedLocal(from, truster);
					if (!isTrustedSender) {
						return;
					}

					const isTrustedTrustee = await this._isTrustedLocal(trustee, truster);
					if (isTrustedTrustee) {
						stopper && stopper();
						trusted = true;
					}
				},
				{
					stopper: (s) => {
						stopper = s;
					},
					timeout: options?.timeout || 10 * 1000,
				}
			);
			try {
				await waitFor(() => trusted);
				return trusted;
			} catch (error) {
				return false;
			}
		}
	}

	async _isTrustedLocal(
		trustee: PublicSignKey | PeerIdAddress,
		truster: PublicSignKey = this.rootTrust
	): Promise<boolean> {
		const trustPath = await hasPath(
			trustee,
			truster,
			this.trustGraph,
			getFromByTo
		);
		return !!trustPath;
	}

	async getTrusted(): Promise<PublicSignKey[]> {
		const current = this.rootTrust;
		const participants: PublicSignKey[] = [current];
		const generator = getPathGenerator(current, this.trustGraph, getToByFrom);
		for await (const next of generator) {
			participants.push(next.to);
		}
		return participants;
	}

	hashCode(): string {
		return sodium.crypto_generichash(32, serialize(this), null, "hex");
	}
}
