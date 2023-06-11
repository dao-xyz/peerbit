import { deserialize, field, serialize, variant, vec } from "@dao-xyz/borsh";
import {
	SearchRequest,
	Documents,
	Operation,
	PutOperation,
} from "@dao-xyz/peerbit-document";
import { AppendOptions, Entry } from "@dao-xyz/peerbit-log";
import { PublicSignKey } from "@dao-xyz/peerbit-crypto";
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
import { Program, ReplicatorType } from "@dao-xyz/peerbit-program";
import { CanRead } from "@dao-xyz/peerbit-rpc";
import { sha256Base64Sync } from "@dao-xyz/peerbit-crypto";

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

	constructor(props?: { relationGraph?: Documents<IdentityRelation> }) {
		super();
		if (props) {
			this.relationGraph = props.relationGraph || createIdentityGraphStore();
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
			index: {
				fields: (obj, _entry) => {
					return {
						from: obj.from.hashcode(),
						to: obj.to.hashcode(),
					};
				},
			},
		}); // self referencing access controller
	}

	async addRelation(
		to: PublicSignKey,
		options?: AppendOptions<Operation<IdentityRelation>>
	) {
		/*  trustee = PublicKey.from(trustee); */
		await this.relationGraph.put(
			new IdentityRelation({
				to: to,
				from:
					options?.identity?.publicKey ||
					this.relationGraph.log.identity.publicKey,
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

	constructor(props: { rootTrust: PublicSignKey }) {
		super();
		if (props) {
			this.trustGraph = createIdentityGraphStore();
			this.rootTrust = props.rootTrust;
		}
	}

	async setup() {
		await this.trustGraph.setup({
			type: IdentityRelation,
			canAppend: this.canAppend.bind(this),
			canRead: this.canRead.bind(this),
			index: {
				fields: (obj, _entry) => {
					return {
						from: obj.from.hashcode(),
						to: obj.to.hashcode(),
					};
				},
			},
		}); // self referencing access controller
	}

	async canAppend(entry: Entry<Operation<IdentityRelation>>): Promise<boolean> {
		return canAppendByRelation(entry, (key) => this.isTrusted(key));
	}

	async canRead(_key?: PublicSignKey): Promise<boolean> {
		return true; // TODO should we have read access control?
	}

	async add(trustee: PublicSignKey): Promise<IdentityRelation | undefined> {
		const key = trustee as PublicSignKey;

		const existingRelation = await this.getRelation(
			key,
			this.trustGraph.log.identity.publicKey
		);
		if (!existingRelation) {
			const relation = new IdentityRelation({
				to: key,
				from: this.trustGraph.log.identity.publicKey,
			});
			await this.trustGraph.put(relation);
			return relation;
		}
		return existingRelation;
	}

	async hasRelation(trustee: PublicSignKey, truster = this.rootTrust) {
		return !!(await this.getRelation(trustee, truster));
	}
	getRelation(trustee: PublicSignKey, truster = this.rootTrust) {
		return getRelation(truster, trustee, this.trustGraph);
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
		trustee: PublicSignKey,
		truster: PublicSignKey = this.rootTrust
	): Promise<boolean> {
		if (trustee.equals(this.rootTrust)) {
			return true;
		}
		if (this.trustGraph.role instanceof ReplicatorType) {
			return this._isTrustedLocal(trustee, truster);
		} else {
			this.trustGraph.index.query(new SearchRequest({ queries: [] }), {
				remote: { sync: true },
			});
			return this._isTrustedLocal(trustee, truster);
		}
	}

	async _isTrustedLocal(
		trustee: PublicSignKey,
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
		return sha256Base64Sync(serialize(this));
	}
}
