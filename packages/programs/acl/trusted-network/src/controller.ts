import { deserialize, field, serialize, variant, vec } from "@dao-xyz/borsh";
import {
	SearchRequest,
	Documents,
	Operation,
	PutOperation,
	Replicator,
	Role,
} from "@peerbit/document";
import { AppendOptions, Entry } from "@peerbit/log";
import { PublicSignKey, getPublicKeyFromPeerId } from "@peerbit/crypto";
import { DeleteOperation } from "@peerbit/document";
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
import { Program } from "@peerbit/program";
import { CanRead } from "@peerbit/rpc";
import { sha256Base64Sync } from "@peerbit/crypto";
import { PeerId } from "@libp2p/interface-peer-id";

const coercePublicKey = (publicKey: PublicSignKey | PeerId) => {
	return publicKey instanceof PublicSignKey
		? publicKey
		: getPublicKeyFromPeerId(publicKey);
};
const canWriteByRelation = async (
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
				// TODO, this clause is only applicable when we modify the identityGraph, but it does not make sense that the canWrite method does not know what the payload will
				// be, upon deserialization. There should be known in the `canWrite` method whether we are appending to the identityGraph.

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

type IdentityGraphArgs = { canRead?: CanRead; role?: Role };
@variant("relations")
export class IdentityGraph extends Program<IdentityGraphArgs> {
	@field({ type: Documents })
	relationGraph: Documents<IdentityRelation>;

	constructor(props?: {
		id?: Uint8Array;
		relationGraph?: Documents<IdentityRelation>;
	}) {
		super();
		if (props) {
			this.relationGraph =
				props.relationGraph || createIdentityGraphStore(props?.id);
		}
	}

	async canWrite(entry: Entry<Operation<IdentityRelation>>): Promise<boolean> {
		return canWriteByRelation(entry);
	}

	async open(options?: IdentityGraphArgs) {
		await this.relationGraph.open({
			type: IdentityRelation,
			canWrite: this.canWrite.bind(this),
			canRead: options?.canRead,
			index: {
				fields: (obj, _entry) => {
					return {
						from: obj.from.hashcode(),
						to: obj.to.hashcode(),
					};
				},
			},
			role: options?.role,
		}); // self referencing access controller
	}

	async addRelation(
		to: PublicSignKey | PeerId,
		options?: AppendOptions<Operation<IdentityRelation>>
	) {
		/*  trustee = PublicKey.from(trustee); */
		await this.relationGraph.put(
			new IdentityRelation({
				to: coercePublicKey(to),
				from: options?.identity?.publicKey || this.node.identity.publicKey,
			}),
			options
		);
	}
}

/**
 * Not shardeable since we can not query trusted relations, because this would lead to a recursive problem where we then need to determine whether the responder is trusted or not
 */

type TrustedNetworkArgs = { role?: Role };
@variant("trusted_network")
export class TrustedNetwork extends Program<TrustedNetworkArgs> {
	@field({ type: PublicSignKey })
	rootTrust: PublicSignKey;

	@field({ type: Documents })
	trustGraph: Documents<IdentityRelation>;

	constructor(props: { id?: Uint8Array; rootTrust: PublicSignKey | PeerId }) {
		super();
		this.trustGraph = createIdentityGraphStore(props.id);
		this.rootTrust = coercePublicKey(props.rootTrust);
	}

	async open(options?: TrustedNetworkArgs) {
		await this.trustGraph.open({
			type: IdentityRelation,
			canWrite: this.canWrite.bind(this),
			canRead: this.canRead.bind(this),
			role: options?.role,
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

	async canWrite(entry: Entry<Operation<IdentityRelation>>): Promise<boolean> {
		return canWriteByRelation(entry, (key) => this.isTrusted(key));
	}

	async canRead(_key?: PublicSignKey): Promise<boolean> {
		return true; // TODO should we have read access control?
	}

	async add(
		trustee: PublicSignKey | PeerId
	): Promise<IdentityRelation | undefined> {
		const key =
			trustee instanceof PublicSignKey
				? trustee
				: getPublicKeyFromPeerId(trustee);

		const existingRelation = await this.getRelation(
			key,
			this.node.identity.publicKey
		);
		if (!existingRelation) {
			const relation = new IdentityRelation({
				to: key,
				from: this.node.identity.publicKey,
			});
			await this.trustGraph.put(relation);
			return relation;
		}
		return existingRelation;
	}

	async hasRelation(
		trustee: PublicSignKey | PeerId,
		truster: PublicSignKey | PeerId = this.rootTrust
	) {
		return !!(await this.getRelation(trustee, truster));
	}
	getRelation(
		trustee: PublicSignKey | PeerId,
		truster: PublicSignKey | PeerId = this.rootTrust
	) {
		return getRelation(
			coercePublicKey(truster),
			coercePublicKey(trustee),
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
		trustee: PublicSignKey,
		truster: PublicSignKey = this.rootTrust
	): Promise<boolean> {
		if (trustee.equals(this.rootTrust)) {
			return true;
		}
		if (this.trustGraph.log.role instanceof Replicator) {
			return this._isTrustedLocal(trustee, truster);
		} else {
			this.trustGraph.index.search(new SearchRequest({ query: [] }), {
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
