import { field, serialize, variant } from "@dao-xyz/borsh";
import { type PeerId } from "@libp2p/interface";
import {
	PublicSignKey,
	getPublicKeyFromPeerId,
	sha256Base64Sync,
} from "@peerbit/crypto";
import {
	type CanPerformOperations,
	type CanRead,
	Documents,
	type Operation,
	SearchRequest,
} from "@peerbit/document";
import { type AppendOptions } from "@peerbit/log";
import { Program } from "@peerbit/program";
import { type ReplicationOptions } from "@peerbit/shared-log";
import {
	FromTo,
	IdentityRelation,
	createIdentityGraphStore,
	getFromByTo,
	getPathGenerator,
	getToByFrom,
	hasPath,
	getRelation as resolveRelation,
} from "./identity-graph.js";

const coercePublicKey = (publicKey: PublicSignKey | PeerId) => {
	return publicKey instanceof PublicSignKey
		? publicKey
		: getPublicKeyFromPeerId(publicKey);
};
const canPerformByRelation = async (
	properties: CanPerformOperations<IdentityRelation>,
	isTrusted?: (key: PublicSignKey) => Promise<boolean>,
): Promise<boolean> => {
	// verify the payload
	const keys = await properties.entry.getPublicKeys();
	const checkKey = async (key: PublicSignKey): Promise<boolean> => {
		if (properties.type === "put") {
			// TODO, this clause is only applicable when we modify the identityGraph, but it does not make sense that the canPerform method does not know what the payload will
			// be, upon deserialization. There should be known in the `canPerform` method whether we are appending to the identityGraph.

			const relation = properties.value;
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
};

type IdentityGraphArgs = {
	canRead?: CanRead<FromTo>;
	replicate?: ReplicationOptions;
};

@variant("relations")
export class IdentityGraph extends Program<IdentityGraphArgs> {
	@field({ type: Documents })
	relationGraph: Documents<IdentityRelation, FromTo>;

	constructor(props?: {
		id?: Uint8Array;
		relationGraph?: Documents<IdentityRelation, FromTo>;
	}) {
		super();
		if (props) {
			this.relationGraph =
				props.relationGraph || createIdentityGraphStore(props?.id);
		}
	}

	async canPerform(
		properties: CanPerformOperations<IdentityRelation>,
	): Promise<boolean> {
		return canPerformByRelation(properties);
	}

	async open(options?: IdentityGraphArgs) {
		await this.relationGraph.open({
			type: IdentityRelation,
			canPerform: this.canPerform.bind(this),
			replicate: options?.replicate,
			index: {
				canRead: options?.canRead,
				type: FromTo,
			},
		});
	}

	async addRelation(
		to: PublicSignKey | PeerId,
		options?: AppendOptions<Operation>,
	) {
		/*  trustee = PublicKey.from(trustee); */
		await this.relationGraph.put(
			new IdentityRelation({
				to: coercePublicKey(to),
				from: options?.identity?.publicKey || this.node.identity.publicKey,
			}),
			options,
		);
	}
}

/**
 * Not shardeable since we can not query trusted relations, because this would lead to a recursive problem where we then need to determine whether the responder is trusted or not
 */

type TrustedNetworkArgs = { replicate?: ReplicationOptions };

@variant("trusted_network")
export class TrustedNetwork extends Program<TrustedNetworkArgs> {
	@field({ type: PublicSignKey })
	rootTrust: PublicSignKey;

	@field({ type: Documents })
	trustGraph: Documents<IdentityRelation, FromTo>;

	constructor(props: { id?: Uint8Array; rootTrust: PublicSignKey | PeerId }) {
		super();
		this.rootTrust = coercePublicKey(props.rootTrust);
		this.trustGraph = createIdentityGraphStore();
	}

	async open(options?: TrustedNetworkArgs) {
		this.trustGraph = this.trustGraph || createIdentityGraphStore();
		await this.trustGraph.open({
			type: IdentityRelation,
			canPerform: this.canPerform.bind(this),
			replicate: options?.replicate || {
				factor: 1,
			},
			index: {
				canRead: this.canRead.bind(this),
				type: FromTo,
			},
		}); // self referencing access controller
	}

	async canPerform(
		properties: CanPerformOperations<IdentityRelation>,
	): Promise<boolean> {
		return canPerformByRelation(properties, (key) => this.isTrusted(key));
	}

	async canRead(relation: any, publicKey?: PublicSignKey): Promise<boolean> {
		return true; // TODO should we have read access control?
	}

	async add(
		trustee: PublicSignKey | PeerId,
		options?: AppendOptions<Operation>,
	) {
		const key =
			trustee instanceof PublicSignKey
				? trustee
				: getPublicKeyFromPeerId(trustee);

		const existingRelation = await this.getRelation(
			key,
			this.node.identity.publicKey,
		);
		if (!existingRelation) {
			const relation = new IdentityRelation({
				to: key,
				from: this.node.identity.publicKey,
			});
			await this.trustGraph!.put(relation);
			return relation;
		}
		return existingRelation;
	}

	async hasRelation(
		trustee: PublicSignKey | PeerId,
		truster: PublicSignKey | PeerId = this.rootTrust,
	) {
		return !!(await this.getRelation(trustee, truster));
	}
	getRelation(
		trustee: PublicSignKey | PeerId,
		truster: PublicSignKey | PeerId = this.rootTrust,
	) {
		return resolveRelation(
			coercePublicKey(trustee),
			coercePublicKey(truster),
			this.trustGraph!,
		);
	}

	/**
	 * Follow trust path back to trust root.
	 * Trust root is always trusted.
	 * Hence if
	 * Root trust A trust B trust C
	 * C is trusted by Root
	 * @param trustee
	 * @param truster the truster "root", if undefined defaults to the root trust
	 * @returns true, if trusted
	 */
	async isTrusted(
		trustee: PublicSignKey,
		truster: PublicSignKey = this.rootTrust,
	): Promise<boolean> {
		if (trustee.equals(this.rootTrust)) {
			return true;
		}
		if (await this.trustGraph.log.isReplicating()) {
			return this._isTrustedLocal(trustee, truster);
		} else {
			this.trustGraph.index.search(new SearchRequest({ query: [] }), {
				remote: { replicate: true },
			});
			return this._isTrustedLocal(trustee, truster);
		}
	}

	async _isTrustedLocal(
		trustee: PublicSignKey,
		truster: PublicSignKey = this.rootTrust,
	): Promise<boolean> {
		const trustPath = await hasPath(
			trustee,
			truster,
			this.trustGraph,
			getFromByTo,
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
/* TODO do we need these decorator functions? 
export const getNetwork = (object: any): TrustedNetwork | undefined => {
	return (
		object.constructor.prototype._network &&
		object[object.constructor.prototype._network]
	);
};

export function network(options: { property: string }) {
	return (constructor: any) => {
		constructor.prototype._network = options.property;
	};
}
*/
