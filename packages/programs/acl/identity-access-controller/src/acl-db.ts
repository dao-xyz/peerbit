import { field, variant } from "@dao-xyz/borsh";
import { type PeerId } from "@libp2p/interface";
import { PublicSignKey, sha256Sync } from "@peerbit/crypto";
import {
	type CanPerformOperations,
	Documents,
	type DocumentsLike,
} from "@peerbit/document";
import { Compare, IntegerCompare, Or, SearchRequest } from "@peerbit/document";
import { Program } from "@peerbit/program";
import { type ReplicationOptions } from "@peerbit/shared-log";
import {
	IdentityGraph,
	TrustedNetwork,
	createIdentityGraphStore,
	getFromByTo,
	getPathGenerator,
} from "@peerbit/trusted-network";
import { concat } from "uint8arrays";
import { ACCESS_TYPE_PROPERTY, Access, AccessType } from "./access.js";

const openDocumentsLike = async <T, I extends Record<string, any> = any>(
	owner: Program<any, any>,
	docs: DocumentsLike<T, I>,
	args: any,
): Promise<DocumentsLike<T, I>> => {
	if (!(docs instanceof Program)) {
		return docs;
	}
	const opened = await owner.node.open(docs as Documents<T, I>, {
		args,
		parent: owner as any,
		existing: "reuse",
	});
	if (opened instanceof Documents && !(opened as any)._clazz) {
		await opened.open(args);
	}
	return opened as DocumentsLike<T, I>;
};

@variant("identity_acl")
export class IdentityAccessController extends Program {
	@field({ type: Documents })
	access: DocumentsLike<Access, Access>;

	@field({ type: IdentityGraph })
	identityGraphController: IdentityGraph;

	@field({ type: TrustedNetwork })
	trustedNetwork: TrustedNetwork;

	constructor(opts: {
		id?: Uint8Array;
		rootTrust: PublicSignKey | PeerId;
		trustedNetwork?: TrustedNetwork;
	}) {
		super();
		if (!opts.trustedNetwork && !opts.rootTrust) {
			throw new Error("Expecting either TrustedNetwork or rootTrust");
		}
		this.access = new Documents({
			id: opts.id && sha256Sync(concat([opts.id, new Uint8Array([0])])),
		});

		this.trustedNetwork = opts.trustedNetwork
			? opts.trustedNetwork
			: new TrustedNetwork({
					id: opts.id && sha256Sync(concat([opts.id, new Uint8Array([1])])),
					rootTrust: opts.rootTrust,
				});
		this.identityGraphController = new IdentityGraph({
			relationGraph: createIdentityGraphStore(
				opts.id && sha256Sync(concat([opts.id, new Uint8Array([2])])),
			),
		});
	}

	// allow anyone write to the ACL db, but assume entry is invalid until a verifier verifies
	// can append will be anyone who has peformed some proof of work

	// or

	// custom can append

	async canRead(_obj: any, s: PublicSignKey | undefined): Promise<boolean> {
		// TODO, improve, caching etc

		if (!s) {
			return false;
		}

		// Check whether it is trusted by trust web
		if (await this.trustedNetwork.isTrusted(s)) {
			return true;
		}

		// Else check whether its trusted by this access controller
		const canReadCheck = async (key: PublicSignKey) => {
			const accessReadOrAny = await this.access.index.search(
				new SearchRequest({
					query: [
						new Or([
							new IntegerCompare({
								key: ACCESS_TYPE_PROPERTY,
								compare: Compare.Equal,
								value: AccessType.Any,
							}),
							new IntegerCompare({
								key: ACCESS_TYPE_PROPERTY,
								compare: Compare.Equal,
								value: AccessType.Read,
							}),
						]),
					],
				}),
				// Access control must be conservative and non-blocking.
				// Do not wait on remote discovery/replication here.
				{ remote: false, local: true } as any,
			);
			for (const access of accessReadOrAny) {
				if (access instanceof Access) {
					if (
						access.accessTypes.find(
							(x) => x === AccessType.Any || x === AccessType.Read,
						) !== undefined
					) {
						// check condition
						if (await access.accessCondition.allowed(key)) {
							return true;
						}
						continue;
					}
				}
			}
		};

		if (await canReadCheck(s)) {
			return true;
		}
		for await (const trustedByKey of getPathGenerator(
			s,
			this.identityGraphController.relationGraph,
			getFromByTo,
		)) {
			if (await canReadCheck(trustedByKey.from)) {
				return true;
			}
		}

		return false;
	}

	async canPerform(properties: CanPerformOperations<any>): Promise<boolean> {
		// TODO, improve, caching etc

		// Check whether it is trusted by trust web
		const canPerformByKey = async (key: PublicSignKey): Promise<boolean> => {
			if (await this.trustedNetwork.isTrusted(key)) {
				return true;
			}
			// Else check whether its trusted by this access controller
			const canPerformCheck = async (key: PublicSignKey) => {
				const accessWritedOrAny = await this.access.index.search(
					new SearchRequest({
						query: [
							new Or([
								new IntegerCompare({
									key: ACCESS_TYPE_PROPERTY,
									compare: Compare.Equal,
									value: AccessType.Any,
								}),
								new IntegerCompare({
									key: ACCESS_TYPE_PROPERTY,
									compare: Compare.Equal,
									value: AccessType.Write,
								}),
							]),
						],
					}),
					// Access control must be conservative and non-blocking.
					// Do not wait on remote discovery/replication here.
					{ remote: false, local: true } as any,
				);

				for (const access of accessWritedOrAny) {
					if (access instanceof Access) {
						if (
							access.accessTypes.find(
								(x) => x === AccessType.Any || x === AccessType.Write,
							) !== undefined
						) {
							// check condition
							if (await access.accessCondition.allowed(key)) {
								return true;
							}
							continue;
						}
					}
				}
			};
			if (await canPerformCheck(key)) {
				return true;
			}
			for await (const trustedByKey of getPathGenerator(
				key,
				this.identityGraphController.relationGraph,
				getFromByTo,
			)) {
				if (await canPerformCheck(trustedByKey.from)) {
					return true;
				}
			}

			return false;
		};

		for (const key of await properties.entry.getPublicKeys()) {
			if (await canPerformByKey(key)) {
				return true;
			}
		}
		return false;
	}

	async open(properties?: { replicate?: ReplicationOptions }) {
		await this.identityGraphController.open({
			replicate: properties?.replicate || { factor: 1 },
			canRead: this.canRead.bind(this),
		});
		this.access = await openDocumentsLike(this, this.access, {
			replicate: properties?.replicate || { factor: 1 },
			type: Access,
			canPerform: this.canPerform.bind(this),
			index: {
				canRead: () => true, // TODO set this correctly
			},
		});
		await this.trustedNetwork.open(properties);
	}
}
