import { AbstractType, field, option, variant, vec } from "@dao-xyz/borsh";
import { Program } from "./program";
import {
	And,
	BoolQuery,
	ByteMatchQuery,
	Documents,
	MissingField,
	Or,
	PutOperation,
	Query,
	SearchRequest,
	Sort,
	SortDirection,
	StateFieldQuery,
	StringMatch
} from "../../data/document/src";
import {
	EncryptedThing,
	EncryptedSecp256k1Keypair,
	EncryptedX25519Keypair,
	EncryptedEd25519Keypair,
	EncryptedKeypair,
	DecryptedThing,
	createLocalEncryptProvider,
	X25519Keypair,
	X25519PublicKey,
	createDecrypterFromKeyResolver,
	KeyResolverReturnType,
	Ed25519PublicKey,
	Secp256k1PublicKey,
	Identity,
	EncryptProvide,
	KeyExchangeOptions,
	DecryptProvider,
	Ed25519Keypair,
	PublicSignKey,
	Secp256k1Keypair,
	KeyResolver,
	PublicKeyEncryptionKey
} from "@peerbit/crypto";
import { compare } from "@peerbit/uint8arrays";

@variant(0)
class EncryptedExtendedKey {
	// TODO: The owner of the key determined by wallet address or public sign key?
	@field({ type: PublicSignKey })
	owner: PublicSignKey;
	// Is this key revoked? HAS TO BE TRUE if recipient is set.
	@field({ type: "bool" })
	revoked: boolean;
	// Provide a single-use key for someone else
	@field({ type: option(PublicSignKey) })
	recipient?: PublicSignKey;
	// TODO: add the actual key here
	@field({ type: EncryptedKeypair })
	keypair:
		| EncryptedX25519Keypair
		| EncryptedEd25519Keypair
		| EncryptedSecp256k1Keypair;

	constructor(parameters: {
		owner: PublicSignKey;
		keypair:
			| EncryptedX25519Keypair
			| EncryptedEd25519Keypair
			| EncryptedSecp256k1Keypair;
		revoked?: boolean;
		recipient?: PublicSignKey
	}) {
		this.keypair = parameters.keypair;
		this.owner = parameters.owner;
		this.revoked = parameters.revoked ?? false;
		this.recipient = parameters.recipient
	}
}

type L1EncryptKeyResolver = (owner: PublicSignKey) => Promise<X25519PublicKey>

export class KeychainProgram extends Program {
	@field({ type: Documents<EncryptedExtendedKey> })
	keys: Documents<EncryptedExtendedKey>;

	@field({ type: Identity })
	identity: Identity;

	@field({ type: EncryptProvide })
	l1EncryptProvider: EncryptProvide<KeyExchangeOptions>;

	// Should resolve l1 sign keys to l1 public keys
	@field({type: L1EncryptKeyResolver})
	l1EncryptKeyResolver: L1EncryptKeyResolver

	@field({ type: DecryptProvider })
	l1DecryptProvider: DecryptProvider;

	// TODO: Offer rotate keys functionality. revokes all unrevoked keys and adds a new key which it returns
	
	
	// TODO: Write Key updater that either adds a given key or generates a new one.
	// TODO: the key updater should always set the revoked flag for keys with a recipient

	async updateKey(parameters: {keypair?: X25519Keypair | Ed25519Keypair | Secp256k1Keypair, recipient?: PublicSignKey, revoked?: boolean} = {}) {
		const {keypair = await X25519Keypair.create(), recipient, revoked} = parameters
		// We assume owner is identity here.
		// Generate regular keypair if I am the owner or the recipient
		// Otherwise generate a temporary key pair
		const recipientIsMe = recipient == undefined || recipient.equals(this.identity.publicKey) 
		// for whom are we encrypting? Me (owner) and the specified recipient.
		const encryptedExtendedKey = new EncryptedExtendedKey({
			owner: this.identity.publicKey,
			keypair: await keypair.encrypt(this.l1EncryptProvider, {type: "publicKey", receiverPublicKeys: [await this.l1EncryptKeyResolver(this.identity.publicKey), ...(recipientIsMe ? [] : [await this.l1EncryptKeyResolver(recipient)])]}), 
			revoked: revoked || !recipientIsMe,
			recipient: recipient
		})
		this.keys.put(encryptedExtendedKey, { signers: [this.identity.sign] })
	}

	// TODO: Write a key getter that get's my latest key. If no valid key is found, run rotate and return newly created
	// TODO: Write a key getter that get's the latest key's of recipients. If no valid key is found for a recipient, add a new, temporary key for the recipient.
	// TODO: Write a key getter that get's a specified key, no matter the revoked status
	async getKey(
		parameters:
			| { owner?: PublicSignKey; publicKey?: never }
			| {
					owner?: never;
					publicKey?: X25519PublicKey | Ed25519PublicKey | Secp256k1PublicKey;
			  } = {}
	) {
		const { owner = this.identity.publicKey, publicKey } = parameters;
		// Create Filter options
		const queries =
			// Looking for specific public keys. We don't care about revocation status.
			publicKey != undefined
				? [
						new ByteMatchQuery({
							key: ["keypair", "publicKey", "publicKey"],
							value: publicKey.publicKey
						})
				  ]
				: // Looking for latest key from a specific owner.
				  [
						new And([
							// Only non-revoked
							new BoolQuery({ key: "revoked", value: false }),
							// Recipient has to be undefined
							// TODO: Is this correct?
							new MissingField({ key: "recipient" }),
							// And from the specified owner
							// TODO: fix this query to compare PublicSignKey
							new ByteMatchQuery({
								key: "owner",
								value: owner
							})
						])
				  ];
		// TODO: How can we limit this to only fetch the last entry - so a single one?
		const keys = await this.keys.index.search(
			new SearchRequest({
				query: queries,
				sort: new Sort({
					key: "timestamp",
					direction: SortDirection.DESC
				})
			})
		);
		// Create a new key in case we can't find a match
		// If owner is not me, create a new, temporary key with me as owner and recipient as the requested owner
		// If owner is me, create a new permanent key with
		if (keys.length === 0) {
			return this.identity.publicKey.equals(owner) ? 
		}
		return keys[0].keypair.decrypt(this.l1DecryptProvider);
	}

	// TODO: Unsure about encryptProvider here honestly - should this be layer 1 keychain?
	async open(parameters: {
		identity: Identity;
		l1EncryptProvider: EncryptProvide<KeyExchangeOptions>;
		l1DecryptProvider: DecryptProvider;
	}) {
		this.identity = parameters.identity;
		this.l1EncryptProvider = parameters.l1EncryptProvider;
		this.l1DecryptProvider = parameters.l1DecryptProvider;
		// TODO: This should be opened with the layer 1 encryption provider
		await this.keys.open({
			type: EncryptedExtendedKey,
			canPerform: async (operation, { entry }) => {
				// Sender can put, identified by masterkey
				const owner = (
					operation instanceof PutOperation
						? operation.value
						: await this.keys.index.get(operation.key)
				)?.owner;
				for (const signer of entry.signatures) {
					if (signer.publicKey.equals(owner)) return true;
				}
				return false;
			},
			index: {
				// TODO: not sure about this
				key: ["keyPair", "publicKey"],
				// -> I want to be able to search .pub and .owner properties
				fields: async (doc, context) => {
					{
						return {
							...doc,
							timestamp: context.created
						};
					}
				},
				canRead: () => true,
				canSearch: () => true
			},
			// TODO: Potentially unsafe, as untrusted nodes might hide entries
			canReplicate: () => true
		});
	}

	async encrypt<T>(plainText: T, recipients: Uint8Array[]) {
		if (recipients.length === 0) throw new Error("No recipients specified");
		// TODO: Get and Feed my own key into this.
		const myEncryptionKeypair = await X25519Keypair.create();
		const encryptionProvider = createLocalEncryptProvider(myEncryptionKeypair);
		// TODO: Get and Feed recipient keys into .encrypt
		const recipientPublicKeys = await Promise.all([
			X25519PublicKey.create(),
			X25519PublicKey.create()
		]);
		return new DecryptedThing({ value: plainText }).encrypt(
			encryptionProvider,
			{ type: "publicKey", receiverPublicKeys: recipientPublicKeys }
		);
	}

	async decrypt<T>(encryptedThing: EncryptedThing<T>, clazz: AbstractType<T>) {
		const keyResolver = async <PublicKey extends X25519PublicKey | Uint8Array>(
			key: X25519PublicKey | Uint8Array
		) => {
			// TODO: fetch keys here based on key parameter and return them
			if (key instanceof X25519PublicKey)
				return X25519Keypair.create() as Promise<
					KeyResolverReturnType<PublicKey>
				>;
			return undefined;
		};
		const decryptionProvider = createDecrypterFromKeyResolver(keyResolver);
		const decryptedThing = await encryptedThing.decrypt(decryptionProvider);
		return decryptedThing.getValue(clazz);
	}
}
