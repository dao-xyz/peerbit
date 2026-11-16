import { Ed25519Keypair, type PublicSignKey } from "@peerbit/crypto";
import { type CanPerformOperations, Documents } from "@peerbit/document";
import { type Entry, EntryType } from "@peerbit/log";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import { equals } from "uint8arrays";
import { FromTo, IdentityRelation, TrustedNetwork } from "../src/index.js";

const deferred = <T = void>() => {
	let resolve!: (value: T) => void;
	const promise = new Promise<T>((fn) => (resolve = fn));
	return { promise, resolve };
};

const graphOf = (network: TrustedNetwork) =>
	network.trustGraph as Documents<IdentityRelation, FromTo>;

// The root trusts W, revokes W (the CUT D1 names h1 and removes it from the
// root's log), then trusts W again (h2, same relation id, next = []). A peer
// that admits D1 while it has no row for that id, and indexes h2 before D1 is
// applied, must keep h2.
const revokeAndRegrant = async (
	root: TrustedNetwork,
	trustee: PublicSignKey,
) => {
	await root.add(trustee);
	await root.revoke(trustee);
	const relation = await root.add(trustee);
	const heads = await graphOf(root).log.log.getHeads(true).all();
	expect(heads).to.have.length(2);
	const revoke = heads.find((entry) => entry.meta.type === EntryType.CUT)!;
	const regrant = heads.find((entry) => entry.meta.type !== EntryType.CUT)!;
	expect(revoke, "revoke CUT").to.exist;
	expect(regrant.meta.next).to.deep.equal([]);
	return { relation, revoke, regrant };
};

const addsRelation = (relation: IdentityRelation) => (event: CustomEvent) =>
	event.detail.added.some((added: IdentityRelation) =>
		equals(added.id, relation.id),
	);

describe("revoke and re-grant", () => {
	let session: TestSession | undefined;

	afterEach(async () => {
		await session?.stop();
		session = undefined;
	});

	it("keeps a re-grant indexed while an older revoke of the same relation is admitted", async () => {
		session = await TestSession.disconnected(2);
		const trustee = (await Ed25519Keypair.create()).publicKey;
		const root = await session.peers[0].open(
			new TrustedNetwork({ rootTrust: session.peers[0].identity.publicKey }),
		);
		const { relation, revoke, regrant } = await revokeAndRegrant(root, trustee);

		const cutEntered = deferred();
		const regrantIndexed = deferred();
		let injected: Promise<void> | undefined;
		const canPerform = TrustedNetwork.prototype.canPerform;
		const joiner = root.clone();
		// open() binds whatever canPerform the instance has.
		joiner.canPerform = async function (
			this: TrustedNetwork,
			properties: CanPerformOperations<IdentityRelation>,
		) {
			if (properties.type === "delete") {
				cutEntered.resolve();
				await regrantIndexed.promise;
			} else if (properties.value.to.equals(trustee)) {
				injected = graphOf(this).log.join([revoke as Entry<any>]);
				await cutEntered.promise;
			}
			return canPerform.call(this, properties);
		};
		await session.peers[1].open(joiner);
		const isRegrant = addsRelation(relation);
		graphOf(joiner).events.addEventListener("change", (event) => {
			if (isRegrant(event)) {
				regrantIndexed.resolve();
			}
		});

		await graphOf(joiner).log.join([regrant as Entry<any>]);
		expect(injected, "the re-grant's put reached the gate").to.be.instanceOf(
			Promise,
		);
		await injected;

		expect(await graphOf(joiner).log.log.has(revoke.hash)).to.be.true;
		expect(await graphOf(joiner).log.log.has(regrant.hash)).to.be.true;
		expect(
			await graphOf(joiner).index.get(relation.id, {
				local: true,
				remote: false,
			}),
		).to.exist;
		expect(await joiner.isTrusted(trustee)).to.be.true;
	});

	it("keeps a re-grant that sync delivers next to an older revoke of the same relation", async () => {
		session = await TestSession.connected(2);
		const trustee = (await Ed25519Keypair.create()).publicKey;
		const root = await session.peers[0].open(
			new TrustedNetwork({ rootTrust: session.peers[0].identity.publicKey }),
		);
		const { relation, revoke, regrant } = await revokeAndRegrant(root, trustee);

		const joinerKey = session.peers[1].identity.publicKey;
		const isRegrant = addsRelation(relation);
		const cutEntered = deferred();
		const regrantIndexed = deferred();
		const injected = deferred<Promise<void>>();
		let regrantStarted = false;
		const canPerform = TrustedNetwork.prototype.canPerform;
		// Only the joiner is gated. A revoke that sync delivers before the re-grant
		// passes through and is refused, as without the gate. The re-grant then
		// joins the revoke again and holds until it is being admitted; the revoke
		// holds until the re-grant is indexed. That straddle does not depend on
		// the order in which sync delivers the two entries.
		TrustedNetwork.prototype.canPerform = async function (
			this: TrustedNetwork,
			properties: CanPerformOperations<IdentityRelation>,
		) {
			if (!this.node.identity.publicKey.equals(joinerKey)) {
				return canPerform.call(this, properties);
			}
			if (properties.type === "put") {
				if (!regrantStarted && properties.value.to.equals(trustee)) {
					regrantStarted = true;
					graphOf(this).events.addEventListener("change", (event) => {
						if (isRegrant(event)) {
							regrantIndexed.resolve();
						}
					});
					injected.resolve(graphOf(this).log.join([revoke as Entry<any>]));
					await cutEntered.promise;
				}
			} else if (regrantStarted) {
				cutEntered.resolve();
				await regrantIndexed.promise;
			}
			return canPerform.call(this, properties);
		};
		let joiner: TrustedNetwork;
		try {
			joiner = await TrustedNetwork.open(root.address, session.peers[1]);
			const revokeJoin = await injected.promise;
			await revokeJoin;
		} finally {
			TrustedNetwork.prototype.canPerform = canPerform;
		}

		expect(await graphOf(joiner).log.log.has(revoke.hash)).to.be.true;
		expect(await graphOf(joiner).log.log.has(regrant.hash)).to.be.true;
		expect(
			await graphOf(joiner).index.get(relation.id, {
				local: true,
				remote: false,
			}),
		).to.exist;
		expect(await joiner.isTrusted(trustee)).to.be.true;
	});
});
