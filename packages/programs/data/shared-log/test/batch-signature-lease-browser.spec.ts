import type { Entry } from "@peerbit/log";
import { expect } from "chai";
import {
	type ReceiveSignatureVerificationFact,
	receiveSignatureVerificationResult,
	withReceiveSignatureVerificationFacts,
} from "../src/receive-signature-verification.js";

const deferred = () => {
	let resolve!: () => void;
	const promise = new Promise<void>((settle) => {
		resolve = settle;
	});
	return { promise, resolve };
};

describe("receive admission signature verification leases", () => {
	it("binds exact bytes and retains an overlapping same-entry lease", async () => {
		const factsByEntry = new WeakMap<
			Entry<unknown>,
			ReceiveSignatureVerificationFact<unknown>[]
		>();
		const verifiedBytes = new Uint8Array([1, 2, 3]);
		let currentBytes = verifiedBytes.slice();
		const entry = {
			getStorageBytes: () => currentBytes,
		} as Entry<unknown>;
		const makeFact = () => ({
			entry,
			storageBytes: verifiedBytes.slice(),
			verified: true,
		});
		const firstEntered = deferred();
		const firstRelease = deferred();
		const secondEntered = deferred();
		const secondRelease = deferred();

		const first = withReceiveSignatureVerificationFacts(
			factsByEntry,
			[makeFact()],
			async () => {
				firstEntered.resolve();
				await firstRelease.promise;
			},
		);
		await firstEntered.promise;
		const second = withReceiveSignatureVerificationFacts(
			factsByEntry,
			[makeFact()],
			async () => {
				secondEntered.resolve();
				await secondRelease.promise;
			},
		);

		try {
			await secondEntered.promise;
			expect(factsByEntry.get(entry)).to.have.length(2);
			expect(receiveSignatureVerificationResult(factsByEntry, entry)).to.equal(
				true,
			);

			currentBytes = new Uint8Array([1, 2, 4]);
			expect(receiveSignatureVerificationResult(factsByEntry, entry)).to.equal(
				undefined,
			);
			currentBytes = verifiedBytes.slice();

			firstRelease.resolve();
			await first;
			expect(factsByEntry.get(entry)).to.have.length(1);
			expect(receiveSignatureVerificationResult(factsByEntry, entry)).to.equal(
				true,
			);

			secondRelease.resolve();
			await second;
			expect(factsByEntry.has(entry)).to.equal(false);
			expect(receiveSignatureVerificationResult(factsByEntry, entry)).to.equal(
				undefined,
			);
		} finally {
			firstRelease.resolve();
			secondRelease.resolve();
			await Promise.allSettled([first, second]);
		}
	});
});
