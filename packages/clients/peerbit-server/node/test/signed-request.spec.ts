import { Ed25519Keypair } from "@peerbit/crypto";
import { signRequest, verifyRequest } from "../src/signed-request.js";
import { expect } from "chai";

describe("signed-request", () => {
	let signedRequest: {
		headers: Record<string, string>;
		method: string;
		data: string;
		url: string;
	};
	const data = "hello";
	let keypair: Ed25519Keypair;
	beforeEach(async () => {
		keypair = await Ed25519Keypair.create();
		signedRequest = {
			data,
			headers: {},
			method: "POST",
			url: "https://example.com/hello"
		};
		await signRequest(
			signedRequest.headers,
			signedRequest.method,
			new URL(signedRequest.url).pathname,
			data,
			keypair
		);
	});

	it("verifies", async () => {
		expect(
			(
				await verifyRequest(
					signedRequest.headers,
					signedRequest.method,
					new URL(signedRequest.url).pathname,
					data
				)
			).equals(keypair.publicKey)
		).to.be.true;
	});

	it("invalid time", async () => {
		signedRequest.headers["X-Peerbit-Signature-Time"] = String(
			Number(signedRequest.headers["X-Peerbit-Signature-Time"] as string) + 1
		);
		await expect(
			verifyRequest(
				signedRequest.headers,
				signedRequest.method,
				new URL(signedRequest.url).pathname,
				data
			)
		).rejectedWith("Invalid signature");
	});

	it("invalid method", async () => {
		await expect(
			verifyRequest(
				signedRequest.headers,
				"?",
				new URL(signedRequest.url).pathname,
				data
			)
		).rejectedWith("Invalid signature");
	});

	it("invalid url", async () => {
		await expect(
			verifyRequest(signedRequest.headers, signedRequest.method, "?", data)
		).rejectedWith("Invalid signature");
	});

	it("invalid data", async () => {
		await expect(
			verifyRequest(
				signedRequest.headers,
				signedRequest.method,
				new URL(signedRequest.url).pathname,
				"bye"
			)
		).rejectedWith("Invalid signature");
	});
});
