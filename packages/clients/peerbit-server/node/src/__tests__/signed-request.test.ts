import { Ed25519Keypair } from "@peerbit/crypto";
import { signRequest, verifyRequest } from "../signes-request";

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
			url: "https://example.com/hello",
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
		).toBeTrue();
	});

	it("invalid time", async () => {
		signedRequest.headers["X-Peerbit-Signature-Time"] = String(
			Number(signedRequest.headers["X-Peerbit-Signature-Time"] as string) + 1
		);
		await expect(() =>
			verifyRequest(
				signedRequest.headers,
				signedRequest.method,
				new URL(signedRequest.url).pathname,
				data
			)
		).rejects.toThrowError("Invalid signature");
	});

	it("invalid method", async () => {
		await expect(() =>
			verifyRequest(
				signedRequest.headers,
				"?",
				new URL(signedRequest.url).pathname,
				data
			)
		).rejects.toThrowError("Invalid signature");
	});

	it("invalid url", async () => {
		await expect(() =>
			verifyRequest(signedRequest.headers, signedRequest.method, "?", data)
		).rejects.toThrowError("Invalid signature");
	});

	it("invalid data", async () => {
		await expect(() =>
			verifyRequest(
				signedRequest.headers,
				signedRequest.method,
				new URL(signedRequest.url).pathname,
				"bye"
			)
		).rejects.toThrowError("Invalid signature");
	});
});
