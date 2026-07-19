import { Ed25519Keypair, toBase64 } from "@peerbit/crypto";
import { expect } from "chai";
import type http from "http";
import { PassThrough } from "stream";
import {
	ReplayCapacityError,
	ReplayRequestError,
	RequestAuthenticator,
	RequestBodyTooLargeError,
	SIGNATURE_TIME_KEY,
	SIGNATURE_VERSION_KEY,
	SignedRequestError,
	createAuthDescriptor,
	getBody,
	signRequest,
	verifyAuthDescriptor,
	verifyRequestBody,
} from "../src/signed-request.js";

describe("signed-request v2", () => {
	const nowMs = 1_750_000_000_000;
	const bootId = toBase64(new Uint8Array(32).fill(7));
	const audience = {
		serverPeerId: "12D3KooWTestServerIdentity",
		bootId,
	};
	const body = "hello 🌍";
	let keypair: Ed25519Keypair;
	let headers: Record<string, string>;

	const authenticator = (
		properties: Partial<
			ConstructorParameters<typeof RequestAuthenticator>[0]
		> = {},
	) =>
		new RequestAuthenticator({
			...audience,
			isTrusted: (key) => key.equals(keypair.publicKey),
			wallClockMs: () => nowMs,
			...properties,
		});

	beforeEach(async () => {
		keypair = await Ed25519Keypair.create();
		headers = {};
		await signRequest(
			headers,
			"POST",
			"/hello?Case=Kept",
			body,
			keypair,
			audience,
			{ nowMs, nonce: new Uint8Array(32).fill(3) },
		);
	});

	it("verifies an audience-bound request and its exact body", async () => {
		const verifier = authenticator();
		const verified = await verifier.verify(headers, "POST", "/hello?Case=Kept");
		expect(verified.publicKey.equals(keypair.publicKey)).to.equal(true);
		expect(
			verifyRequestBody(verified, new TextEncoder().encode(body)),
		).to.equal(body);
		verifier.consume(verified);
	});

	it("rejects an exact replay", async () => {
		const verifier = authenticator();
		const first = await verifier.verify(headers, "POST", "/hello?Case=Kept");
		verifyRequestBody(first, new TextEncoder().encode(body));
		verifier.consume(first);
		await expect(
			verifier.verify(headers, "POST", "/hello?Case=Kept"),
		).rejectedWith(ReplayRequestError);
	});

	it("atomically accepts only one concurrent duplicate", async () => {
		const verifier = authenticator();
		const verifyBodyAndConsume = () =>
			verifier.verify(headers, "POST", "/hello?Case=Kept").then((verified) => {
				verifyRequestBody(verified, new TextEncoder().encode(body));
				verifier.consume(verified);
			});
		const results = await Promise.allSettled([
			verifyBodyAndConsume(),
			verifyBodyAndConsume(),
		]);
		expect(
			results.filter((result) => result.status === "fulfilled"),
		).to.have.length(1);
		expect(
			results.filter((result) => result.status === "rejected"),
		).to.have.length(1);
	});

	it("preserves method, path, query, and case exactly", async () => {
		for (const [method, target] of [
			["GET", "/hello?Case=Kept"],
			["POST", "/Hello?Case=Kept"],
			["POST", "/hello?case=Kept"],
			["POST", "/hello?Case=kept"],
			["POST", "/hello?Case=Kept&extra=1"],
		] as const) {
			await expect(
				authenticator().verify(headers, method, target),
			).rejectedWith(SignedRequestError);
		}
	});

	it("rejects a request signed for another server or boot", async () => {
		await expect(
			authenticator({ serverPeerId: "another-server" }).verify(
				headers,
				"POST",
				"/hello?Case=Kept",
			),
		).rejectedWith("audience");
		await expect(
			authenticator({ bootId: toBase64(new Uint8Array(32).fill(8)) }).verify(
				headers,
				"POST",
				"/hello?Case=Kept",
			),
		).rejectedWith("audience");
	});

	it("strictly rejects malformed, stale, and future timestamps", async () => {
		for (const timestamp of ["", "01", "+1", "1.0", "1e3", " 1", "1 "]) {
			const changed = { ...headers, [SIGNATURE_TIME_KEY]: timestamp };
			await expect(
				authenticator().verify(changed, "POST", "/hello?Case=Kept"),
			).rejectedWith(SignedRequestError);
		}

		const stale: Record<string, string> = {};
		await signRequest(stale, "GET", "/programs", undefined, keypair, audience, {
			nowMs: nowMs - 301_000,
		});
		await expect(
			authenticator().verify(stale, "GET", "/programs"),
		).rejectedWith("outside the window");

		const future: Record<string, string> = {};
		await signRequest(
			future,
			"GET",
			"/programs",
			undefined,
			keypair,
			audience,
			{
				nowMs: nowMs + 61_000,
			},
		);
		await expect(
			authenticator().verify(future, "GET", "/programs"),
		).rejectedWith("outside the window");
	});

	it("rejects duplicate and unsupported version headers", async () => {
		await expect(
			authenticator().verify(
				{
					...headers,
					[SIGNATURE_VERSION_KEY.toLowerCase()]: "2",
				},
				"POST",
				"/hello?Case=Kept",
			),
		).rejectedWith("duplicate");
		await expect(
			authenticator().verify(
				{ ...headers, [SIGNATURE_VERSION_KEY]: "1" },
				"POST",
				"/hello?Case=Kept",
			),
		).rejectedWith("Unsupported");
	});

	it("fails closed when the replay cache is full", async () => {
		const verifier = authenticator({ maxReplayEntries: 1 });
		const first = await verifier.verify(headers, "POST", "/hello?Case=Kept");
		verifier.consume(first);
		const second: Record<string, string> = {};
		await signRequest(
			second,
			"GET",
			"/programs",
			undefined,
			keypair,
			audience,
			{
				nowMs,
				nonce: new Uint8Array(32).fill(4),
			},
		);
		await expect(verifier.verify(second, "GET", "/programs")).rejectedWith(
			ReplayCapacityError,
		);
	});

	it("does not allocate replay capacity for an untrusted signer", async () => {
		const stranger = await Ed25519Keypair.create();
		const untrusted: Record<string, string> = {};
		await signRequest(
			untrusted,
			"GET",
			"/programs",
			undefined,
			stranger,
			audience,
			{ nowMs },
		);
		const verifier = authenticator({ maxReplayEntries: 1 });
		await expect(verifier.verify(untrusted, "GET", "/programs")).rejectedWith(
			"not trusted",
		);
		const trusted = await verifier.verify(headers, "POST", "/hello?Case=Kept");
		verifier.consume(trusted);
	});

	it("expires cache entries without resurrecting timestamps after clock rollback", async () => {
		let wall = nowMs;
		let monotonic = 0;
		const verifier = authenticator({
			maxReplayEntries: 1,
			wallClockMs: () => wall,
			monotonicClockMs: () => monotonic,
		});
		const first = await verifier.verify(headers, "POST", "/hello?Case=Kept");
		verifier.consume(first);
		wall += 400_000;
		monotonic += 400_000;
		const fresh: Record<string, string> = {};
		await signRequest(fresh, "GET", "/programs", undefined, keypair, audience, {
			nowMs: wall,
			nonce: new Uint8Array(32).fill(5),
		});
		const verifiedFresh = await verifier.verify(fresh, "GET", "/programs");
		verifier.consume(verifiedFresh);
		wall = nowMs;
		await expect(
			verifier.verify(headers, "POST", "/hello?Case=Kept"),
		).rejectedWith("outside the window");
	});

	it("does not burn a nonce for a mismatched body", async () => {
		const verifier = authenticator();
		const verified = await verifier.verify(headers, "POST", "/hello?Case=Kept");
		expect(() =>
			verifyRequestBody(verified, new TextEncoder().encode("hello")),
		).to.throw("does not match");

		const retried = await verifier.verify(headers, "POST", "/hello?Case=Kept");
		expect(verifyRequestBody(retried, new TextEncoder().encode(body))).to.equal(
			body,
		);
		verifier.consume(retried);
	});

	it("rechecks freshness and trust immediately before consuming", async () => {
		let wallClockMs = nowMs;
		let trusted = true;
		const verifier = authenticator({
			wallClockMs: () => wallClockMs,
			isTrusted: () => trusted,
		});
		const verified = await verifier.verify(headers, "POST", "/hello?Case=Kept");
		verifyRequestBody(verified, new TextEncoder().encode(body));
		trusted = false;
		expect(() => verifier.consume(verified)).to.throw("not trusted");

		trusted = true;
		wallClockMs += 301_000;
		expect(() => verifier.consume(verified)).to.throw("outside the window");
	});

	it("signs only the bytes in an offset Uint8Array view", async () => {
		const backing = new TextEncoder().encode('xx{ "message": "hello 🌍" }\nyy');
		const view = backing.subarray(2, backing.length - 2);
		const viewHeaders: Record<string, string> = {};
		await signRequest(viewHeaders, "PUT", "/program", view, keypair, audience, {
			nowMs,
		});
		const verifier = authenticator();
		const verified = await verifier.verify(viewHeaders, "PUT", "/program");
		expect(verified.bodyLength).to.equal(view.byteLength);
		expect(verifyRequestBody(verified, view)).to.equal(
			'{ "message": "hello 🌍" }\n',
		);
		verifier.consume(verified);
	});

	it("rejects signed body bytes that are not valid UTF-8", async () => {
		const invalid = new Uint8Array([0xff]);
		const invalidHeaders: Record<string, string> = {};
		await signRequest(
			invalidHeaders,
			"PUT",
			"/program",
			invalid,
			keypair,
			audience,
			{ nowMs },
		);
		const verified = await authenticator().verify(
			invalidHeaders,
			"PUT",
			"/program",
		);
		expect(() => verifyRequestBody(verified, invalid)).to.throw("valid UTF-8");
	});

	it("collects exact raw bytes across UTF-8 chunk boundaries", async () => {
		const stream = new PassThrough();
		const received = getBody(stream as unknown as http.IncomingMessage);
		const expected = new TextEncoder().encode("before 🌍 after");
		stream.write(expected.subarray(0, 8));
		stream.write(expected.subarray(8, 9));
		stream.end(expected.subarray(9));
		expect(await received).to.deep.equal(expected);
	});

	it("rejects a streamed body that exceeds the configured bound", async () => {
		const stream = new PassThrough();
		const received = getBody(stream as unknown as http.IncomingMessage, 3);
		stream.write(new Uint8Array([1, 2]));
		stream.write(new Uint8Array([3, 4]));
		await expect(received).rejectedWith(RequestBodyTooLargeError);
		stream.end();
	});

	it("authenticates and pins the signed boot descriptor", async () => {
		const server = await Ed25519Keypair.create();
		const serverPeerId = server.publicKey.toPeerId().toString();
		const descriptor = await createAuthDescriptor(
			server,
			{ serverPeerId, bootId },
			nowMs,
		);
		expect(
			(await verifyAuthDescriptor(descriptor, serverPeerId, { nowMs }))
				.serverPeerId,
		).to.equal(serverPeerId);
		await expect(
			verifyAuthDescriptor(
				descriptor,
				keypair.publicKey.toPeerId().toString(),
				{
					nowMs,
				},
			),
		).rejectedWith("pinned ID");

		await expect(
			verifyAuthDescriptor(
				{ ...descriptor, bootId: toBase64(new Uint8Array(32).fill(8)) },
				serverPeerId,
				{ nowMs },
			),
		).rejectedWith("signature");
		await expect(
			verifyAuthDescriptor({ ...descriptor, serverTime: "01" }, serverPeerId, {
				nowMs,
			}),
		).rejectedWith("server time");
	});

	it("rejects stale and future signed authentication descriptors", async () => {
		const server = await Ed25519Keypair.create();
		const serverPeerId = server.publicKey.toPeerId().toString();
		const stale = await createAuthDescriptor(
			server,
			{ serverPeerId, bootId },
			nowMs - 301_000,
		);
		await expect(
			verifyAuthDescriptor(stale, serverPeerId, { nowMs }),
		).rejectedWith("outside the window");

		const future = await createAuthDescriptor(
			server,
			{ serverPeerId, bootId },
			nowMs + 61_000,
		);
		await expect(
			verifyAuthDescriptor(future, serverPeerId, { nowMs }),
		).rejectedWith("outside the window");
	});
});
