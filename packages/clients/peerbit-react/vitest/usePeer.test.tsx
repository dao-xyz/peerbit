// @vitest-environment jsdom

import React from "react";
import ReactDOM from "react-dom/client";
import { act } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => {
	return {
		create: vi.fn(),
		getBootstrapPeerId: vi.fn((address: string) =>
			address.includes("/p2p/12D3KooW-bootstrap")
				? "12D3KooW-bootstrap"
				: address.includes("/p2p/12D3KooW-unrelated")
					? "12D3KooW-unrelated"
					: undefined,
		),
		resolveBootstrapAddresses: vi.fn(async () => [
			"/dns4/bootstrap.peerbit.org/tcp/4003/wss/p2p/12D3KooW-bootstrap",
		]),
		privateKeyFromRaw: vi.fn(() => ({})),
		noise: vi.fn(() => ({})),
		yamux: vi.fn(() => ({})),
		webSockets: vi.fn(() => ({})),
	};
});

vi.mock("detectincognitojs", () => ({
	detectIncognito: vi.fn(async () => ({ isPrivate: false })),
}));

vi.mock("libsodium-wrappers", () => ({
	default: { ready: Promise.resolve() },
	ready: Promise.resolve(),
}));

vi.mock("peerbit", () => ({
	Peerbit: {
		create: mocks.create,
	},
	getBootstrapPeerId: mocks.getBootstrapPeerId,
	resolveBootstrapAddresses: mocks.resolveBootstrapAddresses,
}));

vi.mock("@chainsafe/libp2p-noise", () => ({
	noise: mocks.noise,
}));

vi.mock("@chainsafe/libp2p-yamux", () => ({
	yamux: mocks.yamux,
}));

vi.mock("@libp2p/websockets", () => ({
	webSockets: mocks.webSockets,
}));

vi.mock("@libp2p/crypto", () => ({
	keys: {
		privateKeyFromRaw: mocks.privateKeyFromRaw,
	},
}));

import { PeerProvider, usePeer } from "../src/usePeer.tsx";

const waitFor = async (predicate: () => boolean, timeout = 5_000) => {
	const started = Date.now();
	while (!predicate()) {
		if (Date.now() - started > timeout) {
			throw new Error("Timed out waiting for condition");
		}
		await new Promise((resolve) => setTimeout(resolve, 10));
	}
};

const createFakeKeypair = () =>
	({
		privateKeyPublicKey: new Uint8Array(64),
		toPeerId: () => ({
			toString: () => "12D3KooW-test",
		}),
	}) as any;

const createPeerInstance = (options?: {
	bootstrap?: () => Promise<
		void | {
			connectedPeerIds?: string[];
			failures?: { peerId?: string; label: string[]; reason: string }[];
		}
	>;
	stop?: () => Promise<void>;
	connectionPeers?: string[];
}) =>
	({
		peerId: { toString: () => "12D3KooW-test" },
		identity: {
			publicKey: {
				hashcode: () => "peer-hash",
			},
		},
		getMultiaddrs: () => [],
		dial: async () => true,
		hangUp: async () => undefined,
		start: async () => undefined,
		stop: options?.stop ?? (async () => undefined),
		bootstrap: options?.bootstrap,
		open: async () => {
			throw new Error("not used in test");
		},
		libp2p: {
			getDialQueue: () => [],
			getConnections: () =>
				(options?.connectionPeers ?? []).map((peerId) => ({
					remotePeer: { toString: () => peerId },
				})),
		},
	}) as any;

const StatusView = () => {
	const { status, loading, warning } = usePeer();
	return (
		<div data-testid="status">
			{loading
				? `loading:${status}`
				: `${status}${warning ? `|warning:${warning.failures.length}` : ""}`}
		</div>
	);
};

describe("PeerProvider bootstrap handling", () => {
	let container: HTMLDivElement;
	let root: ReactDOM.Root;
	let consoleError: ReturnType<typeof vi.spyOn>;
	let consoleWarn: ReturnType<typeof vi.spyOn>;

	beforeEach(() => {
		container = document.createElement("div");
		document.body.appendChild(container);
		root = ReactDOM.createRoot(container);
		consoleError = vi.spyOn(console, "error").mockImplementation(() => {});
		consoleWarn = vi.spyOn(console, "warn").mockImplementation(() => {});
		mocks.create.mockReset();
	});

	afterEach(async () => {
		await act(async () => {
			root.unmount();
		});
		container.remove();
		consoleError.mockRestore();
		consoleWarn.mockRestore();
	});

	it("ignores bootstrap failures after unmount", async () => {
		const stop = vi.fn(async () => undefined);
		mocks.create.mockResolvedValue(
				createPeerInstance({
					stop,
					connectionPeers: [],
					bootstrap: () =>
						new Promise<void>((_, reject) => {
							setTimeout(() => reject(new Error("stale bootstrap failure")), 20);
					}),
			}),
		);

		await act(async () => {
			root.render(
				<PeerProvider
					config={{
						runtime: "node",
						network: "remote",
						waitForConnected: true,
						inMemory: true,
						keypair: createFakeKeypair(),
					}}
				>
					<StatusView />
				</PeerProvider>,
			);
		});

		await act(async () => {
			root.unmount();
		});

		await waitFor(() => stop.mock.calls.length > 0);
		await new Promise((resolve) => setTimeout(resolve, 30));

		expect(consoleError).not.toHaveBeenCalledWith(
			"Failed to bootstrap:",
			expect.anything(),
		);
	});

	it("keeps the peer connected when bootstrap fails after a connection exists", async () => {
		mocks.create.mockResolvedValue(
			createPeerInstance({
				connectionPeers: ["12D3KooW-bootstrap"],
				bootstrap: () =>
					new Promise<void>((_, reject) => {
						setTimeout(
							() => reject(new Error("bootstrap failed after connect")),
							20,
						);
					}),
			}),
		);

		await act(async () => {
			root.render(
				<PeerProvider
					config={{
						runtime: "node",
						network: "remote",
						waitForConnected: true,
						inMemory: true,
						keypair: createFakeKeypair(),
					}}
				>
					<StatusView />
				</PeerProvider>,
			);
		});

		await waitFor(
			() => container.querySelector("[data-testid='status']")?.textContent === "connected",
		);

			expect(consoleError).not.toHaveBeenCalledWith(
				"Failed to bootstrap:",
				expect.anything(),
			);
		});

		it("surfaces bootstrap failure when only non-bootstrap peers are connected", async () => {
			mocks.create.mockResolvedValue(
				createPeerInstance({
					connectionPeers: ["12D3KooW-unrelated"],
					bootstrap: () =>
						new Promise<void>((_, reject) => {
							setTimeout(() => reject(new Error("bootstrap peer unavailable")), 20);
						}),
				}),
			);

			await act(async () => {
				root.render(
					<PeerProvider
						config={{
							runtime: "node",
							network: "remote",
							waitForConnected: true,
							inMemory: true,
							keypair: createFakeKeypair(),
						}}
					>
						<StatusView />
					</PeerProvider>,
				);
			});

			await waitFor(
				() => container.querySelector("[data-testid='status']")?.textContent === "failed",
			);

		expect(consoleError).toHaveBeenCalledWith(
			"Failed to bootstrap:",
			expect.any(Error),
		);
	});

	it("surfaces partial bootstrap failures as a non-blocking warning", async () => {
		mocks.create.mockResolvedValue(
			createPeerInstance({
				connectionPeers: ["12D3KooW-bootstrap"],
				bootstrap: async () => ({
					connectedPeerIds: ["12D3KooW-bootstrap"],
					failures: [
						{
							peerId: "12D3KooW-other",
							label: ["/dns4/other.peer/tcp/4003/wss/p2p/12D3KooW-other"],
							reason: "dial timeout",
						},
					],
				}),
			}),
		);

		await act(async () => {
			root.render(
				<PeerProvider
					config={{
						runtime: "node",
						network: "remote",
						waitForConnected: true,
						inMemory: true,
						keypair: createFakeKeypair(),
					}}
				>
					<StatusView />
				</PeerProvider>,
			);
		});

		await waitFor(
			() =>
				container.querySelector("[data-testid='status']")?.textContent ===
				"connected|warning:1",
		);

		expect(consoleError).not.toHaveBeenCalledWith(
			"Failed to bootstrap:",
			expect.anything(),
		);
		expect(consoleWarn).toHaveBeenCalledWith(
			expect.stringContaining("Connected to 1 bootstrap peer"),
			expect.arrayContaining([expect.stringContaining("dial timeout")]),
		);
	});
	});
