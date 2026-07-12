// @vitest-environment jsdom

import React from "react";
import ReactDOM from "react-dom/client";
import { act } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => {
	return {
		create: vi.fn(),
		detectIncognito: vi.fn(async () => ({ isPrivate: false })),
		persist: vi.fn(async () => false),
		getDirectory: vi.fn(async () => ({})),
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
	detectIncognito: mocks.detectIncognito,
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
			failures?: { peerId?: string; reason: string }[];
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
	let originalStorage: PropertyDescriptor | undefined;

	beforeEach(() => {
		container = document.createElement("div");
		document.body.appendChild(container);
		root = ReactDOM.createRoot(container);
		consoleError = vi.spyOn(console, "error").mockImplementation(() => {});
		consoleWarn = vi.spyOn(console, "warn").mockImplementation(() => {});
		mocks.create.mockReset();
		mocks.detectIncognito.mockReset();
		mocks.detectIncognito.mockResolvedValue({ isPrivate: false });
		mocks.persist.mockReset();
		mocks.persist.mockResolvedValue(false);
		mocks.getDirectory.mockReset();
		mocks.getDirectory.mockResolvedValue({});
		originalStorage = Object.getOwnPropertyDescriptor(navigator, "storage");
		Object.defineProperty(navigator, "storage", {
			configurable: true,
			value: {
				persist: mocks.persist,
				getDirectory: mocks.getDirectory,
			},
		});
	});

	afterEach(async () => {
		await act(async () => {
			root.unmount();
		});
		container.remove();
		consoleError.mockRestore();
		consoleWarn.mockRestore();
		if (originalStorage) {
			Object.defineProperty(navigator, "storage", originalStorage);
		} else {
			Reflect.deleteProperty(navigator, "storage");
		}
	});

	const renderStorageProvider = async (inMemory?: boolean) => {
		mocks.create.mockResolvedValue(createPeerInstance());

		await act(async () => {
			root.render(
				<PeerProvider
					config={{
						runtime: "node",
						network: { bootstrap: [] },
						waitForConnected: true,
						keypair: createFakeKeypair(),
						...(inMemory === undefined ? {} : { inMemory }),
					}}
				>
					<StatusView />
				</PeerProvider>,
			);
		});

		await waitFor(
			() =>
				container.querySelector("[data-testid='status']")?.textContent ===
				"connected",
		);
	};

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

	it("uses OPFS when eviction protection is denied", async () => {
		await renderStorageProvider();

		expect(mocks.persist).toHaveBeenCalledTimes(1);
		expect(mocks.getDirectory).toHaveBeenCalledTimes(1);
		expect(mocks.create).toHaveBeenCalledWith(
			expect.objectContaining({
				directory: "./repo/12D3KooW-test/",
			}),
		);
		expect(consoleWarn).toHaveBeenCalledWith(
			"Browser storage is not protected from eviction; continuing with OPFS-backed storage.",
		);
		expect((window as any).__peerInfo.persisted).toBe(false);
	});

	it("reports granted eviction protection while using OPFS", async () => {
		mocks.persist.mockResolvedValue(true);

		await renderStorageProvider();

		expect(mocks.getDirectory).toHaveBeenCalledTimes(1);
		expect(mocks.create).toHaveBeenCalledWith(
			expect.objectContaining({
				directory: "./repo/12D3KooW-test/",
			}),
		);
		expect(consoleWarn).not.toHaveBeenCalledWith(
			"Browser storage is not protected from eviction; continuing with OPFS-backed storage.",
		);
		expect((window as any).__peerInfo.persisted).toBe(true);
	});

	it("uses OPFS when requesting eviction protection throws", async () => {
		const error = new Error("storage permission failed");
		mocks.persist.mockRejectedValue(error);

		await renderStorageProvider();

		expect(mocks.create).toHaveBeenCalledWith(
			expect.objectContaining({
				directory: "./repo/12D3KooW-test/",
			}),
		);
		expect(consoleWarn).toHaveBeenCalledWith(
			"Failed to request protection from browser storage eviction.",
			error,
		);
		expect((window as any).__peerInfo.persisted).toBe(false);
	});

	it("uses OPFS when the persistence API is unavailable", async () => {
		Object.defineProperty(navigator, "storage", {
			configurable: true,
			value: { getDirectory: mocks.getDirectory },
		});

		await renderStorageProvider();

		expect(mocks.persist).not.toHaveBeenCalled();
		expect(mocks.getDirectory).toHaveBeenCalledTimes(1);
		expect(mocks.create).toHaveBeenCalledWith(
			expect.objectContaining({
				directory: "./repo/12D3KooW-test/",
			}),
		);
		expect((window as any).__peerInfo.persisted).toBe(false);
	});

	it("falls back to memory when OPFS cannot be opened", async () => {
		const error = new DOMException("OPFS denied", "SecurityError");
		mocks.getDirectory.mockRejectedValue(error);

		await renderStorageProvider();

		expect(mocks.create).toHaveBeenCalledWith(
			expect.objectContaining({ directory: undefined }),
		);
		expect(consoleError).toHaveBeenCalledWith(
			"Origin-private file system storage could not be opened; falling back to in-memory storage.",
			error,
		);
	});

	it("falls back to memory when OPFS is unavailable", async () => {
		Object.defineProperty(navigator, "storage", {
			configurable: true,
			value: { persist: mocks.persist },
		});

		await renderStorageProvider();

		expect(mocks.create).toHaveBeenCalledWith(
			expect.objectContaining({ directory: undefined }),
		);
		expect(consoleError).toHaveBeenCalledWith(
			"Origin-private file system storage is unavailable; falling back to in-memory storage.",
		);
	});

	it("keeps explicit in-memory mode in memory", async () => {
		await renderStorageProvider(true);

		expect(mocks.detectIncognito).not.toHaveBeenCalled();
		expect(mocks.persist).not.toHaveBeenCalled();
		expect(mocks.create).toHaveBeenCalledWith(
			expect.objectContaining({ directory: undefined }),
		);
	});

	it("keeps private browsing mode in memory", async () => {
		mocks.detectIncognito.mockResolvedValue({ isPrivate: true });

		await renderStorageProvider();

		expect(mocks.persist).not.toHaveBeenCalled();
		expect(mocks.getDirectory).not.toHaveBeenCalled();
		expect(mocks.create).toHaveBeenCalledWith(
			expect.objectContaining({ directory: undefined }),
		);
	});
	});
