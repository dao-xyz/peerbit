import { act, render, waitFor } from "@testing-library/react";
import React, { StrictMode } from "react";
import { describe, expect, it, vi } from "vitest";
import { useProgram } from "../src/useProgram.js";

class CountingEventTarget extends EventTarget {
	readonly added = new Map<string, number>();
	readonly removed = new Map<string, number>();
	readonly listeners = new Map<
		string,
		Set<EventListenerOrEventListenerObject>
	>();

	addEventListener(
		type: string,
		callback: EventListenerOrEventListenerObject | null,
		options?: AddEventListenerOptions | boolean,
	) {
		if (callback) {
			this.added.set(type, (this.added.get(type) ?? 0) + 1);
			const listeners = this.listeners.get(type) ?? new Set();
			listeners.add(callback);
			this.listeners.set(type, listeners);
		}
		super.addEventListener(type, callback, options);
	}

	removeEventListener(
		type: string,
		callback: EventListenerOrEventListenerObject | null,
		options?: EventListenerOptions | boolean,
	) {
		if (callback) {
			this.removed.set(type, (this.removed.get(type) ?? 0) + 1);
			this.listeners.get(type)?.delete(callback);
		}
		super.removeEventListener(type, callback, options);
	}

	listenerCount(type: string) {
		return this.listeners.get(type)?.size ?? 0;
	}
}

class ThrowingEventTarget extends CountingEventTarget {
	addEventListener(
		type: string,
		callback: EventListenerOrEventListenerObject | null,
		options?: AddEventListenerOptions | boolean,
	) {
		if (type === "leave") {
			throw new Error("listener setup failed");
		}
		super.addEventListener(type, callback, options);
	}
}

class FakeProgram {
	private _address?: string;
	closed = false;
	allPrograms: any[] = [];
	events = new CountingEventTarget();
	closeCalls = 0;
	closeFailures = 0;
	readonly topics: string[];

	constructor(address?: string, topics: string[] = []) {
		this._address = address;
		this.topics = topics;
	}
	get address() {
		if (!this._address) {
			throw new Error("address is not available until open");
		}
		return this._address;
	}
	set address(address: string) {
		this._address = address;
	}
	getTopics() {
		return this.topics;
	}
	async getReady() {
		return new Map();
	}
	async close() {
		this.closeCalls += 1;
		if (this.closeFailures > 0) {
			this.closeFailures -= 1;
			throw new Error("close failed");
		}
		this.closed = true;
	}
}

const deferred = <T,>() => {
	let resolve!: (value: T) => void;
	let reject!: (reason?: unknown) => void;
	const promise = new Promise<T>((res, rej) => {
		resolve = res;
		reject = rej;
	});
	return { promise, resolve, reject };
};

describe("useProgram hook", () => {
	it("opens program via peer context and exposes peers list", async () => {
		const program = new FakeProgram("dummy-address");
		const fakePeer = {
			identity: { publicKey: { hashcode: () => "peer-1" } },
			open: async () => program,
		};

		let latest: ReturnType<typeof useProgram<any>> | undefined;
		const Wrapper = () => {
			latest = useProgram(fakePeer as any, "dummy-address");
			return null;
		};

		render(<Wrapper />);

		await waitFor(() => expect(latest?.program).toBeDefined());
		expect(latest?.program).toEqual(program);
		expect(latest?.peers).toEqual([fakePeer.identity.publicKey]);
		expect(latest?.loading).toBe(false);
		expect(latest?.status).toBe("ready");
		expect(latest?.error).toBeUndefined();
	});

	it("keeps fresh address-equivalent object targets on the same request", async () => {
		const renderedTargets: FakeProgram[] = [];
		const fakePeer = {
			identity: { publicKey: { hashcode: () => "peer-1" } },
			open: vi.fn(async (target: FakeProgram) => target),
		};

		let latest: ReturnType<typeof useProgram<any>> | undefined;
		const Wrapper = () => {
			const target = new FakeProgram("shared-address");
			renderedTargets.push(target);
			latest = useProgram(fakePeer as any, target as any);
			return null;
		};
		const view = render(<Wrapper />);
		await waitFor(() => expect(latest?.program).toBeDefined());
		const opened = latest!.program as FakeProgram;

		view.rerender(<Wrapper />);
		await act(async () => {
			for (let i = 0; i < 5; i += 1) await Promise.resolve();
		});

		expect(renderedTargets.length).toBeGreaterThan(1);
		expect(fakePeer.open).toHaveBeenCalledTimes(1);
		expect(latest?.program).toBe(opened);
		expect(opened.closeCalls).toBe(0);
	});

	it("keeps fresh inline unsaved targets on the legacy shared request", async () => {
		const renderedTargets: FakeProgram[] = [];
		const fakePeer = {
			identity: { publicKey: { hashcode: () => "peer-1" } },
			open: vi.fn(async (target: FakeProgram) => {
				target.address = "assigned-during-open";
				return target;
			}),
		};

		let latest: ReturnType<typeof useProgram<any>> | undefined;
		const Wrapper = () => {
			const target = new FakeProgram();
			renderedTargets.push(target);
			latest = useProgram(fakePeer as any, target as any);
			return null;
		};
		const view = render(<Wrapper />);
		await waitFor(() => expect(latest?.program).toBeDefined());
		const opened = latest!.program as FakeProgram;

		view.rerender(<Wrapper />);
		await act(async () => {
			for (let i = 0; i < 5; i += 1) await Promise.resolve();
		});

		expect(renderedTargets.length).toBeGreaterThan(1);
		expect(fakePeer.open).toHaveBeenCalledTimes(1);
		expect(latest?.program).toBe(opened);
		expect(opened.closeCalls).toBe(0);
	});

	it("keeps an unsaved object target on its identity after open assigns its address", async () => {
		const target = new FakeProgram();
		const fakePeer = {
			identity: { publicKey: { hashcode: () => "peer-1" } },
			open: vi.fn(async (program: FakeProgram) => {
				program.address = "assigned-during-open";
				return program;
			}),
		};

		let latest: ReturnType<typeof useProgram<any>> | undefined;
		const Wrapper = () => {
			latest = useProgram(fakePeer as any, target as any);
			return null;
		};
		const view = render(<Wrapper />);
		await waitFor(() => expect(latest?.program).toBe(target));

		view.rerender(<Wrapper />);
		await act(async () => {
			for (let i = 0; i < 5; i += 1) await Promise.resolve();
		});

		expect(fakePeer.open).toHaveBeenCalledTimes(1);
		expect(target.closeCalls).toBe(0);
	});

	it("is idle (not loading) when peer is missing", async () => {
		let latest: ReturnType<typeof useProgram<any>> | undefined;
		const Wrapper = () => {
			latest = useProgram(undefined, "dummy-address");
			return null;
		};

		render(<Wrapper />);

		await waitFor(() => expect(latest?.status).toBe("idle"));
		expect(latest?.loading).toBe(false);
		expect(latest?.program).toBeUndefined();
	});

	it("reports error when open fails", async () => {
		const fakePeer = {
			identity: { publicKey: { hashcode: () => "peer-1" } },
			open: async () => {
				throw new Error("open failed");
			},
		};

		let latest: ReturnType<typeof useProgram<any>> | undefined;
		const Wrapper = () => {
			latest = useProgram(fakePeer as any, "dummy-address");
			return null;
		};

		render(<Wrapper />);

		await waitFor(() => expect(latest?.status).toBe("error"));
		expect(latest?.loading).toBe(false);
		expect(latest?.program).toBeUndefined();
		expect(latest?.error?.message).toBe("open failed");
	});

	it("contains a synchronous getReady failure and still cleans up before replacement", async () => {
		const first = new FakeProgram("first", ["topic"]);
		first.getReady = () => {
			throw new Error("getReady failed synchronously");
		};
		const second = new FakeProgram("second");
		const fakePeer = {
			identity: { publicKey: { hashcode: () => "peer-1" } },
			open: vi.fn(async (target: FakeProgram) => target),
		};
		const consoleLog = vi.spyOn(console, "log").mockImplementation(() => {});

		try {
			let target = first;
			let latest: ReturnType<typeof useProgram<any>> | undefined;
			const Wrapper = () => {
				latest = useProgram(fakePeer as any, target as any);
				return null;
			};
			const view = render(<Wrapper />);
			await waitFor(() => expect(latest?.program).toBe(first));
			await waitFor(() => expect(consoleLog).toHaveBeenCalledTimes(1));

			target = second;
			view.rerender(<Wrapper />);
			await waitFor(() => expect(first.closeCalls).toBe(1));
			await waitFor(() => expect(latest?.program).toBe(second));

			expect(first.events.listenerCount("join")).toBe(0);
			expect(first.events.listenerCount("leave")).toBe(0);
			expect(fakePeer.open).toHaveBeenCalledTimes(2);
		} finally {
			consoleLog.mockRestore();
		}
	});

	it("settles an unexpected post-open setup exception so cleanup cannot block replacements", async () => {
		const first = new FakeProgram("first", ["topic"]);
		first.events = new ThrowingEventTarget();
		const second = new FakeProgram("second");
		const fakePeer = {
			identity: { publicKey: { hashcode: () => "peer-1" } },
			open: vi.fn(async (target: FakeProgram) => target),
		};
		const consoleError = vi
			.spyOn(console, "error")
			.mockImplementation(() => {});

		try {
			let target = first;
			let latest: ReturnType<typeof useProgram<any>> | undefined;
			const Wrapper = () => {
				latest = useProgram(fakePeer as any, target as any);
				return null;
			};
			const view = render(<Wrapper />);
			await waitFor(() => expect(latest?.status).toBe("error"));
			expect(latest?.error?.message).toBe("listener setup failed");
			const publicPromise = latest?.promise;
			expect(publicPromise).toBeDefined();
			await expect(publicPromise).resolves.toBeUndefined();

			target = second;
			view.rerender(<Wrapper />);
			await waitFor(() => expect(first.closeCalls).toBe(1));
			await waitFor(() => expect(latest?.program).toBe(second));

			expect(first.events.listenerCount("join")).toBe(0);
			expect(fakePeer.open).toHaveBeenCalledTimes(2);
		} finally {
			consoleError.mockRestore();
		}
	});

	it("keeps StrictMode opens owned across target changes and pending unmount cleanup", async () => {
		const first = new FakeProgram(undefined, ["topic"]);
		const second = new FakeProgram(undefined, ["topic"]);
		const third = new FakeProgram(undefined, ["topic"]);
		const gates = new Map([
			[first, deferred<FakeProgram>()],
			[second, deferred<FakeProgram>()],
			[third, deferred<FakeProgram>()],
		]);
		const fakePeer = {
			identity: { publicKey: { hashcode: () => "peer-1" } },
			open: vi.fn(async (target: FakeProgram) => {
				const opened = await gates.get(target)!.promise;
				target.address =
					target === first ? "first" : target === second ? "second" : "third";
				return opened;
			}),
		};

		let latest: ReturnType<typeof useProgram<any>> | undefined;
		let target = first;
		let requestId = "first";
		const Wrapper = () => {
			latest = useProgram(fakePeer as any, target as any, { id: requestId });
			return null;
		};
		const view = render(
			<StrictMode>
				<Wrapper />
			</StrictMode>,
		);

		await waitFor(() => expect(fakePeer.open).toHaveBeenCalledTimes(1));
		expect(latest?.session).toBe(0);

		target = second;
		requestId = "second";
		view.rerender(
			<StrictMode>
				<Wrapper />
			</StrictMode>,
		);
		expect(latest?.program).toBeUndefined();

		await act(async () => gates.get(first)!.resolve(first));
		await waitFor(() => expect(first.closeCalls).toBe(1));
		await waitFor(() => expect(fakePeer.open).toHaveBeenCalledTimes(2));
		expect(first.events.added.size).toBe(0);
		expect(latest?.session).toBe(0);

		await act(async () => gates.get(second)!.resolve(second));
		await waitFor(() => expect(latest?.program).toBe(second));
		expect(latest?.session).toBe(1);
		expect(second.events.listenerCount("join")).toBe(1);
		expect(second.events.listenerCount("leave")).toBe(1);

		// Assigning the address during open must not turn the same object target into
		// a new request and cause a redundant close/reopen cycle.
		view.rerender(
			<StrictMode>
				<Wrapper />
			</StrictMode>,
		);
		await Promise.resolve();
		expect(fakePeer.open).toHaveBeenCalledTimes(2);
		expect(second.closeCalls).toBe(0);

		target = third;
		requestId = "third";
		view.rerender(
			<StrictMode>
				<Wrapper />
			</StrictMode>,
		);
		expect(latest?.program).toBeUndefined();
		await waitFor(() => expect(second.closeCalls).toBe(1));
		await waitFor(() => expect(fakePeer.open).toHaveBeenCalledTimes(3));
		expect(second.events.listenerCount("join")).toBe(0);
		expect(second.events.listenerCount("leave")).toBe(0);

		view.unmount();
		await act(async () => gates.get(third)!.resolve(third));
		await waitFor(() => expect(third.closeCalls).toBe(1));
		expect(fakePeer.open).toHaveBeenCalledTimes(3);
		expect(third.events.added.size).toBe(0);
		expect(second.events.added.get("join")).toBe(1);
		expect(second.events.added.get("leave")).toBe(1);
		expect(second.events.removed.get("join")).toBe(1);
		expect(second.events.removed.get("leave")).toBe(1);
		expect(latest?.session).toBe(1);
	});

	it("ignores a stale open rejection after the committed request changes", async () => {
		const first = new FakeProgram("first");
		const second = new FakeProgram("second");
		const firstGate = deferred<FakeProgram>();
		const fakePeer = {
			identity: { publicKey: { hashcode: () => "peer-1" } },
			open: vi.fn((target: FakeProgram) =>
				target === first ? firstGate.promise : Promise.resolve(second),
			),
		};

		let target = first;
		let latest: ReturnType<typeof useProgram<any>> | undefined;
		const Wrapper = () => {
			latest = useProgram(fakePeer as any, target as any);
			return null;
		};
		const view = render(<Wrapper />);
		await waitFor(() => expect(fakePeer.open).toHaveBeenCalledTimes(1));

		target = second;
		view.rerender(<Wrapper />);
		await act(async () => firstGate.reject(new Error("stale open failed")));
		await waitFor(() => expect(latest?.program).toBe(second));

		expect(fakePeer.open).toHaveBeenCalledTimes(2);
		expect(latest?.error).toBeUndefined();
		expect(latest?.status).toBe("ready");
		expect(latest?.session).toBe(1);
	});

	it("does not expose a stale program while an equal-identity peer reopens the same target", async () => {
		const target = new FakeProgram("target");
		const firstProgram = new FakeProgram("target");
		const secondProgram = new FakeProgram("target");
		const secondGate = deferred<FakeProgram>();
		const publicKey = { hashcode: () => "same-peer-identity" };
		const firstPeer = {
			identity: { publicKey },
			open: vi.fn(async () => firstProgram),
		};
		const secondPeer = {
			identity: { publicKey },
			open: vi.fn(async () => secondGate.promise),
		};

		let peer = firstPeer;
		let latest: ReturnType<typeof useProgram<any>> | undefined;
		const Wrapper = () => {
			latest = useProgram(peer as any, target as any);
			return null;
		};
		const view = render(<Wrapper />);

		await waitFor(() => expect(latest?.program).toBe(firstProgram));
		expect(latest?.session).toBe(1);

		peer = secondPeer;
		view.rerender(<Wrapper />);
		expect(latest?.program).toBeUndefined();
		await waitFor(() => expect(firstProgram.closeCalls).toBe(1));
		await waitFor(() => expect(secondPeer.open).toHaveBeenCalledTimes(1));
		expect(latest?.session).toBe(1);

		await act(async () => secondGate.resolve(secondProgram));
		await waitFor(() => expect(latest?.program).toBe(secondProgram));
		expect(latest?.session).toBe(2);
	});

	it("preserves explicit same-target reopen requests", async () => {
		const target = new FakeProgram("target");
		const firstProgram = new FakeProgram("target");
		const secondProgram = new FakeProgram("target");
		const fakePeer = {
			identity: { publicKey: { hashcode: () => "peer-1" } },
			open: vi
				.fn()
				.mockResolvedValueOnce(firstProgram)
				.mockResolvedValueOnce(secondProgram),
		};

		let id = "first-request";
		let latest: ReturnType<typeof useProgram<any>> | undefined;
		const Wrapper = () => {
			latest = useProgram(fakePeer as any, target as any, { id });
			return null;
		};
		const view = render(<Wrapper />);
		await waitFor(() => expect(latest?.program).toBe(firstProgram));

		id = "second-request";
		view.rerender(<Wrapper />);
		expect(latest?.program).toBeUndefined();
		await waitFor(() => expect(firstProgram.closeCalls).toBe(1));
		await waitFor(() => expect(latest?.program).toBe(secondProgram));

		expect(fakePeer.open).toHaveBeenCalledTimes(2);
		expect(latest?.session).toBe(2);
	});

	it("retries transient close cleanup before opening the replacement", async () => {
		const first = new FakeProgram("first");
		first.closeFailures = 1;
		const second = new FakeProgram("second");
		const fakePeer = {
			identity: { publicKey: { hashcode: () => "peer-1" } },
			open: vi.fn(async (target: FakeProgram) => target),
		};

		let target = first;
		let latest: ReturnType<typeof useProgram<any>> | undefined;
		const Wrapper = () => {
			latest = useProgram(fakePeer as any, target as any);
			return null;
		};
		const view = render(<Wrapper />);
		await waitFor(() => expect(latest?.program).toBe(first));

		target = second;
		view.rerender(<Wrapper />);
		await waitFor(() => expect(first.closeCalls).toBe(2));
		await waitFor(() => expect(latest?.program).toBe(second));
		expect(fakePeer.open).toHaveBeenCalledTimes(2);
	});

	it("hides the prior session immediately when keepOpenOnUnmount changes", async () => {
		const target = new FakeProgram("target");
		const firstProgram = new FakeProgram("target");
		const secondProgram = new FakeProgram("target");
		const fakePeer = {
			identity: { publicKey: { hashcode: () => "peer-1" } },
			open: vi
				.fn()
				.mockResolvedValueOnce(firstProgram)
				.mockResolvedValueOnce(secondProgram),
		};

		let keepOpenOnUnmount = false;
		let latest: ReturnType<typeof useProgram<any>> | undefined;
		const Wrapper = () => {
			latest = useProgram(fakePeer as any, target as any, {
				keepOpenOnUnmount,
			});
			return null;
		};
		const view = render(<Wrapper />);
		await waitFor(() => expect(latest?.program).toBe(firstProgram));

		keepOpenOnUnmount = true;
		view.rerender(<Wrapper />);
		expect(latest?.program).toBeUndefined();
		await waitFor(() => expect(firstProgram.closeCalls).toBe(1));
		await waitFor(() => expect(latest?.program).toBe(secondProgram));

		view.unmount();
		await act(async () => {
			for (let i = 0; i < 5; i += 1) await Promise.resolve();
		});
		expect(fakePeer.open).toHaveBeenCalledTimes(2);
		expect(secondProgram.closeCalls).toBe(0);
	});

	it("detaches listeners without closing when keepOpenOnUnmount is set", async () => {
		const program = new FakeProgram("target", ["topic"]);
		const fakePeer = {
			identity: { publicKey: { hashcode: () => "peer-1" } },
			open: vi.fn(async () => program),
		};

		let latest: ReturnType<typeof useProgram<any>> | undefined;
		const Wrapper = () => {
			latest = useProgram(fakePeer as any, program as any, {
				keepOpenOnUnmount: true,
			});
			return null;
		};
		const view = render(<Wrapper />);
		await waitFor(() => expect(latest?.program).toBe(program));
		expect(program.events.listenerCount("join")).toBe(1);
		expect(program.events.listenerCount("leave")).toBe(1);

		view.unmount();
		await waitFor(() => expect(program.events.listenerCount("join")).toBe(0));
		expect(program.events.listenerCount("leave")).toBe(0);
		expect(program.closeCalls).toBe(0);
	});
});
