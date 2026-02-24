// Polyfill Mocha-style globals some legacy compiled tests still reference
// so we can migrate incrementally without editing build artifacts.
import { afterAll, afterEach, beforeAll, beforeEach } from "vitest";

// Only assign if not already defined (Vitest defines the *All variants)
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
if (!(globalThis as any).before) (globalThis as any).before = beforeAll;
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
if (!(globalThis as any).after) (globalThis as any).after = afterAll;
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
if (!(globalThis as any).beforeEach)
	(globalThis as any).beforeEach = beforeEach;
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
if (!(globalThis as any).afterEach) (globalThis as any).afterEach = afterEach;

// Optional: silence unhandled rejection warnings during migration (can remove later)
process.on("unhandledRejection", (e) => {
	// eslint-disable-next-line no-console
	console.warn(
		"[vitest.setup] Unhandled rejection (test will surface explicit failures separately):",
		e,
	);
});

// Basic EventTarget polyfill for Node tests needing CustomEvent dispatch/listen
// We only implement the tiny subset used (addEventListener/removeEventListener/dispatchEvent with type + detail)
if (typeof (globalThis as any).addEventListener !== "function") {
	const listeners: Record<string, Set<Function>> = {};
	(globalThis as any).addEventListener = (type: string, cb: any) => {
		(listeners[type] ||= new Set()).add(cb);
	};
	(globalThis as any).removeEventListener = (type: string, cb: any) => {
		listeners[type]?.delete(cb);
	};
	(globalThis as any).dispatchEvent = (evt: {
		type?: string;
		detail?: any;
	}) => {
		const type = (evt as any).type;
		if (!type) return true;
		for (const cb of listeners[type] || []) {
			try {
				cb({ detail: (evt as any).detail });
			} catch (err) {
				// eslint-disable-next-line no-console
				console.warn("[vitest.setup] listener error for", type, err);
			}
		}
		return true;
	};
	// Minimal CustomEvent polyfill if missing
	if (typeof (globalThis as any).CustomEvent !== "function") {
		(globalThis as any).CustomEvent = class CustomEvent<T = any> {
			type: string;
			detail: T;
			constructor(type: string, init: { detail?: T } = {}) {
				this.type = type;
				this.detail = init.detail as T;
			}
		} as any;
	}
}

// Optional: debug why a worker won't exit (e.g. active handles / timers)
if (process.env.VITEST_DEBUG_HANDLES) {
	afterAll(() => {
		const handles = (process as any)._getActiveHandles?.() ?? [];
		const requests = (process as any)._getActiveRequests?.() ?? [];
		const counts: Record<string, number> = {};
		const details: any[] = [];
		for (const h of handles) {
			const name = h?.constructor?.name ?? typeof h;
			counts[name] = (counts[name] ?? 0) + 1;
			if (name === "Socket") {
				details.push({
					type: name,
					localAddress: (h as any).localAddress,
					localPort: (h as any).localPort,
					remoteAddress: (h as any).remoteAddress,
					remotePort: (h as any).remotePort,
					bytesRead: (h as any).bytesRead,
					bytesWritten: (h as any).bytesWritten,
					destroyed: (h as any).destroyed,
				});
			} else if (name === "Pipe") {
				details.push({
					type: name,
					fd: (h as any).fd,
				});
			}
		}
		// eslint-disable-next-line no-console
		console.log(
			"[vitest.setup] debug handles",
			counts,
			"requests",
			requests.length,
			"mem",
			(process as any).memoryUsage?.(),
			"details",
			details,
		);

		// Periodic memory probe (unref so it doesn't keep the worker alive).
		try {
			const interval = setInterval(() => {
				// eslint-disable-next-line no-console
				console.log(
					"[vitest.setup] mem tick",
					(process as any).memoryUsage?.(),
				);
			}, 10_000);
			(interval as any).unref?.();
		} catch {
			// ignore
		}
	});
}
