import { createRangePlanner } from "@peerbit/shared-log-rust";

type BrowserResult = {
	nativePlannerActive: boolean;
	length: number;
	samples: Array<[string, { intersecting: boolean }]>;
};

declare global {
	interface Window {
		__sharedLogRustResult?: BrowserResult;
	}
}

const status = document.querySelector('[data-testid="status"]') as HTMLElement;

try {
	const planner = await createRangePlanner("u32");
	planner.put({
		id: "range-a",
		hash: "peer-a",
		timestamp: 0n,
		start1: 10,
		end1: 20,
		start2: 10,
		end2: 20,
		width: 10,
		mode: 0,
	});

	const native = (
		planner as unknown as {
			native?: { find_leaders?: unknown };
		}
	).native;
	window.__sharedLogRustResult = {
		nativePlannerActive: typeof native?.find_leaders === "function",
		length: planner.length,
		samples: [...planner.getSamples([15], { now: 1_000 })],
	};
	status.textContent = "native-ready";
} catch (error) {
	status.textContent = `error: ${
		error instanceof Error ? error.message : String(error)
	}`;
	throw error;
}
