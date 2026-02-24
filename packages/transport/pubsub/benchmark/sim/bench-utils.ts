import { delay } from "@peerbit/time";

export const BENCH_ID_PREFIX = Uint8Array.from([0x50, 0x53, 0x49, 0x4d]); // "PSIM"

export const isBenchId = (id: Uint8Array) =>
	id.length === 32 &&
	id[0] === BENCH_ID_PREFIX[0] &&
	id[1] === BENCH_ID_PREFIX[1] &&
	id[2] === BENCH_ID_PREFIX[2] &&
	id[3] === BENCH_ID_PREFIX[3];

export const writeU32BE = (buf: Uint8Array, offset: number, value: number) => {
	buf[offset + 0] = (value >>> 24) & 0xff;
	buf[offset + 1] = (value >>> 16) & 0xff;
	buf[offset + 2] = (value >>> 8) & 0xff;
	buf[offset + 3] = value & 0xff;
};

export const readU32BE = (buf: Uint8Array, offset: number) =>
	((buf[offset + 0] << 24) |
		(buf[offset + 1] << 16) |
		(buf[offset + 2] << 8) |
		buf[offset + 3]) >>> 0;

export const mulberry32 = (seed: number) => {
	let t = seed >>> 0;
	return () => {
		t += 0x6d2b79f5;
		let x = t;
		x = Math.imul(x ^ (x >>> 15), x | 1);
		x ^= x + Math.imul(x ^ (x >>> 7), x | 61);
		return ((x ^ (x >>> 14)) >>> 0) / 4294967296;
	};
};

export const int = (rng: () => number, maxExclusive: number) =>
	Math.floor(rng() * maxExclusive);

export const quantile = (sorted: number[], q: number) => {
	if (sorted.length === 0) return NaN;
	const idx = Math.min(
		sorted.length - 1,
		Math.max(0, Math.floor(q * (sorted.length - 1))),
	);
	return sorted[idx]!;
};

export const buildRandomGraph = (
	n: number,
	targetDegree: number,
	rng: () => number,
): number[][] => {
	if (n <= 0) throw new Error("nodes must be > 0");
	if (targetDegree < 0) throw new Error("degree must be >= 0");
	if (targetDegree >= n) {
		throw new Error("degree must be < nodes for a simple graph");
	}

	const adj: Set<number>[] = Array.from({ length: n }, () => new Set<number>());
	const degree = new Uint16Array(n);

	const connect = (a: number, b: number) => {
		if (a === b) return false;
		if (adj[a]!.has(b)) return false;
		if (degree[a]! >= targetDegree || degree[b]! >= targetDegree) return false;
		adj[a]!.add(b);
		adj[b]!.add(a);
		degree[a]! += 1;
		degree[b]! += 1;
		return true;
	};

	// Seed connectivity.
	if (targetDegree >= 2 && n >= 3) {
		for (let i = 0; i < n; i++) connect(i, (i + 1) % n);
	} else if (targetDegree >= 1 && n >= 2) {
		for (let i = 0; i < n - 1; i++) connect(i, i + 1);
	}

	const available: number[] = [];
	const pos = new Int32Array(n).fill(-1);
	for (let i = 0; i < n; i++) {
		if (degree[i]! < targetDegree) {
			pos[i] = available.length;
			available.push(i);
		}
	}
	const removeAvailable = (id: number) => {
		const p = pos[id]!;
		if (p < 0) return;
		const last = available.pop()!;
		if (last !== id) {
			available[p] = last;
			pos[last] = p;
		}
		pos[id] = -1;
	};

	const maxAttempts = n * Math.max(1, targetDegree) * 200;
	let attempts = 0;
	while (available.length > 1 && attempts < maxAttempts) {
		attempts++;
		const a = available[int(rng, available.length)]!;
		const b = available[int(rng, available.length)]!;
		if (a === b) continue;
		if (!connect(a, b)) continue;
		if (degree[a]! >= targetDegree) removeAvailable(a);
		if (degree[b]! >= targetDegree) removeAvailable(b);
	}

	return adj.map((s) => [...s]);
};

export const runWithConcurrency = async <T>(
	tasks: Array<() => Promise<T>>,
	concurrency: number,
): Promise<T[]> => {
	const results: T[] = new Array(tasks.length);
	let index = 0;
	const workers = Array.from({ length: Math.max(1, concurrency) }, async () => {
		for (;;) {
			const i = index++;
			if (i >= tasks.length) return;
			results[i] = await tasks[i]!();
		}
	});
	await Promise.all(workers);
	return results;
};

type ProtocolNode = {
	multicodecs: string[];
	components: {
		connectionManager: {
			getConnections: () => Array<{
				streams: Array<{ protocol?: string; direction?: string }>;
			}>;
		};
	};
};

export const waitForProtocolStreams = async (
	nodes: ProtocolNode[],
	timeoutMs = 30_000,
) => {
	const start = Date.now();
	while (Date.now() - start < timeoutMs) {
		let missing = 0;
		for (const node of nodes) {
			const protocols = node.multicodecs;
			for (const conn of node.components.connectionManager.getConnections()) {
				const streams = conn.streams as Array<{
					protocol?: string;
					direction?: string;
				}>;
				const hasOutbound = streams.some(
					(s) =>
						s.protocol &&
						protocols.includes(s.protocol) &&
						s.direction === "outbound",
				);
				const hasInbound = streams.some(
					(s) =>
						s.protocol &&
						protocols.includes(s.protocol) &&
						s.direction === "inbound",
				);
				if (!hasOutbound || !hasInbound) missing++;
			}
		}
		if (missing === 0) return;
		await delay(0);
	}
	throw new Error("Timeout waiting for protocol streams to become duplex");
};
