import { type Multiaddr, multiaddr } from "@multiformats/multiaddr";
import {
	Circuit,
	DNSADDR,
	TCP,
	WebRTC,
	WebSockets,
	WebSocketsSecure,
} from "@multiformats/multiaddr-matcher";

const BOOTSTRAP_LIST_FETCH_TIMEOUT_MS = 10_000;
const MAX_BOOTSTRAP_LIST_BYTES = 64 * 1024;
const MAX_BOOTSTRAP_LIST_LINES = 512;
const MAX_BOOTSTRAP_ADDRESS_COUNT = 256;

const hasRemoteEndpoint = (address: Multiaddr): boolean =>
	!address
		.getComponents()
		.some(
			(component) =>
				(component.name === "tcp" && component.value === "0") ||
				(component.name === "ip4" && component.value === "0.0.0.0") ||
				(component.name === "ip6" && component.value === "::"),
		);

const hasDatagramTransport = (address: Multiaddr): boolean =>
	address
		.getComponents()
		.some((component) =>
			["udp", "quic", "quic-v1", "webtransport", "webrtc-direct"].includes(
				component.name,
			),
		);

const isWebSocketTarget = (address: Multiaddr): boolean =>
	!hasDatagramTransport(address) && WebSockets.exactMatch(address);

const isSecureWebSocketTarget = (address: Multiaddr): boolean =>
	!hasDatagramTransport(address) && WebSocketsSecure.exactMatch(address);

const hasDefaultDirectPrefix = (address: Multiaddr): boolean =>
	!hasDatagramTransport(address) &&
	(DNSADDR.matches(address) ||
		TCP.matches(address) ||
		WebSockets.matches(address) ||
		WebSocketsSecure.matches(address));

const hasBrowserSafeDirectPrefix = (address: Multiaddr): boolean =>
	!hasDatagramTransport(address) &&
	(DNSADDR.matches(address) || WebSocketsSecure.matches(address));

const hasCircuitPeerIds = (address: Multiaddr): boolean => {
	const components = address.getComponents();
	const circuitIndex = components.findIndex(
		(component) => component.name === "p2p-circuit",
	);
	return (
		circuitIndex >= 0 &&
		components
			.slice(0, circuitIndex)
			.some((component) => component.name === "p2p") &&
		components
			.slice(circuitIndex + 1)
			.some((component) => component.name === "p2p")
	);
};

const classifyBootstrapAddress = (
	address: Multiaddr,
): { supported: boolean; crossRuntime: boolean } => {
	if (!hasRemoteEndpoint(address)) {
		return { supported: false, crossRuntime: false };
	}

	const direct =
		DNSADDR.exactMatch(address) ||
		TCP.exactMatch(address) ||
		isWebSocketTarget(address) ||
		isSecureWebSocketTarget(address);
	const circuit =
		hasCircuitPeerIds(address) &&
		Circuit.exactMatch(address) &&
		hasDefaultDirectPrefix(address);
	const webRTC =
		hasCircuitPeerIds(address) &&
		WebRTC.exactMatch(address) &&
		hasBrowserSafeDirectPrefix(address);
	const crossRuntime =
		DNSADDR.exactMatch(address) ||
		isSecureWebSocketTarget(address) ||
		(circuit && hasBrowserSafeDirectPrefix(address));

	return { supported: direct || circuit || webRTC, crossRuntime };
};

const getBootstrapListSources = (v: string): string[] => {
	const file = `bootstrap${v ? "-" + encodeURIComponent(v) : ""}.env`;
	return [
		`https://bootstrap.peerbit.org/${file}`,
		`https://raw.githubusercontent.com/dao-xyz/peerbit-bootstrap/master/${file}`,
	];
};

const parseBootstrapAddresses = (value: string): string[] => {
	const lines = value.split(/\r?\n/);
	if (lines.length > MAX_BOOTSTRAP_LIST_LINES) {
		throw new Error(
			`Bootstrap list has too many lines (${lines.length} > ${MAX_BOOTSTRAP_LIST_LINES})`,
		);
	}

	const addresses = lines
		.map((line) => line.trim())
		.filter((line) => line.length > 0 && !line.startsWith("#"));

	if (addresses.length === 0) {
		throw new Error("Bootstrap list is empty");
	}
	if (addresses.length > MAX_BOOTSTRAP_ADDRESS_COUNT) {
		throw new Error(
			`Bootstrap list has too many addresses (${addresses.length} > ${MAX_BOOTSTRAP_ADDRESS_COUNT})`,
		);
	}

	const canonicalAddresses: string[] = [];
	let hasCrossRuntimeAddress = false;
	for (const address of addresses) {
		let parsed: Multiaddr;
		try {
			parsed = multiaddr(address);
		} catch {
			continue;
		}
		const classification = classifyBootstrapAddress(parsed);
		if (!classification.supported) {
			continue;
		}
		hasCrossRuntimeAddress ||= classification.crossRuntime;
		canonicalAddresses.push(parsed.toString());
	}
	if (canonicalAddresses.length === 0) {
		throw new Error("Bootstrap list has no supported dial targets");
	}
	if (!hasCrossRuntimeAddress) {
		throw new Error(
			"Bootstrap list has no browser-safe cross-runtime dial target",
		);
	}

	return [...new Set(canonicalAddresses)];
};

const readBootstrapList = async (response: Response): Promise<string> => {
	const declaredLength = response.headers.get("content-length");
	if (declaredLength != null) {
		if (!/^\d+$/.test(declaredLength)) {
			throw new Error(
				`Invalid bootstrap list Content-Length: ${declaredLength}`,
			);
		}
		const length = Number(declaredLength);
		if (!Number.isSafeInteger(length) || length > MAX_BOOTSTRAP_LIST_BYTES) {
			throw new Error(
				`Bootstrap list is too large (${declaredLength} > ${MAX_BOOTSTRAP_LIST_BYTES} bytes)`,
			);
		}
	}

	if (!response.body) {
		return "";
	}

	const reader = response.body.getReader();
	const decoder = new TextDecoder("utf-8", { fatal: true });
	let bytesRead = 0;
	let value = "";
	let completed = false;
	try {
		while (true) {
			const chunk = await reader.read();
			if (chunk.done) {
				break;
			}
			bytesRead += chunk.value.byteLength;
			if (bytesRead > MAX_BOOTSTRAP_LIST_BYTES) {
				throw new Error(
					`Bootstrap list is too large (${bytesRead} > ${MAX_BOOTSTRAP_LIST_BYTES} bytes)`,
				);
			}
			value += decoder.decode(chunk.value, { stream: true });
		}
		value += decoder.decode();
		completed = true;
		return value;
	} finally {
		if (!completed) {
			await reader
				.cancel("Bootstrap list read did not complete")
				.catch(() => {});
		}
		reader.releaseLock();
	}
};

const fetchBootstrapAddresses = async (source: string): Promise<string[]> => {
	const controller = new AbortController();
	const timeout = setTimeout(() => {
		controller.abort(
			new Error(
				`Timed out fetching bootstrap list after ${BOOTSTRAP_LIST_FETCH_TIMEOUT_MS} ms`,
			),
		);
	}, BOOTSTRAP_LIST_FETCH_TIMEOUT_MS);

	let response: Response | undefined;
	try {
		response = await fetch(source, { signal: controller.signal });
		if (!response.ok) {
			throw new Error(
				`Bootstrap list returned HTTP ${response.status}${
					response.statusText ? ` ${response.statusText}` : ""
				}`,
			);
		}
		return parseBootstrapAddresses(await readBootstrapList(response));
	} catch (error) {
		if (response?.body && !response.body.locked) {
			await response.body.cancel(error).catch(() => {});
		}
		if (!controller.signal.aborted) {
			controller.abort(error);
		}
		throw error;
	} finally {
		clearTimeout(timeout);
	}
};

export const resolveBootstrapAddresses = async (
	v: string = "5",
): Promise<string[]> => {
	const failures: Error[] = [];
	for (const source of getBootstrapListSources(v)) {
		try {
			return await fetchBootstrapAddresses(source);
		} catch (error) {
			failures.push(
				new Error(
					`Failed to load bootstrap addresses from ${source}: ${
						error instanceof Error ? error.message : String(error)
					}`,
				),
			);
		}
	}

	throw new AggregateError(
		failures,
		`Failed to resolve bootstrap addresses for network version ${v || "default"}`,
	);
};

export const getBootstrapPeerId = (
	address: string | Multiaddr,
): string | undefined => {
	try {
		const parsed = typeof address === "string" ? multiaddr(address) : address;
		const components = parsed.getComponents();
		for (let index = components.length - 1; index >= 0; index--) {
			if (components[index].name === "p2p") {
				return components[index].value;
			}
		}
		return undefined;
	} catch {
		return undefined;
	}
};
