import {
	CanonicalHost,
	type CanonicalHostOptions,
	type CanonicalModule,
	type CanonicalRuntimeOptions,
	PeerbitCanonicalRuntime,
} from "./index.js";

export const CANONICAL_SERVICE_WORKER_CONNECT_OP =
	"__peerbit_canonical_connect__";
export const CANONICAL_SERVICE_WORKER_READY_KEY = "__peerbit_canonical_ready__";

export type InstallServiceWorkerHostOptions = CanonicalRuntimeOptions & {
	modules?: CanonicalModule[];
	hostOptions?: CanonicalHostOptions;
	host?: CanonicalHost;
	createHost?: () => CanonicalHost | Promise<CanonicalHost>;
	lifecycle?: {
		skipWaiting?: boolean;
		clientsClaim?: boolean;
	};
	onError?: (error: unknown) => void;
};

let hostPromise: Promise<CanonicalHost> | undefined;

const getHost = async (options?: InstallServiceWorkerHostOptions) => {
	if (hostPromise) return hostPromise;

	const provided = options?.host;
	const create = options?.createHost;
	const { modules, hostOptions, ...runtimeOptions } = options ?? {};

	const host = provided
		? provided
		: create
			? await create()
			: new CanonicalHost(
					new PeerbitCanonicalRuntime(runtimeOptions),
					hostOptions,
				);

	if (modules?.length) {
		host.registerModules(modules);
	}

	hostPromise = Promise.resolve(host);
	return hostPromise;
};

const coerceErrorMessage = (error: unknown): string => {
	if (error instanceof Error) return error.message;
	return String((error as any)?.message ?? error);
};

export const installServiceWorkerHost = (
	options?: InstallServiceWorkerHostOptions,
) => {
	const scope = self as unknown as ServiceWorkerGlobalScope;

	if (options?.lifecycle?.skipWaiting) {
		scope.addEventListener("install", (event) => {
			event.waitUntil(scope.skipWaiting());
		});
	}

	if (options?.lifecycle?.clientsClaim) {
		scope.addEventListener("activate", (event) => {
			event.waitUntil(scope.clients.claim());
		});
	}

	scope.addEventListener("message", (event: ExtendableMessageEvent) => {
		const data = event.data as any;
		if (
			data?.op !== CANONICAL_SERVICE_WORKER_CONNECT_OP &&
			data?.__peerbit_canonical_connect__ !== true
		) {
			return;
		}

		const port = event.ports?.[0];
		if (!port) return;

		event.waitUntil(
			getHost(options)
				.then((host) => {
					host.attachControlPort(port);
					try {
						port.postMessage({ [CANONICAL_SERVICE_WORKER_READY_KEY]: true });
					} catch {}
				})
				.catch((error) => {
					options?.onError?.(error);
					const message = coerceErrorMessage(error);
					try {
						port.postMessage({
							[CANONICAL_SERVICE_WORKER_READY_KEY]: false,
							error: message,
						});
					} catch {}
				}),
		);
	});
};
