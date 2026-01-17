import {
	CanonicalHost,
	type CanonicalHostOptions,
	type CanonicalModule,
	type CanonicalRuntimeOptions,
	PeerbitCanonicalRuntime,
} from "./index.js";

export type InstallSharedWorkerHostOptions = CanonicalRuntimeOptions & {
	modules?: CanonicalModule[];
	hostOptions?: CanonicalHostOptions;
};

let hostPromise: Promise<CanonicalHost> | undefined;

const getHost = async (options?: InstallSharedWorkerHostOptions) => {
	if (hostPromise) return hostPromise;
	const { modules, hostOptions, ...runtimeOptions } = options ?? {};
	const runtime = new PeerbitCanonicalRuntime(runtimeOptions);
	const host = new CanonicalHost(runtime, hostOptions);
	if (modules?.length) {
		host.registerModules(modules);
	}
	hostPromise = Promise.resolve(host);
	return hostPromise;
};

export const installSharedWorkerHost = (
	options?: InstallSharedWorkerHostOptions,
) => {
	const scope = self as unknown as SharedWorkerGlobalScope;
	const onConnect = async (e: MessageEvent) => {
		const port: MessagePort = (e as any).ports?.[0];
		if (!port) {
			throw new Error("SharedWorker onconnect event missing MessagePort");
		}
		const host = await getHost(options);
		host.attachControlPort(port);
	};
	scope.addEventListener("connect", onConnect as unknown as EventListener);
};
