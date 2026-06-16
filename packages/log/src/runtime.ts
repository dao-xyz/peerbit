export const canUseOptionalNativeModuleImports = (): boolean => {
	const scope = globalThis as {
		ServiceWorkerGlobalScope?: unknown;
		clients?: unknown;
		registration?: unknown;
		skipWaiting?: unknown;
	};
	const serviceWorkerGlobalScope = scope.ServiceWorkerGlobalScope;
	return !(
		(typeof serviceWorkerGlobalScope === "function" &&
			globalThis instanceof serviceWorkerGlobalScope) ||
		(!!scope.clients &&
			!!scope.registration &&
			typeof scope.skipWaiting === "function")
	);
};
