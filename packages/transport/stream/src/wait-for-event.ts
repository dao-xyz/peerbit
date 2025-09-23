import type { TypedEventEmitter } from "@libp2p/interface";
import { AbortError, TimeoutError } from "@peerbit/time";
import pDefer, { type DeferredPromise } from "p-defer";

export function waitForEvent<
	Emitter extends TypedEventEmitter<Events>,
	Events extends Record<string, any> = Emitter extends TypedEventEmitter<
		infer E
	>
		? E
		: never,
	Event extends keyof Events = keyof Events,
>(
	emitter: Emitter,
	events: Event[],
	resolver: (deferred: DeferredPromise<void>) => void | Promise<void>,
	options?: {
		signals?: AbortSignal[];
		timeout?: number;
	},
): Promise<void> {
	const deferred = pDefer<void>();
	const abortFn = (e: any) =>
		deferred.reject(
			e ??
				new AbortError(
					"Aborted waiting for event: " +
						String(events.length > 1 ? events.join(", ") : events[0]),
				),
		);

	const checkIsReady = (...args: any[]) => resolver(deferred);

	deferred.promise.finally(() => {
		for (const event of events) {
			emitter.removeEventListener(event as any, checkIsReady);
		}
		clearTimeout(timeout);
		options?.signals?.forEach((signal) =>
			signal.removeEventListener("abort", abortFn),
		);
	});

	for (const event of events) {
		emitter.addEventListener(event as any, (evt) => {
			checkIsReady(event);
		});
	}
	let timeout = setTimeout(
		() => abortFn(new TimeoutError("Timeout waiting for event")),
		options?.timeout ?? 10 * 1000,
	);
	options?.signals?.forEach((signal) =>
		signal.addEventListener("abort", abortFn),
	);
	(resolver as any)(deferred);
	return deferred.promise;
}
