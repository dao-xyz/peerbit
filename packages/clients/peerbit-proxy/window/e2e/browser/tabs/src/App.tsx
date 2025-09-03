import { randomBytes } from "@peerbit/crypto";
import type { Change } from "@peerbit/log";
import { SharedLog } from "@peerbit/shared-log";
import { useEffect, useReducer, useRef } from "react";
import { createSharedWorkerClient } from "../../../../src/sharedworker/client";

const client = await createSharedWorkerClient(
	new URL("../../../../src/sharedworker/host.ts", import.meta.url),
);
console.log("Client connected", client);
export const App = () => {
	const dbRef = useRef<SharedLog<any, any>>();
	const [, forceUpdate] = useReducer((x) => x + 1, 0);

	useEffect(() => {
		let mounted = true;
		(async () => {
			const db = await client.open<SharedLog<Uint8Array, any>>(
				new SharedLog({ id: new Uint8Array(32) }),
				{
					args: {
						onChange: (_change: Change<Uint8Array>) => {
							if (!mounted) return;
							forceUpdate();
						},
					},
				},
			);
			dbRef.current = db;
			// Append unless read-only
			const qp = new URLSearchParams(window.location.search);
			if (qp.get("read") !== "true") {
				setTimeout(
					() => db.append(randomBytes(32), { meta: { next: [] } }),
					300,
				);
			}
		})();
		return () => {
			mounted = false;
		};
	}, []);

	return (
		<>
			<div data-testid="counter">{dbRef.current?.log.length}</div>
			<button onClick={() => dbRef.current?.log.load({ reset: true })}>
				Reload
			</button>
		</>
	);
};
