import { randomBytes } from "@peerbit/crypto";
import { Change } from "@peerbit/log";
import { createClient } from "@peerbit/proxy-window";
import { SharedLog } from "@peerbit/shared-log";
import { useEffect, useReducer, useRef, useState } from "react";

const client = await createClient("*");
console.log("client", client);

export const App = () => {
	const mounted = useRef<boolean>(false);
	const dbRef = useRef<SharedLog<any, any>>();
	const [_, forceUpdate] = useReducer((x) => x + 1, 0);
	const [peers, setPeers] = useState<Set<string>>(new Set());
	useEffect(() => {
		const queryParameters = new URLSearchParams(window.location.search);

		if (mounted.current) {
			return;
		}
		mounted.current = true;
		client
			.open<SharedLog<Uint8Array, any>>(
				new SharedLog({ id: new Uint8Array(32) }),
				{
					args: {
						replicate: {
							factor: 1,
						},
						onChange: (change: Change<Uint8Array>) => {
							forceUpdate();
							setTimeout(() => {
								dbRef.current?.log.load().then(() => {
									forceUpdate();
									console.log(client.messages.id, dbRef.current?.log.length);
								});
							}, 1000);
						},
					},
				},
			)
			.then((x: any) => {
				console.log("open db", x.address);
				dbRef.current = x;
				if (queryParameters.get("read") !== "true") {
					setTimeout(() => {
						// FIX make sure this works without timeout in the test
						x.append(randomBytes(32), { meta: { next: [] } });
					}, 1000);
				}
			});
		client.services.pubsub.addEventListener("peer:reachable", () => {
			setPeers((prev) => new Set(prev.add(client.peerId.toString())));
		});
		client.services.pubsub.addEventListener("peer:unreachable", () => {
			setPeers((prev) => {
				const newSet = new Set(prev);
				newSet.delete(client.peerId.toString());
				return newSet;
			});
		});
	}, []);
	return (
		<>
			<div data-testid="counter">{dbRef.current?.log.length}</div>
			<div data-testid="peers">{peers.size}</div>
			<button onClick={() => dbRef.current?.log.load({ reset: true })}>
				Reload
			</button>
		</>
	);
};
