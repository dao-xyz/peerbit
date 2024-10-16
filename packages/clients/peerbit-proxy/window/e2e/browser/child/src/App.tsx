import { randomBytes } from "@peerbit/crypto";
import { Change } from "@peerbit/log";
import { createClient } from "@peerbit/proxy-window";
import { SharedLog } from "@peerbit/shared-log";
import { useEffect, useReducer, useRef } from "react";

const client = await createClient("*");

export const App = () => {
	const mounted = useRef<boolean>(false);
	const dbRef = useRef<SharedLog>();
	const [_, forceUpdate] = useReducer((x) => x + 1, 0);
	useEffect(() => {
		const queryParameters = new URLSearchParams(window.location.search);

		if (mounted.current) {
			return;
		}
		mounted.current = true;
		client
			.open<SharedLog<Uint8Array>>(new SharedLog({ id: new Uint8Array(32) }), {
				args: {
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
			})
			.then((x: any) => {
				dbRef.current = x;
				if (queryParameters.get("read") !== "true") {
					setTimeout(() => {
						// FIX make sure this works without timeout in the test
						x.append(randomBytes(32), { meta: { next: [] } });
					}, 1000);
				}
			});
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
