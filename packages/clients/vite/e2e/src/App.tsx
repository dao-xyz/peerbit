import { randomBytes } from "@peerbit/crypto";
import type { Change } from "@peerbit/log";
import { SharedLog } from "@peerbit/shared-log";
import { Peerbit } from "peerbit";
import { useEffect, useReducer, useRef } from "react";

export const App = () => {
	const mounted = useRef<boolean>(false);
	const dbRef = useRef<SharedLog<any, any> | undefined>(undefined);
	const [_, forceUpdate] = useReducer((x) => x + 1, 0);
	useEffect(() => {
		if (mounted.current) {
			return;
		}
		mounted.current = true;
		Peerbit.create().then((client) =>
			client
				.open<SharedLog<Uint8Array, any>>(
					new SharedLog({ id: new Uint8Array(32) }),
					{
						args: {
							onChange: (change: Change<Uint8Array>) => {
								forceUpdate();
								setTimeout(() => {
									dbRef.current?.log.load().then(() => {
										forceUpdate();
										console.log(dbRef.current?.log.length);
									});
								}, 1000);
							},
						},
					},
				)
				.then((x: any) => {
					dbRef.current = x;
					setTimeout(() => {
						// FIX make sure this works without timeout in the test
						x.append(randomBytes(32), { meta: { next: [] } });
					}, 1000);
				})
				.catch((e) => {
					console.error(e);
				}),
		);
	});
	return (
		<>
			<>Log length</>
			<div data-testid="counter">{dbRef.current?.log.length}</div>
		</>
	);
};
