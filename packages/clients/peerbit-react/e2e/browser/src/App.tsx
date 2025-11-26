import { PeerProvider, usePeer } from "@peerbit/react";
import React from "react";

const getBootstrapAddrs = () => {
	const search = new URLSearchParams(window.location.search);
	const params = search.getAll("bootstrap");
	const fromComma = params.flatMap((p) =>
		p
			.split(",")
			.map((x) => x.trim())
			.filter(Boolean),
	);
	return fromComma;
};

const PeerInfo = () => {
	const { peer, loading, status, error } = usePeer();
	const [peerHash, setPeerHash] = React.useState<string | undefined>(undefined);

	React.useEffect(() => {
		if (peer?.identity?.publicKey?.hashcode) {
			setPeerHash(peer.identity.publicKey.hashcode());
		}
	}, [peer]);

	return (
		<div>
			<h1>Peerbit React E2E</h1>
			<div data-testid="status">status: {status}</div>
			<div data-testid="loading">loading: {loading ? "yes" : "no"}</div>
			<div data-testid="peer-hash">{peerHash ?? "no-peer"}</div>
			{error ? <div data-testid="error">{error.message}</div> : null}
		</div>
	);
};

const App = () => {
	const bootstrapAddrs = React.useMemo(getBootstrapAddrs, []);

	const network = React.useMemo(() => {
		return bootstrapAddrs.length
			? { type: "explicit" as const, bootstrap: bootstrapAddrs }
			: ("local" as const);
	}, [bootstrapAddrs]);

	return (
		<PeerProvider network={network}>
			<PeerInfo />
		</PeerProvider>
	);
};

export default App;
