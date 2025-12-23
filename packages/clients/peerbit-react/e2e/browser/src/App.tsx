import { field, variant } from "@dao-xyz/borsh";
import { Documents } from "@peerbit/document";
import { useQuery } from "@peerbit/document-react";
import { PeerProvider, usePeer, useProgram } from "@peerbit/react";
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

const getSqliteEnabled = () =>
	new URLSearchParams(window.location.search).has("sqlite");

const getDocumentQueryEnabled = () =>
	new URLSearchParams(window.location.search).has("doc");

const SqliteStatus = () => {
	const enabled = React.useMemo(getSqliteEnabled, []);
	const [status, setStatus] = React.useState("idle");

	React.useEffect(() => {
		if (!enabled) {
			return;
		}
		let cancelled = false;
		const run = async () => {
			setStatus("loading");
			try {
				const { create } = await import("@peerbit/indexer-sqlite3");
				await create();
				if (!cancelled) {
					setStatus("ready");
				}
			} catch (error) {
				console.error(error);
				if (!cancelled) {
					setStatus("error");
				}
			}
		};
		void run();
		return () => {
			cancelled = true;
		};
	}, [enabled]);

	if (!enabled) {
		return null;
	}

	return <div data-testid="sqlite-status">{status}</div>;
};

@variant(0)
class Post {
	@field({ type: "string" })
	id!: string;

	@field({ type: "string" })
	message!: string;

	constructor(props?: { id?: string; message?: string }) {
		if (!props) return; // borsh
		this.id = props.id ?? `${Date.now()}-${Math.random()}`;
		this.message = props.message ?? "";
	}
}

@variant(0)
class PostIndexed {
	@field({ type: "string" })
	id!: string;

	@field({ type: "string" })
	indexedMessage!: string;

	constructor(props?: Post) {
		if (!props) return; // borsh
		this.id = props.id;
		this.indexedMessage = props.message;
	}
}

const DocumentQueryStatus = () => {
	const enabled = React.useMemo(getDocumentQueryEnabled, []);
	const { peer } = usePeer();
	const docs = React.useMemo(() => {
		if (!enabled) {
			return undefined;
		}
		return new Documents<Post, PostIndexed>();
	}, [enabled]);

	const { program, loading } = useProgram(
		peer,
		peer && docs ? docs : undefined,
		{
			args: {
				type: Post,
				index: {
					type: PostIndexed,
				},
				replicate: false,
			},
			keepOpenOnUnmount: true,
		},
	);

	const [seedState, setSeedState] = React.useState<
		"idle" | "seeding" | "seeded" | "error"
	>("idle");
	const hasSeededRef = React.useRef(false);

	React.useEffect(() => {
		if (!enabled || !program || hasSeededRef.current) {
			return;
		}
		hasSeededRef.current = true;

		let cancelled = false;
		const run = async () => {
			setSeedState("seeding");
			try {
				await program.put(new Post({ id: "a", message: "hello" }));
				await program.put(new Post({ id: "b", message: "bye" }));
				if (!cancelled) {
					setSeedState("seeded");
				}
			} catch (error) {
				console.error(error);
				if (!cancelled) {
					setSeedState("error");
				}
			}
		};
		void run();

		return () => {
			cancelled = true;
		};
	}, [enabled, program]);

	const query = useQuery(seedState === "seeded" ? program : undefined, {
		query: {
			query: {
				indexedMessage: "hello",
			},
		},
		resolve: true,
		local: true,
		prefetch: true,
	});

	if (!enabled) {
		return null;
	}

	const queryItems = query.items ?? [];

	let status = "idle";
	if (seedState === "error") {
		status = "error";
	} else if (loading || !program) {
		status = "opening";
	} else if (seedState !== "seeded") {
		status = "seeding";
	} else if (query.isLoading) {
		status = "querying";
	} else if (queryItems.length === 1 && queryItems[0]?.message === "hello") {
		status = "ready";
	} else {
		status = "waiting";
	}

	return (
		<div>
			<div data-testid="doc-query-status">{status}</div>
			<ul data-testid="doc-query-results">
				{queryItems.map((item) => (
					<li key={item.id}>{item.message}</li>
				))}
			</ul>
		</div>
	);
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
			<SqliteStatus />
			<DocumentQueryStatus />
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
