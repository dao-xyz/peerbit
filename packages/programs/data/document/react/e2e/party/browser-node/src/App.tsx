import { useEffect, useMemo, useState } from "react";
import { PeerProvider, usePeer } from "@peerbit/react";
import { useProgram } from "@peerbit/program-react";
import { useQuery } from "@peerbit/document-react";
import {
	PartyDocumentStore,
	PartyMessage,
	PartyMessageIndex,
} from "@peerbit/document-react-party-shared";
import { create as createSimpleIndexer } from "@peerbit/indexer-simple";
import { SortDirection } from "@peerbit/indexer-interface";

const searchParams = new URLSearchParams(window.location.search);
const bootstrapParam = searchParams.get("bootstrap") ?? "";
const bootstrapAddrs = bootstrapParam
	.split(",")
	.map((addr) => addr.trim())
	.filter(Boolean);
const replicateParam = searchParams.get("replicate");
const replicate = replicateParam !== "false" && replicateParam !== "0";
const labelParam = searchParams.get("label");
const label = labelParam && labelParam.length > 0
	? labelParam
	: `peer-${Math.random().toString(36).slice(2, 8)}`;

const networkOption = bootstrapAddrs.length
	? ({ type: "explicit", bootstrap: bootstrapAddrs } as const)
	: ("remote" as const);
console.log("bootstrap options", networkOption);
export const App = () => {
	return (
		<PeerProvider
			network={networkOption}
			waitForConnnected="in-flight"
			indexer={createSimpleIndexer}
		>
			<DocumentParty label={label} replicate={replicate} />
		</PeerProvider>
	);
};

type DocumentPartyProps = {
	label: string;
	replicate: boolean;
};

const DocumentParty = ({ label, replicate }: DocumentPartyProps) => {
	const { peer, status, error } = usePeer();
	const store = useMemo(() => PartyDocumentStore.createFixed(), []);
	const [message, setMessage] = useState("");

	const { program } = useProgram(peer ? store : undefined, {
		args: { replicate },
		keepOpenOnUnmount: true,
	});

	const queryOptions = useMemo(
		() => ({
			query: {
				sort: [{ key: ["timestamp"], direction: SortDirection.ASC }],
			},
			resolve: true as const,
			prefetch: true,
			local: true,
			remote: { reach: { eager: true }, wait: { timeout: 15_000 } },
		}),
		[],
	);

	const query = useQuery<PartyMessage, PartyMessageIndex>(
		program?.documents,
		queryOptions,
	);

	const items = query.items ?? [];

	useEffect(() => {
		if (!program || !peer) return;
		const key = `party-written-${peer.identity.publicKey.hashcode()}`;
		if ((window as any)[key]) {
			return;
		}
		(window as any)[key] = true;
		const now = BigInt(Date.now());
		program.documents
			.put(
				new PartyMessage({
					author: label,
					content: `hello from ${peer.identity.publicKey.hashcode()}`,
					timestamp: now,
				})
			)
			.catch((err: unknown) => console.error("Failed to write", err));
	}, [program, peer, label]);

	return (
		<main style={{ fontFamily: "sans-serif", padding: 16 }}>
			<h1>Document React Party</h1>
			<p data-testid="bootstrap-addrs">
				<strong>Bootstrap addrs:</strong>{" "}
				{bootstrapAddrs.length > 0
					? bootstrapAddrs.join(", ")
					: "Using remote peer"}
			</p>
			<p data-testid="peer-label">
				<strong>Label:</strong> {label}
			</p>
			<p data-testid="replicate-status">
				<strong>Replicating:</strong> {replicate ? "yes" : "no"}
			</p>
			<hr />
			<h2>Peer Status</h2>
			<p data-testid="connection-status">{status}</p>
			{error ? <p data-testid="peer-error">Bootstrap: {error.message}</p> : null}
			<p data-testid="peer-id">
				{peer ? peer.identity.publicKey.hashcode() : ("connecting")}
			</p>
			<p data-testid="message-count">{items.length}</p>
			<ul data-testid="messages">
				{items.map((item) => (
					<li
						key={item.id}
						data-timestamp={String(item.timestamp)}
						data-author={item.author}
					>
						{item.content}
					</li>
				))}
			</ul>

			<input data-testid="message-input" type="text" value={message} onChange={(e) => setMessage(e.target.value)} />	
			<button data-testid="send-button" disabled={!program} onClick={async () => {
				if (!program) {
					throw new Error("Program not ready");
				}
				const now = BigInt(Date.now());
				await program.documents.put(
					new PartyMessage({
						author: label,
						content: message,
						timestamp: now,
					})
				);
				setMessage("");
			}}>Send</button>
		</main>
	);
};
