import { type UseQuerySharedOptions, useQuery } from "@peerbit/document-react";
import {
	PartyDocumentStore,
	PartyMessage,
	PartyMessageIndex,
} from "@peerbit/document-react-party-shared";
import { SortDirection } from "@peerbit/indexer-interface";
import { create as createSimpleIndexer } from "@peerbit/indexer-simple";
import { useProgram } from "@peerbit/program-react";
import { PeerProvider, usePeer } from "@peerbit/react";
import { useEffect, useMemo, useState } from "react";

const searchParams = new URLSearchParams(window.location.search);
const bootstrapParam = searchParams.get("bootstrap") ?? "";
const bootstrapAddrs = bootstrapParam
	.split(",")
	.map((addr) => addr.trim())
	.filter(Boolean);
const replicateParam = searchParams.get("replicate");
const replicate = replicateParam !== "false" && replicateParam !== "0";
const labelParam = searchParams.get("label");
const label =
	labelParam && labelParam.length > 0
		? labelParam
		: `peer-${Math.random().toString(36).slice(2, 8)}`;

const networkOption = {
	type: "explicit",
	bootstrap: bootstrapAddrs || [],
} as const;
const messageToWrite: string[] = [];
const writeParam = searchParams.get("write");
// write="hello,world,test"
if (writeParam && writeParam.length > 0) {
	messageToWrite.push(
		...writeParam
			.split(",")
			.map((s) => s.trim())
			.filter(Boolean),
	);
}
const pushParam = searchParams.get("push");
const push = pushParam === "true" || pushParam === "1";
const insertedWriteParam = messageToWrite.length;

console.log("App config:", {
	bootstrapAddrs,
	label,
	replicate,
	messageToWrite,
	push,
});

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
	useEffect(() => {
		if (!peer) return;
		(window as any)["peerbit"] = peer;
	}, [peer]);
	const store = useMemo(() => PartyDocumentStore.createFixed(), []);
	const [message, setMessage] = useState("");

	const { program } = useProgram(peer ? store : undefined, {
		args: { replicate },
		keepOpenOnUnmount: true,
	});

	const queryOptions: UseQuerySharedOptions<
		PartyMessage,
		PartyMessageIndex,
		true
	> = useMemo(
		() => ({
			query: {
				sort: [{ key: ["timestamp"], direction: SortDirection.ASC }],
			},
			resolve: true as const,
			prefetch: true,
			local: true,
			debug: true,
			updates: {
				merge: true,
				onBatch: (items, meta) => {
					console.log("onBatch", { items, meta });
				},
				notify: (reason) => {
					console.log("notify", reason);
				},
				push,
			},
			remote: {
				reach: { eager: true },
			},
		}),
		[replicate],
	);

	const query = useQuery<PartyMessage, PartyMessageIndex>(
		program?.documents,
		queryOptions,
	);

	const items = query.items ?? [];
	const loadMore = query.loadMore;

	const displayItems = items;

	// read the amount of messages to write from the query params

	useEffect(() => {
		if (!program || !peer || !insertedWriteParam) return;

		// read the amount of messages to write from the query params, insert
		const key = `party-written-${peer.identity.publicKey.hashcode()}`;
		if ((window as any)[key]) {
			return;
		}
		(window as any)[key] = true;

		for (const message of messageToWrite) {
			const now = BigInt(Date.now());
			program.documents.put(
				new PartyMessage({
					author: label,
					content: message,
					timestamp: now,
				}),
			);
		}
	}, [program, peer, label, loadMore, replicate, insertedWriteParam]);

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
				<strong>Replicating: </strong> {replicate ? "yes" : "no"}
			</p>
			<hr />
			<h2>Peer Status</h2>
			<p data-testid="connection-status">{status}</p>
			{error ? (
				<p data-testid="peer-error">Bootstrap: {error.message}</p>
			) : null}
			<p data-testid="peer-id">
				{peer ? peer.identity.publicKey.hashcode() : "connecting"}
			</p>
			<p data-testid="config-status">
				{push ? "push enabled" : "push disabled"}
			</p>
			<p data-testid="message-count">{displayItems.length}</p>
			<ul data-testid="messages">
				{displayItems.map((item) => (
					<li
						key={item.id}
						data-timestamp={String(item.timestamp)}
						data-author={item.author}
					>
						{item.content}
					</li>
				))}
			</ul>

			<input
				data-testid="message-input"
				type="text"
				value={message}
				onChange={(e) => setMessage(e.target.value)}
			/>
			<button
				data-testid="send-button"
				disabled={!program || !message}
				onClick={async () => {
					if (!program) {
						throw new Error("Program not ready");
					}
					const now = BigInt(Date.now());
					await program.documents.put(
						new PartyMessage({
							author: label,
							content: message,
							timestamp: now,
						}),
					);
					setMessage("");
				}}
			>
				Send
			</button>
		</main>
	);
};
