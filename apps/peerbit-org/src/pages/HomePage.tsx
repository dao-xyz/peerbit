import { Card } from "../ui/Card";
import { Container } from "../ui/Container";

export function HomePage() {
	return (
		<Container>
			<div className="mx-auto max-w-3xl py-10">
				<h1 className="text-4xl font-extrabold tracking-tight sm:text-5xl">
					Build for the distributed web
				</h1>
				<p className="mt-4 text-lg text-slate-600 dark:text-slate-300">
					Peerbit is a P2P framework for building apps with E2EE, sharding, and
					searchable data.
				</p>

				<div className="mt-8 grid auto-rows-fr gap-4 sm:grid-cols-2">
					<Card
						title="Get started"
						description="Spin up a peer, open a program, store documents."
						href="#/docs/getting-started"
					/>
					<Card
						title="Modules"
						description="Learn about clients, programs, encoding and encryption."
						href="#/docs/modules/client"
					/>
					<Card
						title="Release notes"
						description="What changed in the latest versions."
						href="#/release-notes"
					/>
					<Card
						title="Status"
						description="Public bootstrap health and network signals."
						href="#/status"
					/>
				</div>
			</div>
		</Container>
	);
}
