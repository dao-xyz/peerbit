import { field, variant } from "@dao-xyz/borsh";
import { Documents } from "@peerbit/document";
import { SearchRequest } from "@peerbit/indexer-interface";
import { Program } from "@peerbit/program";

abstract class AbstractPost {}

@variant(0)
class PostV0 extends AbstractPost {
	@field({ type: "string" })
	id: string;

	@field({ type: "string" })
	message: string;

	constructor(properties: { id: string; title: string; message: string }) {
		super();
		this.id = properties.id;
		this.message = properties.message;
	}
}

@variant(1)
class PostV1 extends AbstractPost {
	@field({ type: "string" })
	id: string;

	@field({ type: "string" })
	title: string;

	@field({ type: "string" })
	message: string;

	constructor(properties: { id: string; title: string; message: string }) {
		super();
		this.id = properties.id;
		this.title = properties.title;
		this.message = properties.message;
	}
}

@variant("post-store")
class PostStore extends Program {
	@field({ type: Documents })
	posts: Documents<AbstractPost>; // Use base class here

	constructor() {
		super();
		this.posts = new Documents();
	}

	async open(args?: any): Promise<void> {
		await this.posts.open({
			type: AbstractPost, // Use base class here
			canPerform: (properties) => {
				if (properties.type === "put") {
					if (properties.value instanceof PostV0) {
						// Validate something with the 'old' post type
						return true;
					} else if (properties.value instanceof PostV1) {
						// Validate something with the 'new' post type
						return true;
					} else {
						return false;
					}
				}
				return false;
			},
		});

		// Migration loop. This code will be included in everyone's code bases once the code/project owners want everyone to migrate
		// Peers can reject not doing migrations by simply not downloading updates
		for (const results of await this.posts.index.queryDetailed(
			new SearchRequest(),
			{ local: true, remote: false },
		)) {
			for (const result of results.results) {
				const latestCommit = await this.posts.log.log.get(result.context.head);

				// Is Post of type V0 && Am I the signer of the post (i.e. creator?)
				if (
					result.value instanceof PostV0 &&
					latestCommit &&
					latestCommit.publicKeys.find((x) =>
						x.equals(this.node.identity.publicKey),
					)
				) {
					// Then migrate
					await this.posts.put(
						new PostV1({
							id: result.value.id,
							message: result.value.message,
							title: "Migrated post",
						}),
					);

					// Since the same id is used, the old document will be replaced with a new document.
					// if you want to use a different id you can delete the old document with
					// await this.posts.del(result.value.id)
				}
			}
		}
	}
}
