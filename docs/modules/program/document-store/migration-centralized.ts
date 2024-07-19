import { field, variant } from "@dao-xyz/borsh";
import { Ed25519Keypair } from "@peerbit/crypto";
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

const CENTRAL_AUTHORITY = (await Ed25519Keypair.create()).publicKey;

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
			canPerform: (props) => {
				// Signed by the CENTRAL_AUTHORITY, just trust it?
				if (props.entry.publicKeys.find((x) => x.equals(CENTRAL_AUTHORITY))) {
					return true;
				}

				// Do some other kind of validation work for data not signed by central authority
				// TODO
				return true;
			},
		});

		// Migration loop only running by the central authority
		if (CENTRAL_AUTHORITY.equals(this.node.identity.publicKey)) {
			const allLocalPosts = await this.posts.index.search(new SearchRequest(), {
				local: true,
				remote: false,
			});
			for (const post of allLocalPosts) {
				if (post instanceof PostV0) {
					// Then migrate (since I am the central authority, this put operation will make the insertion signed by the central authority)
					await this.posts.put(
						new PostV1({
							id: post.id,
							message: post.message,
							title: "Migrated post",
						}),
					);
				}
			}
		}
	}
}
