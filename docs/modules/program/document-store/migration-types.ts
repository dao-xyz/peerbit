import { field, variant } from "@dao-xyz/borsh";
import { Documents, PutOperation } from "@peerbit/document";
import { Program } from "@peerbit/program";

abstract class AbstractPost {}

@variant(1) // Adding this will prepend the byte 0 to posts of this type (important (!))
class PostV0 extends AbstractPost {
	@field({ type: "string" })
	id: string;

	@field({ type: "string" })
	message: string;
}

@variant(1) // Adding this will prepend the byte 1 to posts of this type (important (!))
class PostV1 extends AbstractPost {
	@field({ type: "string" })
	id: string;

	@field({ type: "string" })
	title: string;

	@field({ type: "string" })
	message: string;
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
			canPerform: (operation, contenxt) => {
				if (operation instanceof PutOperation) {
					// Because bytes 0 and 1 was prepended to the post on serialization
					// we can distinguish between them
					// here after loading from disc

					if (operation.value instanceof PostV0) {
						// Validate something with the 'old' post type
						return true;
					} else if (operation.value instanceof PostV1) {
						// Validate something with the 'new' post type
						return true;
					} else {
						return false;
					}
				}
				return false;
			}
		});
	}
}
