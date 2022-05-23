import { field } from "@dao-xyz/borsh";

export class Post {

    @field({ type: 'String' })
    message: string;
}