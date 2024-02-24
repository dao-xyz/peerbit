import { SearchRequest } from "./query";
import { IndexKey } from "./types";

abstract class IndexEngine {
	abstract get(id: IndexKey): Promise<Document>;
	abstract query(query: SearchRequest): Promise<Document[]>;
}

class HashMapIndexEngine extends IndexEngine {
	// ...
}
