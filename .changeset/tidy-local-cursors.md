---
"@peerbit/document": patch
---

Keep a local query going when a page resolves no documents but its local cursor still has rows, for example rows of an index with a custom `type` whose entries are no longer in the local log. Before, the iterator ended after such a first page, or dropped its local cursor after such a later page, so `next()`, `all()` and `search()` with a `fetch` limit could stop early and skip the remaining rows. An empty local page still lets remote fallback run, and a cursor with no rows left still closes as before. A first-page response, or a local next-page response, that arrives after `close()` is now ignored instead of reopening the iterator or returning documents. The internal `onResponse` callback of the query path (not part of the public `QueryOptions` type) now also runs for an empty local page that still has rows kept, as it already did for remote responses.
