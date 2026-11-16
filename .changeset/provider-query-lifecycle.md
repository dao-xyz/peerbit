---
"@peerbit/pubsub": patch
---

Retire a provider query's request records and cancel its unsent query frames when `queryProviderCandidates` (and so `queryProviders`) ends by reply timeout, caller abort or service stop. Before, an abort or a stop left the request records behind. A query frame still being signed at a reply timeout or an abort was sent after the query had returned, and one still being signed at a stop failed as an unhandled promise rejection. A query send that throws, for example when signing fails, now rejects the awaited query with that error at once and cancels the query's other tracker attempts, so their answers are not returned. Before, the error was an unhandled promise rejection, which ends a Node.js process by default, and the query waited for its reply timeout, then resolved with the other trackers' answers. Delivery failures still do not fail a query, a tracker that does not answer in time still adds no candidates, and default timeouts are unchanged.
