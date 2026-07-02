//! JsValue-free port of the DirectStream protocol state machine
//! (`packages/transport/stream/src`): the multi-hop routing table, the
//! seen-cache dedup counter, the 4-lane weighted-round-robin outbound
//! scheduler and the seek-routing/relay decision helpers. The state machine
//! never owns sockets: the TS adapter pumps bytes and applies the decisions
//! these modules produce.

pub mod decisions;
pub mod lanes;
pub mod routes;
pub mod seen_cache;
