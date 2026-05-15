# @peerbit/native-backbone

Experimental native owner for Peerbit write transactions.

This package is intentionally internal-facing while the native write path is being
fused. It owns the native lower-log graph, native log block store, and shared-log
resident coordinate state in one Rust object so higher layers can move toward a
single `JS -> native -> compact facts` transaction boundary.
