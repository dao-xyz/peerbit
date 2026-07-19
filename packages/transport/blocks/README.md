# Direct block

Block swap/share protocol built on top of [Direct Stream](./../direct-stream/README.md)

Remote responses are treated as opaque bytes. Their CID is verified before a
response can resolve a read, be persisted, or teach the provider cache about
the sender. The transport does not decode DAG-CBOR responses while performing
that integrity check.
