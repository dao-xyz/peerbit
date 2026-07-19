# Direct block interface

Message types for the Block swap/share protocol

## Verifying opaque block bytes

Use `verifyBlockBytes` when a transport or store only needs to validate and
forward opaque bytes. It checks the CID version, codec code, and multihash
without decoding the block value. Logical decoding should happen later, at the
consumer boundary, after the bytes are authenticated.

`checkDecodeBlock` also verifies the digest before invoking a codec. A matching
DAG-CBOR block can still allocate its decoded object graph, so callers handling
attacker-selected CIDs should prefer `verifyBlockBytes` unless they explicitly
need the logical value and enforce suitable resource bounds.
