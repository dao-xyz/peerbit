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

## Reclamation capability

`Blocks.localReclamation` is an operational capability, not caller-declared
metadata. A service may report `enforcedReclamation: "scoped-references-v1"`
through the additive `observedLocalStoreSafety` property only when that
capability exists; callers must additionally require a `ready` health result.
The existing `localStoreSafety` property and `BlockStoreSafety` declaration
type remain restricted to `enforcedReclamation: "none"` for source
compatibility. `DeclaredBlockStoreSafety` is an explicit alias, while
`ObservedBlockStoreSafety` includes the service-minted enforced variant.
Custom-store configuration therefore cannot assert an enforcement mechanism it
does not implement. Peerbit's enforced metadata value is private to the
built-in wrapper and is derived from that wrapper's actual capability; neither
the opt-in flag nor caller metadata can mint it.

Each `openScope` key is exactly 32 bytes and is defensively copied. Scope
handles belong to one service lifecycle. Managed retain/release operations are
single-block, bounded, idempotent, local-only, and isolated from raw `put`/`rm`
blocks.
