# Trusted network

## 🚧 Experimental state 🚧

`@peerbit/trusted-network` stores a directed graph of trust between signing
identities. A configured root identity is always trusted. An identity is also
trusted when the replicated graph contains a path from the root to that
identity.

```ts
import { TrustedNetwork } from "@peerbit/trusted-network";

const network = await peer.open(
	new TrustedNetwork({ rootTrust: rootPeer.identity.publicKey }),
);

// The local append identity owns the new local -> member edge.
await network.add(memberPeer.identity.publicKey);

// Only that same owner can remove the edge.
await network.revoke(memberPeer.identity.publicKey);
```

Trust relations are directional. A trusted identity may add an outgoing edge
of its own, but it cannot add an edge on behalf of another identity. Likewise,
`revoke()` removes only the caller's direct outgoing edge while that owner is
still trusted. For example, B may revoke B → C, and the root may revoke root →
B, but B cannot delete root → C. Calling `revoke()` for an absent local edge is
a no-op and returns `undefined`.

Revoking an edge immediately affects future `isTrusted()` checks after the
operation has replicated. Removing root → B also makes identities reachable
only through B untrusted. It does not retroactively revalidate application
writes that replicas accepted before the revocation.

The graph itself is currently readable by any peer (`canRead` allows all), and
replication is not a confidentiality boundary. Applications that require a
private membership graph or confidential content must add content-layer
encryption and key rotation.

## Upgrade note

Releases before the owner-authorized revoke change accepted a relation delete
signed by any currently trusted identity. Deploy the security update across all
writers and validating replicas before relying on the stronger rule; an old
replica can still accept an unauthorized delete that an updated replica rejects.
