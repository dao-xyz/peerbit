# Fanout Tree Protocol (WIP)

This is the current wire-level shape for the experimental `FanoutTree` data-plane protocol.

It is **not** the final design yet; we expect breaking changes and will bump the multicodec when we do.

## Multicodec

`/peerbit/fanout-tree/0.4.0`

## Channel identity

A channel is identified by `(root, topic)`.

We derive a 32-byte `channelKey`:

`channelKey = sha256("fanout-tree|${root}|${topic}")`

To avoid storing the full 32-byte key in every data frame, we also use:
- `suffixKey = base64(channelKey[0..24))` (24 bytes)

## Data message id (dedup key)

Each forwarded data message uses a stable 32-byte header id:

- bytes `0..4`: ASCII `"FOUT"`
- bytes `4..8`: big-endian `seq` (u32)
- bytes `8..32`: `channelKey[0..24)`

This makes the stream-level duplicate filter treat all forwarded copies of the same `(channel, seq)` as the same logical message.

Forwarding behavior:
- Relays forward the **original `DataMessage` bytes** (root signature preserved); they do **not** re-sign at each hop.
- This keeps `MessageHeader.timestamp` stable across the tree (useful for deadline-oriented workloads).

## Message kinds

All control messages begin with:

- byte `0`: `kind` (u8)
- bytes `1..33`: `channelKey` (32 bytes)

### `JOIN_REQ` (kind = 1)

Child → parent request.

- `reqId` (u32) at bytes `33..37`
- `bidPerByte` (u32) at bytes `37..41`

### `JOIN_ACCEPT` (kind = 2)

Parent → child accept.

- `reqId` (u32)
- `parentLevel` (u16)
- `routeCount` (u8)
- `routeCount * (hashLen (u8) + hashBytes)` where `hashBytes` is utf-8 `PublicSignKey.hashcode()`

`route` is the parent’s current source-route **from root to parent** (inclusive):

`route = [root, ..., parent]`

Child sets:
- `parent = fromHash`
- `level = parentLevel + 1`
- `routeFromRoot = route + [selfHash]`

### `JOIN_REJECT` (kind = 3)

Parent → child reject.

- `reqId` (u32)
- `reason` (u8)
- *(optional, WIP)* `redirectCount` (u8) followed by redirect entries:
  - `hashLen` (u8) + `hash` bytes (utf-8)
  - `addrCount` (u8)
  - `addrCount * (addrLen (u16) + addrBytes)` where `addrBytes` is a raw multiaddr byte array

Current reason codes are not stable yet.

Redirect entries are a best-effort hint: on reject, the receiver can try dialing/joining the suggested peers before re-querying bootstraps.

### `KICK` (kind = 4)

Parent → child kick (child should detach and rejoin).

### `DATA` (kind = 10)

Data plane payload.

Payload begins at byte `1` (`data[0]` is the kind byte).

The `(channel, seq)` is derived from the 32-byte message id described above.

Optional local policy (not on-wire):
- A relay may drop forwarding if the message age exceeds a configured threshold (`maxDataAgeMs`).

### `END` (kind = 11)

Root/parent → subtree signal that publishing has ended at `lastSeqExclusive` (u32).

Receivers use this to detect tail gaps and trigger repair.

### `REPAIR_REQ` (kind = 20)

Child → parent request to re-send missing sequences.

- `reqId` (u32)
- `count` (u8)
- `count * u32` missing seq list

Parent replies by re-sending cached data messages for requested seqs that are still in its window.

### `FETCH_REQ` (kind = 21) *(WIP / experimental)*

Peer → peer request to fetch cached sequences, used for **neighbor-assisted repair**.

Wire format matches `REPAIR_REQ`:
- `reqId` (u32)
- `count` (u8)
- `count * u32` missing seq list

Receiver replies by re-sending any cached data messages for requested seqs.

### `IHAVE` (kind = 22) *(WIP / experimental)*

Peer → peer **cache summary** used to improve neighbor-assisted repair target selection.

This is inspired by GossipSub-style `IHAVE`, but simplified for a single-writer, sequence-numbered stream:
we advertise a **contiguous cached range**.

- `haveFrom` (u32)
- `haveToExclusive` (u32)

Semantics: the sender claims it can serve any cached payload for `seq ∈ [haveFrom, haveToExclusive)`.

### `UNICAST` (kind = 12) *(new in 0.3.0)*

Economical unicast **within a channel**, using a source-route token (no global membership map).

Wire format:
- `routeCount` (u8)
- `routeCount * (hashLen (u8) + hashBytes)` where `hashBytes` is utf-8 `PublicSignKey.hashcode()`
- `payload` (remaining bytes)

Semantics:
- `route = [root, ..., target]` (obtained out-of-band, e.g. target shares `routeFromRoot`)
- Non-root nodes forward upstream to `parent` until the message reaches `root`.
- `root` forwards downstream along `route` by sending to `route[1]`, then `route[2]`, ... until `target`.
- Only tree edges are used (parent/child), so forwarding stays bounded and economical.

### `ROUTE_ANNOUNCE` (kind = 15) *(new in 0.4.0)*

Child/relay → parent route advertisement.

Wire format:
- `routeCount` (u8)
- `routeCount * (hashLen (u8) + hashBytes)` where `hashBytes` is utf-8 `PublicSignKey.hashcode()`

Semantics:
- Carries the sender's current root route token: `route = [root, ..., self]`.
- Receiver caches route by `target = route[route.length - 1]`.
- Non-root relays forward announces upstream to their parent.
- This keeps route-token lookup local and incremental (no global member list exchange).
- Implementations should treat route caches as bounded/expiring (size cap + TTL), and rely on periodic `ROUTE_ANNOUNCE` refreshes.

### `ROUTE_QUERY` (kind = 13) *(new in 0.4.0)*

Child/relay → parent query for a target route token.

Wire format:
- `reqId` (u32)
- `targetHashLen` (u8)
- `targetHash` (utf8 bytes)

Semantics:
- Receiver responds with `ROUTE_REPLY`.
- If receiver has cached route, it may reply immediately.
- If receiver is root and the target is a direct child, it may synthesize `[root, target]`.
- If receiver is an intermediate relay and does not know the route, it may proxy upstream and map request ids hop-by-hop.
- If a parent/root misses cache, it may fan out a bounded subtree lookup and return the first valid reply (or empty if unresolved/timeout).
- This allows route resolution to recover from cache expiry without exchanging global membership maps.

### `ROUTE_REPLY` (kind = 14) *(new in 0.4.0)*

Parent/relay → child/relay response for route lookup.

Wire format:
- `reqId` (u32)
- `routeCount` (u8)
- `routeCount * (hashLen (u8) + hashBytes)`

Semantics:
- Empty route (`routeCount=0`) means route unknown.
- Non-empty route is a route token: `[root, ..., target]`.
- Nodes may cache valid replies for subsequent `unicastTo(...)` sends.
- Under bounded caches, a route can temporarily miss and later become available again via refresh announces.

---

## Bootstrap tracker (rendezvous) messages (WIP)

These messages are a **join/bootstrapping control plane**. They are not used per data message.

Nodes with capacity announce themselves to one or more bootstrap servers (trackers), and joiners query
those trackers for candidate parents.

### `TRACKER_ANNOUNCE` (kind = 30)

Relay/root → tracker.

Tracker stores an expiring `(channelKey -> candidate)` entry keyed by the sender.

- `ttlMs` (u32)
- `level` (u16)
- `maxChildren` (u16) *(currently unused by selection, but included for future)*
- `freeSlots` (u16)
- `bidPerByte` (u32)
- `addrCount` (u8)
- `addrCount * (u16 len + bytes)` multiaddr list

If `ttlMs == 0`, tracker should treat this as a delete for that candidate.

### `TRACKER_QUERY` (kind = 31)

Joiner → tracker.

- `reqId` (u32)
- `want` (u16) number of candidates desired

### `TRACKER_REPLY` (kind = 32)

Tracker → joiner.

- `reqId` (u32)
- `count` (u8)
- `count` entries:
  - `hashLen` (u8)
  - `hash` (utf8 bytes) *(Peerbit publicKey hashcode)*
  - `level` (u16)
  - `freeSlots` (u16)
  - `bidPerByte` (u32)
  - `addrCount` (u8)
  - `addrCount * (u16 len + bytes)` multiaddr list

The joiner dials the returned multiaddrs (libp2p), then attempts a normal `JOIN_REQ` with the candidate.

### `TRACKER_FEEDBACK` (kind = 33) *(WIP / experimental)*

Joiner/relay → tracker.

Used to help trackers converge faster when their cached `freeSlots` or reachability info is stale.

- `hashLen` (u8)
- `candidateHash` (utf8 bytes)
- `event` (u8)
  - `1` joined (tracker may decrement `freeSlots`)
  - `2` dial failed (tracker should drop the entry)
  - `3` join timed out (tracker should drop the entry)
  - `4` join rejected (see `reason`)
- `reason` (u8) *(only meaningful for `event=4`)*
  - `1` not attached
  - `2` no capacity
  - `3` bid too low

## Current acceptance rules (high level)

Parent accepts a new child iff:
- parent is the root **or** parent already has a parent (i.e. is attached), and
- parent has available child capacity (bounded by `uploadLimitBps` and `maxChildren`), or
- `allowKick` is enabled and the joining child outbids the current worst child.

## Repair model (high level)

- Each node tracks `nextExpectedSeq` and a set of missing sequences for its parent link.
- Each node caches a fixed window of recent payloads (`repairWindowMessages`) for re-send.
- Children periodically request missing seqs (`repairIntervalMs`, bounded by `repairMaxPerReq`).
- Optional: nodes may also query a small set of additional peers (`FETCH_REQ`) as a stepping stone towards Plumtree-style neighbor-assisted repair.
- Optional: nodes may exchange `IHAVE` summaries with a small "lazy repair mesh" and prefer fetching from peers that recently advertised they have the missing ranges.
