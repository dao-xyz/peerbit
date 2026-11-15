[Forked from](https://github.com/orbitdb/ipfs-log) 

Additional features
+ Binary ser/der
+ Log "cutting"
+ Typescript
+ Graph ids (gid) 

Other changes
+ Removal of entry refs (improve load speeds can be achieved in other way)
+ ESM only

## Timestamp trust boundary

Entry timestamps are author-asserted ordering metadata, not trusted physical
time. `Log.append(data, { meta: { timestamp } })` accepts an explicit `Timestamp`;
a signature authenticates the signed value, not its real-world accuracy.

The default `Log` hybrid logical clock (HLC) has no future-skew limit relative to
the receiving clock. Joining an admissible future-dated entry can advance the
local HLC, so subsequent appends using automatic timestamps can inherit that
future wall time. Clock advancement is not proof of a successful commit or
durable replication. Timestamps are not numerically unlimited: `wallTime` is an
unsigned 64-bit nanosecond value, and the HLC enforces its overflow limit.

In mutable `Documents` stores with `strictHistory: false` (the default), entry
wall time influences same-ID winner selection. `strictHistory` validates history
links; it does not establish trusted time or impose a clock-skew limit. Do not
use entry `wallTime` alone as an authorization, revocation, expiry, or safe
retention boundary.
