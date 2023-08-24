# Encryption
Communication between two connected nodes, is, by default, always encrypted with [libp2p-noise](https://docs.libp2p.io/concepts/secure-comm/noise/#:~:text=noise%2Dlibp2p%20is%20an%20implementation,forth%20over%20the%20secure%20channel.).

Additionally, Peerbit supports a simple form of E2E encryption that allows you to have encrypted communication between nodes that are not directly connected. This prevents relays from eavesdropping on your communication.

Every commit that is made can be E2E encrypted with public key encryption with multiple receivers. Peerbit does not *currently* support forward secrecy. 

## E2E in detail

### When appending 1 entry to a log 
1. We generate an ephemeral key that is used to encrypt the content. 
2. This key is encrypted with Public Key encryption (with one or many receivers)
3. The final message is then packed with the following. 
    - The encrypted message
    - The encrypted ephemeral key for every receiver
    - Public keys, packed as well so that a receiver can find out whether they can decrypt a message (this will be optional in the future)

See [this](./../../../packages/utils/crypto/src/encryption.ts) for implementation details. 

### Log entry fields that are encrypted (separately)
- Commit metadata, e.g. timestamps, links (references to other commits). More info [here](https://github.com/dao-xyz/peerbit/blob/464e807d679e24b897b7811ac99d6f85fbd756f9/packages/log/src/entry.ts#L141C18-L141C18).
- Payload
- Signatures



### Examples

#### Encrypted log

[encrypted-log](./encrypted-log.ts ':include')


#### Encrypted document store


Document stores, which extends the Log, can also encrypt things in the same way. 


[encrypted-log](./encrypted-document.ts ':include')
