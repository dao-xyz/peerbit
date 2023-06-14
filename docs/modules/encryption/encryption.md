# Encryption
Communication between two connected nodes, are by default always encrypted with [libp2p-noise](https://docs.libp2p.io/concepts/secure-comm/noise/#:~:text=noise%2Dlibp2p%20is%20an%20implementation,forth%20over%20the%20secure%20channel.).

Additionally, Peerbit supports a simple form of E2E encryption. That allows you to have encrypted communication between nodes that are not directly connected. This prevents relays to eavesdrop on your communication.

Every commit that is made can be E2E encrypted with public key encryption with multiple receivers. Peerbit does not *currently* support forward secrecy. 

## E2E in detail

### When appending 1 entry to a log 
1. We generate an ephemeral key that is used to encrypt the content with. 
2. This key is encrypted with Public Key encryption (with one or many recievers)
3. The final message then is packed with the following. 
    - The encrypted message
    - The encrypted ephemeral key for every receiver
    - Public keys are packed along side so that an receiver can find out whether they can decrypt a message (this will be optional in the future)

See [this](./../../../packages/utils/crypto/src/encryption.ts) for implementation details. 

### Log entry fields that are encrypted (separately)
- Commit metadata
- Payload
- Signatures
- Links (references to other commits)

See below for an examples with a Log

[encrypted-log](./encrypted-log.ts ':include')


Document stores, which extends the Log, can also encrypt things in the same way. See below as an example


[encrypted-log](./encrypted-document-store.ts ':include')
