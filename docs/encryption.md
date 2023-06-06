# Encryption
Peerbit supports a simple form of E2EE encryption. 
Every commit that is made can be encrypted with public key encryption with multiple receivers. Peerbit does not *currently* support forward secrecy 

## In detail

### For every datapoint we want to encrypt
1. We generate an ephemeral key that is used to encrypt the content with. 
2. This key is encrypted with Public Key encryption (with one or many recievers)
3. The final message then is packed with the following. 
    - The encrypted message
    - The encrypted ephemeral key for every receiver
    - Public keys are packed along side so that an receiver can find out whether they can decrypt a message (this will be optional in the future)

See [this](./../packages/utils/crypto/src/encryption.ts) for implementation details. 

### Log entry fields that are encrypted (separately)
- Commit metadata
- Payload
- Signature
- Links (references to other commits)

See [this](./../packages/ipfs/ipfs-log/src/entry.ts) for implementation details. 


In practice, this is how it looks when you are inserting a document into a Document Store
```typescript

let doc = new Document({
    id: "123",
    name: "this document is not for everyone",
});

const someKey = await X25519PublicKey.create(); // a random reciever

// save document and send it to peers
const entry = await db.docs.put(doc, {
    reciever: {
        payload: [someKey],
        metadata: undefined,
        next: undefined,
        signatures: undefined,
    },
});

```
