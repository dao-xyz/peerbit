# Encryption
Peerbit supports a simple form of E2EE encryption. 
Every commit that is made can be encrypted with public key encryption with multiple receivers. Peerbit does *not* currently support forward secrecy 

## In detail

### For every datapoint we want to encrypt
1. We generate an ephemeral key that is used to encrypt the content with. 
2. This key is encrypted with Public Key encryption (with one or many recievers)
3. The final message then is packed with the following. 
    - The encrypted message
    - The encrypted ephemeral key for every receiver
    - Public keys are packed along side so that an receiver can find out whether they can decrypt a message (this will be optional in the future)

### Log entry fields that are encrypted (separately)
- Lamport clock
- Payload
- Signature

(Work is still under development whether DAGs links should also be encrypted, and whether Lamport clock encryption is really necessary. )