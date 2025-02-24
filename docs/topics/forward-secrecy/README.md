# Forward Secrecy in Peerbit: Current Mechanisms and Considerations
Peerbit’s encryption scheme is designed to secure data in the append-only log by combining dynamic symmetric key usage with a static X25519 key derived from the node’s ed25519 identity. In this model, each new message or log entry is protected by a fresh ephemeral symmetric key, while the X25519 key remains constant for a given identity. Below is an overview of the procedure for encrypting a new log entry and the implications for forward secrecy.

## Current Message Encryption Procedure
For each new message or log entry, the following steps are executed:

1. **Ephemeral Symmetric Key Generation**  
   A new symmetric (ephemeral) key is generated specifically for the message. This key will be used only once to encrypt that message.

2. **Message Encryption**  
   The message is encrypted using the newly generated ephemeral symmetric key. This ensures that each message is protected by a unique encryption key.

3. **Derivation of a Static X25519 Key**  
   A static X25519 key is derived from the node’s ed25519 identity. This derived key remains constant for a given node and is used to secure the transfer of the ephemeral key.

4. **Ephemeral Key Encryption**  
   The ephemeral symmetric key is then encrypted with the derived X25519 key. Only recipients—who have access to the appropriate decryption key—can decrypt this ephemeral key.

5. **Decryption at the Recipient End**  
   Upon receiving the message, the recipient first uses their version of the X25519 decryption process to recover the ephemeral symmetric key. They then use the ephemeral key to decrypt the actual message payload.

This process ensures that every message benefits from unique symmetric encryption while leveraging a long-term key (derived from the ed25519 identity) for secure key exchange.

## Forward Secrecy: Enhancements and Tradeoffs

Although Peerbit’s design already uses ephemeral symmetric keys per message, the static nature of the X25519 key introduces important considerations for forward secrecy:

### What Forward Secrecy Means in Peerbit

- **Current Protection:**  
  Each message is encrypted with a new symmetric key, so the compromise of one message does not affect others. Even if an attacker intercepts a message, without the correct ephemeral key, decryption is not possible.

- **Static X25519 Key Role:**  
  The derived X25519 key is used to protect the ephemeral symmetric key. If this static key were to be compromised, future ephemeral keys could be at risk. However, past messages remain secure because each ephemeral key was unique and used only once.

### Challenges for New Peers and Historical Data

In a forward-secrecy‑enhanced system, if all ephemeral keys are discarded after use, new peers would not be able to decrypt historical messages unless additional measures are taken. Two primary approaches to address this are:

- **Re-encryption of Historical Data:**  
  Existing nodes could periodically re-encrypt past log entries under a long-term archive key. This process, however, is resource intensive and may expose data during the re-encryption window.

- **Forward Key (FK) Mechanism:**  
  An FK is a securely managed piece of key material that allows authorized nodes to derive past ephemeral keys without storing every key individually. Implementing an FK would let new peers reconstruct the necessary ephemeral keys to access historical data. This approach, however, introduces complexity and demands strong protection of the FK itself.

### Balancing Performance and Security

Enhancing forward secrecy further in Peerbit might involve:
- **Rotating the X25519 Key:**  
  Introducing a mechanism to periodically update the static X25519 key would mean that even if a long-term key is compromised, only messages encrypted under that key would be at risk. Such rotation, however, complicates key management and may affect data replication.
- **Optional Forward Secrecy:**  
  Given the tradeoffs, forward secrecy could be offered as an optional feature. Clients that require the highest level of security for historical data can opt in, accepting a potential performance hit and additional complexity, while others continue with the current design.

## Comparison with Signal’s Model
Signal employs the Double Ratchet algorithm to provide strong forward secrecy. Each message in Signal is encrypted with a fresh key derived through an ephemeral key exchange, ensuring that even if current keys are compromised, previous messages remain protected. Peerbit’s approach—generating a new symmetric key for each message and encrypting it with a derived static X25519 key—shares a similar philosophy for individual messages. The difference lies in how the long-term key is managed:
- **Signal:** Both the session and message keys are frequently rotated, with a dedicated mechanism ensuring past keys are discarded.
- **Peerbit:** The symmetric key is new per message, but the X25519 key is static for the node. This provides a balance between ease of replication and encryption efficiency but leaves room for further enhancements if complete forward secrecy is required.

## Conclusion
Peerbit’s current encryption strategy secures each message with a unique ephemeral symmetric key that is encrypted using a static X25519 key derived from an ed25519 identity. This design offers strong per-message confidentiality but leaves open the possibility that compromise of the static key could impact future messages. Enhancing forward secrecy might involve periodic key rotation or an FK mechanism, though such changes must be balanced against increased complexity and performance costs. An optional forward secrecy mode could allow clients to choose enhanced security where needed without imposing constraints on the broader network.

## References
1. Signal Protocol Whitepaper – [Signal.org](https://signal.org/docs/specifications/doubleratchet/)