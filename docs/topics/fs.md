# Forward Secrecy in Peerbit: Current Implementation, Tradeoffs, and Comparison with Signal

This document explains how Peerbit currently encrypts data at rest, the implications of its design without forward secrecy, and the tradeoffs of introducing forward secrecy into the system. It also compares Peerbit’s encryption model with the end-to-end encryption (E2EE) used by Signal, highlighting similarities and differences in approach.

## Current Encryption Model in Peerbit

Peerbit’s append-only log encodes data using end-to-end encryption (E2EE) with long-term keys. Every log entry is serialized (using Borsh) and then encrypted with keys derived from a node’s static identity (e.g., ed25519 and X25519 keypairs). This approach offers:

- **Simplicity:** A single set of long-term keys is used for encryption and decryption, which simplifies key management and supports straightforward replication.
- **Ease of Access:** New peers can obtain the long-term key via secure distribution and decrypt historical data.

However, a major limitation is that if a node’s long-term key is compromised, an attacker can retrospectively decrypt all previously stored data.

## Tradeoffs in Adding Forward Secrecy

Integrating forward secrecy (FS) would involve using ephemeral keys for each session or data block, ensuring that even if a long-term key is later compromised, past communications remain secure. This introduces several challenges:

### Decryption Challenges for New Peers

- **Static-Key Model:**  
  New peers can decrypt historical data because all entries use the same long-term key.

- **Ephemeral-Key Model:**  
  With FS, each entry is encrypted using a unique ephemeral key that is discarded after use. Consequently, a new peer will not have access to the ephemeral keys used in past sessions. Two potential solutions are:
  
  1. **Re-encryption of Historical Data:**  
     Existing nodes would re-encrypt past data using a long-term archive key. While effective, this approach is resource-intensive and introduces a temporary window of vulnerability.
  
  2. **Forward Key (FK) Mechanism:**  
     Instead of discarding all ephemeral keys, a portion of the key material (the Forward Key, or FK) is retained securely. The FK acts as a seed to reconstruct the ephemeral keys for previous sessions, enabling new peers to derive them without having direct access to each ephemeral key. This method requires stringent security controls for the FK.

### Additional Tradeoffs

- **Increased Complexity and Overhead:**  
  Ephemeral key exchanges (e.g., using ephemeral Diffie–Hellman) add computational overhead and complicate key management.

- **Optionality:**  
  Due to these tradeoffs, forward secrecy could be offered as an optional feature. Clients needing the highest security can opt in for FS despite potential performance penalties, while others continue using the simpler static-key model.

## Comparison with Signal’s End-to-End Encryption

Signal is widely regarded for its robust E2EE, which is built on the Double Ratchet algorithm and uses ephemeral key exchanges to provide forward secrecy and deniability.

- **Ephemeral Keys and Forward Secrecy:**  
  Signal generates a new ephemeral key for each message exchange. The Double Ratchet mechanism ensures that even if a key is compromised, past messages remain secure. In contrast, Peerbit’s current model uses static long-term keys, meaning that if a key is compromised, all historical data can be decrypted.

- **Key Management and Data Replication:**  
  Signal’s approach is optimized for real-time messaging, where the overhead of frequent key exchanges is acceptable for high privacy. Peerbit’s design, however, prioritizes data replication and persistence across a distributed network, which is simplified by using a single set of keys. Introducing FS into Peerbit would add complexity to data replication and require mechanisms (like FK or re-encryption) to ensure that new peers can access historical data.

- **Potential for Optional FS:**  
  Lessons from Signal’s design can inform an optional FS feature in Peerbit. Clients opting for FS would benefit from enhanced security for past data, while those prioritizing performance and replication simplicity could continue with the current approach.

For further reading on Signal’s encryption, see:
- [The Signal Protocol Whitepaper](https://signal.org/docs/specifications/doubleratchet/)
- [Signal’s Technical Overview by Open Whisper Systems](https://signal.org)

## The Concept of a Forward Key (FK)

A Forward Key (FK) is an additional piece of key material maintained during forward secrecy implementations. Instead of discarding ephemeral keys after each session, a portion of the key material is securely stored or derivable, allowing authorized nodes to reconstruct the ephemeral keys for previous sessions.

**Key aspects of FK:**
- **Controlled Storage:** The FK must be stored securely (e.g., using threshold cryptography or secret sharing) to prevent its compromise.
- **Regeneration of Ephemeral Keys:** The FK acts as a seed to derive ephemeral keys for historical sessions, enabling new peers to decrypt past data without re-encryption.
- **Security Tradeoff:** While useful for data accessibility, the FK introduces a potential vulnerability if it is not adequately protected. Therefore, its use must be carefully isolated from day-to-day operations to maintain the benefits of forward secrecy.

## Conclusion

Peerbit currently relies on static long-term keys for data encryption, offering simplicity and ease of replication at the expense of forward secrecy. Integrating forward secrecy—by using ephemeral keys and potentially a Forward Key (FK) mechanism—would enhance the security of historical data but introduces complexity, performance overhead, and challenges for new peers needing to decrypt past data.

Signal’s E2EE model, which uses the Double Ratchet algorithm and ephemeral keys, provides a useful point of comparison. Although Signal’s method is highly effective for real-time messaging, its complexity may not suit all aspects of a distributed, data-replicated system like Peerbit. Thus, offering forward secrecy as an optional feature provides a balanced approach, allowing clients to choose enhanced security when needed.


## References
1. Signal Protocol Double Ratchet aglorithm specification. [Signal.org](https://signal.org/docs/specifications/doubleratchet/)

