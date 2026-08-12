//! Ed25519 identity bridge — one raw 32-byte key drives both the libp2p
//! swarm peerId and the DirectStream message-signing key.
//!
//! This is plan item 3 / FEASIBILITY.md §3: Peerbit hard-requires Ed25519
//! peerIds (`peer.ts:140`) and derives the peerId as the standard libp2p
//! Ed25519 identity-multihash over the raw 32-byte public key
//! (`crypto/src/ed25519.ts:39-41`: `peerIdFromPublicKey(publicKeyFromRaw(raw))`).
//! `libp2p::identity::Keypair::ed25519_from_bytes` over the same raw 32-byte
//! secret therefore produces a **byte-identical peerId**, and the *same* raw
//! key is what `peerbit_wire` signs frames with (`ed25519_dalek::SigningKey::
//! from_bytes` takes the identical 32-byte seed). We never let those two keys
//! drift: they are both derived here from one [`NodeIdentity`].

use libp2p::identity::{self, PublicKey};
use libp2p::PeerId;

/// A Peerbit node identity: one raw 32-byte Ed25519 secret that yields both the
/// libp2p transport identity (peerId) and the wire-signing key.
#[derive(Clone)]
pub struct NodeIdentity {
    /// The raw 32-byte Ed25519 seed. Kept so the same bytes can be handed to
    /// the wire signer; the libp2p `Keypair` below is derived from a copy.
    secret: [u8; 32],
    keypair: identity::Keypair,
    peer_id: PeerId,
}

/// Errors constructing a [`NodeIdentity`].
#[derive(Debug)]
pub enum IdentityError {
    /// The raw bytes were not a valid Ed25519 secret key.
    Decoding(String),
}

impl std::fmt::Display for IdentityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IdentityError::Decoding(message) => {
                write!(f, "failed to decode Ed25519 secret key: {message}")
            }
        }
    }
}

impl std::error::Error for IdentityError {}

impl NodeIdentity {
    /// Build an identity from a raw 32-byte Ed25519 secret key — the same raw
    /// key material a Peerbit node holds. The peerId derived here is
    /// byte-identical to `Ed25519PublicKey.toPeerId()` on the js side.
    pub fn from_ed25519_bytes(raw_secret: [u8; 32]) -> Result<Self, IdentityError> {
        // `ed25519_from_bytes` zeroizes its input, so pass a copy and keep the
        // original for the wire signer.
        let mut for_libp2p = raw_secret;
        let keypair = identity::Keypair::ed25519_from_bytes(&mut for_libp2p)
            .map_err(|error| IdentityError::Decoding(error.to_string()))?;
        let peer_id = PeerId::from(keypair.public());
        Ok(NodeIdentity {
            secret: raw_secret,
            keypair,
            peer_id,
        })
    }

    /// The libp2p keypair driving the swarm's transport identity.
    pub fn keypair(&self) -> &identity::Keypair {
        &self.keypair
    }

    /// The libp2p public key.
    pub fn public(&self) -> PublicKey {
        self.keypair.public()
    }

    /// The peerId — the standard libp2p Ed25519 identity multihash. Matches the
    /// js-side `Ed25519PublicKey.toPeerId()` for the same raw key.
    pub fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// The raw 32-byte public key — the exact bytes Peerbit hashes into the
    /// peerId AND stores as `Ed25519PublicKey.publicKey` for the routing tables
    /// / `publicKeyHash`.
    pub fn public_key_bytes(&self) -> [u8; 32] {
        // libp2p's ed25519 PublicKey::to_bytes is the raw 32-byte encoding.
        self.keypair
            .public()
            .try_into_ed25519()
            .expect("identity is ed25519 by construction")
            .to_bytes()
    }

    /// The raw 32-byte secret — the SAME key `peerbit_wire` signs with
    /// (`ed25519_dalek::SigningKey::from_bytes`). Exposing it here is what makes
    /// "one key drives both" a structural guarantee rather than a convention.
    pub fn signing_key_bytes(&self) -> [u8; 32] {
        self.secret
    }
}
