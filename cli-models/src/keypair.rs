use serde::{Deserialize, Serialize};

/// Keypair model output for key generation commands.
#[derive(Debug, Serialize, Deserialize)]
pub struct KeypairOutput {
    /// Public key in hex format.
    pub public: String,

    /// Secret key in hex format.
    pub secret: String,
}
