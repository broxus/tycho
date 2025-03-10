use serde::Serialize;

/// Keypair model output for key generation commands.
#[derive(Debug, Serialize)]
pub struct KeypairOutput {
    /// Public key in hex format.
    pub public: String,
    
    /// Secret key in hex format.
    pub secret: String,
}