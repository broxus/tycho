use serde::{Deserialize, Serialize};

/// Account state output for account generation commands.
#[derive(Debug, Serialize, Deserialize)]
pub struct AccountStateOutput {
    /// Account address as string.
    pub account: String,

    /// BOC representation of the account state in base64.
    pub boc: String,
}
