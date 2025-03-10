use serde::Serialize;

/// Output model for config-related operations.
#[derive(Debug, Serialize)]
pub struct ConfigOutput {
    /// Operation status.
    pub success: bool,
    
    /// Path to the generated config file.
    pub path: String,
}