use tycho_types::models::GenesisInfo;

pub trait GenesisInfoExt {
    /// Check if equal or overrides (>=)
    fn eq_or_overrides(&self, other: &Self) -> bool;
}

impl GenesisInfoExt for GenesisInfo {
    fn eq_or_overrides(&self, other: &Self) -> bool {
        self == other || self.overrides(other)
    }
}
