use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OverlayClientSettings {
    pub overlay_options: OverlayOptions,
    pub neighbours_options: NeighboursOptions
}

impl Default for OverlayClientSettings {
    fn default() -> Self {
        Self {
            overlay_options: Default::default(),
            neighbours_options: Default::default()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OverlayOptions {
    pub neighbours_update_interval: u64,
    pub neighbours_ping_interval: u64,
}

impl Default for OverlayOptions {
    fn default() -> Self {
        Self {
            neighbours_update_interval: 60 * 2 * 1000,
            neighbours_ping_interval: 2000
        }
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NeighboursOptions {
    pub max_neighbours: usize,
    pub max_ping_tasks: usize,
    pub default_roundtrip_ms: u64,
}

impl Default for NeighboursOptions {
    fn default() -> Self {
        Self {
            max_neighbours: 16,
            max_ping_tasks: 6,
            default_roundtrip_ms: 2000,
        }
    }
}
