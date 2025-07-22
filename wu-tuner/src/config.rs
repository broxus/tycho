use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tycho_types::cell::HashBytes;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct WuTunerConfig {
    pub wu_span: u16,
    pub wu_ma_interval: u16,
    pub lag_span: u16,
    pub lag_ma_interval: u16,
    pub tune_interval: u16,
    pub tune: WuTuneType,
    pub max_lag_ms: u16,
    pub target_wu_price: u16,
}

impl Default for WuTunerConfig {
    fn default() -> Self {
        Self {
            wu_span: 10,
            wu_ma_interval: 100,
            lag_span: 10,
            lag_ma_interval: 10,
            tune_interval: 2000,
            tune: Default::default(),
            max_lag_ms: 300,
            target_wu_price: 70,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum WuTuneType {
    No,
    Rpc { secret: HashBytes, rpc: String },
}

impl Default for WuTuneType {
    fn default() -> Self {
        Self::No
    }
}

impl WuTunerConfig {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        tycho_util::serde_helpers::load_json_from_file(path)
    }

    pub async fn watch_changes(
        path: PathBuf,
        config_sender: tokio::sync::watch::Sender<Arc<WuTunerConfig>>,
    ) {
        tracing::info!(
            config_path = %path.display(),
            "Started watching for changes in WuTuner config",
        );

        let config_path = path.clone();
        let get_metadata = move || {
            std::fs::metadata(&config_path)
                .ok()
                .and_then(|m| m.modified().ok())
        };

        let mut last_modified = get_metadata();

        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;

            let modified = get_metadata();
            if last_modified == modified {
                continue;
            }
            last_modified = modified;

            match WuTunerConfig::from_file(&path) {
                Ok(config) => {
                    if let Err(err) = config_sender.send(Arc::new(config)) {
                        tracing::warn!(?err, "Error send WuTuner config update");
                        break;
                    }
                    tracing::info!(config_path = %path.display(), "Reloaded WuTuner config");
                }
                Err(err) => {
                    tracing::warn!(
                        config_path = %path.display(),
                        ?err,
                        "Error reading WuTuner config from file",
                    );
                }
            }
        }

        tracing::info!("Stopped watching for changes in WuTuner config");
    }
}
