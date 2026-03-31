use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use tycho_types::cell::HashBytes;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct WuTunerConfig {
    pub wu_span: u16,
    pub wu_ma_spans: u16,
    pub wu_ma_range: u16,
    pub lag_span: u16,
    pub lag_ma_spans: u16,
    pub lag_ma_range: u16,
    pub wu_params_calc_interval: u16,
    pub tune_interval: u16,
    pub tune: WuTuneType,
    pub lag_bounds_ms: (i16, i16),
    pub target_wu_price: u16,
    pub adaptive_wu_price: bool,
}

impl Default for WuTunerConfig {
    fn default() -> Self {
        Self {
            wu_span: 10,
            wu_ma_spans: 5,
            wu_ma_range: 200,
            lag_span: 10,
            lag_ma_spans: 5,
            lag_ma_range: 200,
            wu_params_calc_interval: 100,
            tune_interval: 600,
            tune: Default::default(),
            lag_bounds_ms: (750, 1050),
            target_wu_price: 80,
            adaptive_wu_price: true,
        }
    }
}

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum WuTuneType {
    // FIXME: Look ma, no Option
    #[default]
    No,
    Rpc {
        secret: HashBytes,
        rpc: String,
    },
}

#[derive(Debug, Clone)]
pub enum WuTunerConfigState {
    Valid(Arc<WuTunerConfig>),
    Invalid(String),
}

impl WuTunerConfig {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let config: Self = tycho_util::serde_helpers::load_json_from_file(path)?;
        config.validate()?;
        Ok(config)
    }

    pub fn from_file_or_default_on_missing<P: AsRef<Path>>(path: P) -> Result<Self> {
        match Self::from_file(path) {
            Ok(config) => Ok(config),
            Err(err)
                if err
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound) =>
            {
                let config = Self::default();
                config.validate()?;
                Ok(config)
            }
            Err(err) => Err(err),
        }
    }

    fn validate(&self) -> Result<()> {
        ensure!(self.wu_span >= 1, "wu_span must be >= 1");
        ensure!(self.wu_ma_spans >= 2, "wu_ma_spans must be >= 2");
        ensure!(self.wu_ma_range >= 40, "wu_ma_range must be >= 40");
        ensure!(self.lag_span >= 1, "lag_span must be >= 1");
        ensure!(self.lag_ma_spans >= 2, "lag_ma_spans must be >= 2");
        ensure!(self.lag_ma_range >= 40, "lag_ma_range must be >= 40");
        ensure!(
            self.wu_params_calc_interval >= 40,
            "wu_params_calc_interval must be >= 40",
        );
        ensure!(self.tune_interval >= 200, "tune_interval must be >= 200");

        let wu_span = self.wu_span as u32;
        let wu_ma_range = self.wu_ma_range as u32;
        let lag_span = self.lag_span as u32;
        let lag_ma_interval = (self.lag_ma_spans as u32).saturating_mul(lag_span);
        let lag_ma_range = self.lag_ma_range as u32;
        let tune_interval = self.tune_interval as u32;

        ensure!(
            lag_ma_interval.is_multiple_of(wu_span),
            "(lag_ma_spans * lag_span) must be divisible by wu_span",
        );
        ensure!(
            wu_ma_range.is_multiple_of(wu_span),
            "wu_ma_range must be divisible by wu_span",
        );
        ensure!(
            lag_ma_range.is_multiple_of(lag_span),
            "lag_ma_range must be divisible by lag_span",
        );
        ensure!(
            std::cmp::max(std::cmp::max(wu_ma_range, lag_ma_range), lag_ma_interval)
                <= tune_interval,
            "max(wu_ma_range, lag_ma_range, lag_ma_spans * lag_span) must be <= tune_interval",
        );
        ensure!(
            self.lag_bounds_ms.0 < self.lag_bounds_ms.1,
            "lag_bounds_ms lower bound must be less than upper bound",
        );

        Ok(())
    }

    pub async fn watch_changes(
        path: PathBuf,
        config_sender: tokio::sync::watch::Sender<WuTunerConfigState>,
    ) {
        tracing::info!(
            config_path = %path.display(),
            "Started watching for changes in WuTuner config",
        );
        scopeguard::defer!({
            tracing::info!("Stopped watching for changes in WuTuner config");
        });

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

            match WuTunerConfig::from_file_or_default_on_missing(&path) {
                Ok(config) => {
                    if let Err(err) =
                        config_sender.send(WuTunerConfigState::Valid(Arc::new(config)))
                    {
                        tracing::warn!(?err, "Error send WuTuner config update");
                        break;
                    }
                    tracing::info!(config_path = %path.display(), "Reloaded WuTuner config");
                }
                Err(err) => {
                    let error = err.to_string();
                    if let Err(send_err) =
                        config_sender.send(WuTunerConfigState::Invalid(error.clone()))
                    {
                        tracing::warn!(?send_err, "Error send invalid WuTuner config update");
                        break;
                    }
                    tracing::warn!(
                        config_path = %path.display(),
                        error,
                        "Error reading WuTuner config from file",
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{WuTuneType, WuTunerConfig};

    fn make_valid_config() -> WuTunerConfig {
        WuTunerConfig {
            wu_span: 10,
            wu_ma_spans: 10,
            wu_ma_range: 300,
            lag_span: 10,
            lag_ma_spans: 10,
            lag_ma_range: 300,
            wu_params_calc_interval: 40,
            tune_interval: 500,
            tune: WuTuneType::No,
            lag_bounds_ms: (100, 5000),
            target_wu_price: 65,
            adaptive_wu_price: true,
        }
    }

    fn temp_file_path(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("tycho_wu_tuner_{prefix}_{nanos}.json"))
    }

    #[test]
    fn validate_rejects_raw_values_below_v2_minimums() {
        let mut config = make_valid_config();
        config.wu_span = 0;
        assert!(config.validate().is_err());

        let mut config = make_valid_config();
        config.wu_ma_spans = 1;
        assert!(config.validate().is_err());

        let mut config = make_valid_config();
        config.wu_ma_range = 99;
        assert!(config.validate().is_err());

        let mut config = make_valid_config();
        config.lag_span = 0;
        assert!(config.validate().is_err());

        let mut config = make_valid_config();
        config.lag_ma_spans = 1;
        assert!(config.validate().is_err());

        let mut config = make_valid_config();
        config.lag_ma_range = 99;
        assert!(config.validate().is_err());

        let mut config = make_valid_config();
        config.wu_params_calc_interval = 39;
        assert!(config.validate().is_err());

        let mut config = make_valid_config();
        config.tune_interval = 199;
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_rejects_inconsistent_ranges_and_windows() {
        let mut config = make_valid_config();
        config.wu_span = 12;
        config.lag_ma_spans = 11;
        assert!(config.validate().is_err());

        let mut config = make_valid_config();
        config.wu_span = 12;
        config.wu_ma_range = 301;
        assert!(config.validate().is_err());

        let mut config = make_valid_config();
        config.lag_span = 12;
        config.lag_ma_range = 301;
        assert!(config.validate().is_err());

        let mut config = make_valid_config();
        config.tune_interval = 250;
        assert!(config.validate().is_err());

        let mut config = make_valid_config();
        config.lag_bounds_ms = (1000, 1000);
        assert!(config.validate().is_err());
    }

    #[test]
    fn from_file_or_default_on_missing_returns_default_config() {
        let path = temp_file_path("missing");
        if path.exists() {
            std::fs::remove_file(&path).expect("failed to remove pre-existing temp file");
        }

        let loaded = WuTunerConfig::from_file_or_default_on_missing(&path)
            .expect("missing file should map to default config");

        assert_eq!(loaded.wu_span, WuTunerConfig::default().wu_span);
        assert_eq!(loaded.wu_ma_spans, WuTunerConfig::default().wu_ma_spans);
        assert_eq!(loaded.wu_ma_range, WuTunerConfig::default().wu_ma_range);
        assert_eq!(loaded.lag_span, WuTunerConfig::default().lag_span);
        assert_eq!(loaded.lag_ma_spans, WuTunerConfig::default().lag_ma_spans);
        assert_eq!(loaded.lag_ma_range, WuTunerConfig::default().lag_ma_range);
        assert_eq!(
            loaded.wu_params_calc_interval,
            WuTunerConfig::default().wu_params_calc_interval,
        );
        assert_eq!(loaded.tune_interval, WuTunerConfig::default().tune_interval);
        assert_eq!(loaded.lag_bounds_ms, WuTunerConfig::default().lag_bounds_ms);
        assert_eq!(
            loaded.target_wu_price,
            WuTunerConfig::default().target_wu_price
        );
        assert_eq!(
            loaded.adaptive_wu_price,
            WuTunerConfig::default().adaptive_wu_price,
        );
    }

    #[test]
    fn from_file_returns_error_for_invalid_existing_file() {
        let path = temp_file_path("invalid");

        std::fs::write(&path, "{ \"wu_span\": 0 }").expect("failed to write temp invalid config");

        let load_res = WuTunerConfig::from_file(&path);
        assert!(load_res.is_err());

        std::fs::remove_file(&path).expect("failed to remove temp invalid config");
    }
}
