use std::sync::Arc;

use anyhow::Result;
use reqwest::Url;
use tycho_crypto::ed25519;
use tycho_types::models::WorkUnitsParams;

use crate::config::{WuTuneType, WuTunerConfig};

pub trait WuParamsUpdater {
    fn update_wu_params(
        &self,
        config: Arc<WuTunerConfig>,
        target_wu_params: WorkUnitsParams,
    ) -> impl Future<Output = Result<()>> + Send;
}

impl<F, R> WuParamsUpdater for F
where
    F: Fn(Arc<WuTunerConfig>, WorkUnitsParams) -> R,
    R: Future<Output = Result<()>> + Send,
{
    fn update_wu_params(
        &self,
        config: Arc<WuTunerConfig>,
        target_wu_params: WorkUnitsParams,
    ) -> impl Future<Output = Result<()>> + Send {
        self(config, target_wu_params)
    }
}

pub struct WuTunerRpcConfig {
    pub rpc: Url,
    pub keypair: ed25519::KeyPair,
}

impl WuTuneType {
    pub fn get_wu_tuner_rpc_config(&self) -> Result<Option<WuTunerRpcConfig>> {
        let WuTuneType::Rpc { secret, rpc } = &self else {
            return Ok(None);
        };

        // parse config
        let rpc = Url::parse(rpc)?;
        let secret_key = ed25519::SecretKey::from_bytes(secret.0);
        let keypair = ed25519::KeyPair::from(&secret_key);

        Ok(Some(WuTunerRpcConfig { rpc, keypair }))
    }
}
