use std::pin::Pin;
use std::sync::Arc;
use anyhow::Result;
use tycho_types::models::{BlockchainConfigParams, WorkUnitsParams};
use tycho_wu_tuner::WuTunerConfig;
use crate::cmd::tools;
use crate::util::jrpc_client::JrpcClient;
const BC_PARAM_IDX: u32 = 28;
pub fn update_wu_params(
    config: Arc<WuTunerConfig>,
    target_wu_params: WorkUnitsParams,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
    Box::pin(async move {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::async_block"),
            file!(),
            17u32,
        );
        let Some(rpc_config) = config.tune.get_wu_tuner_rpc_config()? else {
            {
                __guard.end_section(19u32);
                return Ok(());
            };
        };
        let client = JrpcClient::new(rpc_config.rpc)?;
        let curr_bc_config = {
            __guard.end_section(26u32);
            let __result = client.get_config().await;
            __guard.start_section(26u32);
            __result
        }?;
        let mut collation_config = curr_bc_config.config.get_collation_config()?;
        collation_config.work_units_params = target_wu_params;
        let mut params = serde_json::from_value::<
            BlockchainConfigParams,
        >(serde_json::Map::new().into())?;
        params.set_collation_config(&collation_config)?;
        let value = params
            .as_dict()
            .get(BC_PARAM_IDX)?
            .expect("it is guaranteed that idx 28 exists here");
        {
            __guard.end_section(51u32);
            let __result = tools::bc::send_config_action_ext(
                    &client,
                    tools::bc::Action::SubmitParam {
                        index: BC_PARAM_IDX,
                        value,
                    },
                    &rpc_config.keypair,
                    40,
                )
                .await;
            __guard.start_section(51u32);
            __result
        }
    })
}
