use std::future::Future;
use std::io::{IsTerminal, Read};
use std::path::PathBuf;

use anyhow::{Context, Result};
use everscale_crypto::ed25519;
use everscale_types::abi::extend_signature_with_id;
use everscale_types::boc::Boc;
use everscale_types::cell::{Cell, CellBuilder, CellSliceRange};
use everscale_types::models::{
    AccountState, BlockchainConfigParams, ExtInMsgInfo, GlobalCapability, MsgInfo, OwnedMessage,
    StdAddr,
};
use reqwest::Url;
use tycho_util::cli::signal;
use tycho_util::futures::JoinTask;
use tycho_util::time::now_sec;

use crate::util::jrpc_client::{AccountStateResponse, JrpcClient};
use crate::util::{parse_public_key, parse_secret_key, print_json};

/// Blockchain stuff
#[derive(clap::Parser)]
pub struct Cmd {
    #[clap(subcommand)]
    cmd: SubCmd,
}

impl Cmd {
    pub fn run(self) -> Result<()> {
        bc_rt(move || async move {
            match self.cmd {
                SubCmd::GetParam(cmd) => cmd.run().await,
                SubCmd::SetParam(cmd) => cmd.run().await,
                SubCmd::SetMasterKey(cmd) => cmd.run().await,
                SubCmd::SetElectorCode(cmd) => cmd.run().await,
                SubCmd::SetConfigCode(cmd) => cmd.run().await,
            }
        })
    }
}

#[derive(clap::Parser)]
enum SubCmd {
    GetParam(GetParamCmd),
    SetParam(SetParamCmd),
    SetMasterKey(SetMasterKeyCmd),
    SetElectorCode(SetElectorCode),
    SetConfigCode(SetConfigCode),
}

/// Get blockchain config parameter
#[derive(clap::Parser)]
pub struct GetParamCmd {
    /// parameter index
    param: i32,

    /// RPC url
    #[clap(long)]
    rpc: Url,
}

impl GetParamCmd {
    async fn run(self) -> Result<()> {
        let client = JrpcClient::new(self.rpc)?;
        let res = client.get_config().await?;
        let params = serde_json::to_value(res.config.params)?;
        let item = &params.get(self.param.to_string());

        let output = serde_json::json!({
            "global_id": res.global_id,
            "seqno": res.seqno,
            "param": item,
        });
        print_json(output)
    }
}

/// Set blockchain config parameter
#[derive(clap::Parser)]
pub struct SetParamCmd {
    /// parameter index
    param: i32,

    /// parameter value
    value: String,

    /// RPC url
    #[clap(long)]
    rpc: Url,

    #[clap(flatten)]
    sign: KeyArgs,
}

impl SetParamCmd {
    async fn run(self) -> Result<()> {
        let client = JrpcClient::new(self.rpc)?;

        let index = self.param as u32;

        let value = {
            let value =
                serde_json::from_str::<serde_json::Value>(&self.value).context("invalid value")?;

            let mut params = serde_json::Map::new();
            params.insert(index.to_string(), value);
            let params = serde_json::from_value::<BlockchainConfigParams>(params.into())?;
            params.as_dict().get(index)?.context("invalid param")?
        };

        send_config_action(&client, Action::SubmitParam { index, value }, &self.sign).await
    }
}

/// Set blockchain config key
#[derive(clap::Parser)]
pub struct SetMasterKeyCmd {
    /// new public key
    pubkey: String,

    /// RPC url
    rpc: Url,

    #[clap(flatten)]
    sign: KeyArgs,
}

impl SetMasterKeyCmd {
    async fn run(self) -> Result<()> {
        let client = JrpcClient::new(self.rpc)?;
        let pubkey = parse_public_key(self.pubkey.as_bytes(), false)?;
        send_config_action(&client, Action::UpdateMasterKey { pubkey }, &self.sign).await
    }
}

/// Set elector contract code
#[derive(clap::Parser)]
pub struct SetElectorCode {
    /// path to the elector code BOC
    code_path: PathBuf,

    /// optional parameters for `after_code_upgrade`
    #[clap(long)]
    upgrade_args: Option<String>,

    /// RPC url
    rpc: Url,

    #[clap(flatten)]
    sign: KeyArgs,
}

impl SetElectorCode {
    async fn run(self) -> Result<()> {
        let client = JrpcClient::new(self.rpc)?;

        let code = std::fs::read(&self.code_path).context("failed to read elector code file")?;
        let code = Boc::decode(code).context("invalid elector code")?;

        let params = self
            .upgrade_args
            .map(Boc::decode_base64)
            .transpose()
            .context("invalid upgrade params")?;

        send_config_action(
            &client,
            Action::UpdateElectorCode { code, params },
            &self.sign,
        )
        .await
    }
}

/// Set config contract code
#[derive(clap::Parser)]
pub struct SetConfigCode {
    /// path to the config code BOC
    code_path: PathBuf,

    /// optional parameters for `after_code_upgrade`
    #[clap(long)]
    upgrade_args: Option<String>,

    /// RPC url
    rpc: Url,

    #[clap(flatten)]
    sign: KeyArgs,
}

impl SetConfigCode {
    async fn run(self) -> Result<()> {
        let client = JrpcClient::new(self.rpc)?;

        let code = std::fs::read(&self.code_path).context("failed to read elector code file")?;
        let code = Boc::decode(code).context("invalid elector code")?;

        let params = self
            .upgrade_args
            .map(Boc::decode_base64)
            .transpose()
            .context("invalid upgrade params")?;

        send_config_action(
            &client,
            Action::UpdateConfigCode { code, params },
            &self.sign,
        )
        .await
    }
}

#[derive(clap::Args)]
struct KeyArgs {
    /// message ttl
    #[clap(long, value_parser, default_value_t = DEFAULT_TTL)]
    ttl: u32,

    /// secret key (reads from stdin if only flag is provided)
    #[clap(long)]
    #[allow(clippy::option_option)]
    key: Option<String>,

    /// expect a raw key input (32 bytes)
    #[clap(short, long)]
    raw_key: bool,
}

impl KeyArgs {
    fn get_keypair(&self) -> Result<ed25519::KeyPair> {
        let key = match &self.key {
            Some(key) => key.clone().into_bytes(),
            None if std::io::stdin().is_terminal() => {
                anyhow::bail!("expected a `key` param or an stdin input");
            }
            None => {
                let mut key = Vec::new();
                std::io::stdin().read_to_end(&mut key)?;
                key
            }
        };
        let secret_key = parse_secret_key(&key, self.raw_key)?;
        Ok(ed25519::KeyPair::from(&secret_key))
    }
}

async fn send_config_action(client: &JrpcClient, action: Action, sign: &KeyArgs) -> Result<()> {
    let keypair = sign.get_keypair()?;

    let res = client.get_config().await?;
    let config_addr = StdAddr::new(-1, res.config.address);

    let global_version = res.config.get_global_version()?;
    let signature_id = global_version
        .capabilities
        .contains(GlobalCapability::CapSignatureWithId)
        .then_some(res.global_id);

    let seqno = prepare_action(client, &config_addr, &keypair.public_key).await?;
    let (message, expire_at) = create_message(
        seqno,
        &config_addr,
        action,
        &keypair,
        signature_id,
        sign.ttl,
    )?;

    let message_cell = CellBuilder::build_from(message)?;
    client.send_message(message_cell.as_ref()).await?;

    let output = serde_json::json!({
        "expire_at": expire_at,
        "message_hash": message_cell.repr_hash(),
        "message": Boc::encode_base64(message_cell.as_ref()),
    });
    print_json(output)
}

async fn prepare_action(
    client: &JrpcClient,
    config_addr: &StdAddr,
    expected_pubkey: &ed25519::PublicKey,
) -> Result<u32> {
    let state_init = match client.get_account(config_addr).await? {
        AccountStateResponse::Exists { account, .. } => match account.state {
            AccountState::Active(state_init) => state_init,
            AccountState::Frozen { .. } => anyhow::bail!("config account is frozen"),
            AccountState::Uninit => anyhow::bail!("config account is uninit"),
        },
        AccountStateResponse::NotExists { .. } => anyhow::bail!("config account not found"),
        AccountStateResponse::Unchanged { .. } => anyhow::bail!("unexpected response"),
    };

    let data = state_init.data.context("config contract data is missing")?;
    let mut data = data.as_slice()?;

    let seqno = data.load_u32().context("Failed to get seqno")?;

    let mut buffer = [0u8; 32];
    data.load_raw(&mut buffer, 256)
        .context("Failed to get public key")?;
    let public = ed25519::PublicKey::from_bytes(buffer).context("Invalid config public key")?;

    anyhow::ensure!(
        &public == expected_pubkey,
        "Config has a different public key"
    );

    Ok(seqno)
}

fn create_message(
    seqno: u32,
    config_addr: &StdAddr,
    action: Action,
    keypair: &ed25519::KeyPair,
    signature_id: Option<i32>,
    ttl: u32,
) -> Result<(Box<OwnedMessage>, u32)> {
    let (action, data) = action.build().context("Failed to build action")?;

    let now = now_sec();
    let expire_at = now + ttl;

    let mut builder = CellBuilder::new();
    builder.store_u32(action)?; // action
    builder.store_u32(seqno)?; // msg_seqno
    builder.store_u32(expire_at)?; // valid_until
    builder.store_builder(&data)?; // action data

    let hash = *builder.clone().build()?.repr_hash();
    let data = extend_signature_with_id(hash.as_slice(), signature_id);
    let signature = keypair.sign_raw(&data);
    builder.prepend_raw(&signature, 512)?;

    let body = builder.build()?;
    let body_range = CellSliceRange::full(body.as_ref());

    let message = Box::new(OwnedMessage {
        info: MsgInfo::ExtIn(ExtInMsgInfo {
            dst: config_addr.clone().into(),
            ..Default::default()
        }),
        init: None,
        body: (body, body_range),
        layout: None,
    });

    Ok((message, expire_at))
}

#[derive(Debug, Clone)]
enum Action {
    /// Param index and param value
    SubmitParam { index: u32, value: Cell },

    /// New config public key
    UpdateMasterKey { pubkey: ed25519::PublicKey },

    /// First ref is elector code.
    /// Remaining data is passed to `after_code_upgrade`
    UpdateElectorCode { code: Cell, params: Option<Cell> },

    /// First ref is config code.
    /// Remaining data is passed to `after_code_upgrade`
    UpdateConfigCode { code: Cell, params: Option<Cell> },
}

impl Action {
    fn build(self) -> Result<(u32, CellBuilder)> {
        let mut data = CellBuilder::new();

        Ok(match self {
            Self::SubmitParam { index, value } => {
                data.store_u32(index)?;
                data.store_reference(value)?;
                (0x43665021, data)
            }
            Self::UpdateMasterKey { pubkey } => {
                data.store_raw(pubkey.as_bytes(), 256)?;
                (0x50624b21, data)
            }
            Self::UpdateElectorCode { code, params } => {
                data.store_reference(code)?;
                if let Some(params) = params {
                    data.store_slice(params.as_slice()?)?;
                }
                (0x4e43ef05, data)
            }
            Self::UpdateConfigCode { code, params } => {
                data.store_reference(code)?;
                if let Some(params) = params {
                    data.store_slice(params.as_slice()?)?;
                }
                (0x4e436f64, data)
            }
        })
    }
}

fn bc_rt<F, FT>(f: F) -> Result<()>
where
    F: FnOnce() -> FT + Send + 'static,
    FT: Future<Output = Result<()>> + Send,
{
    tracing_subscriber::fmt::init();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async move {
            let run_fut = JoinTask::new(async move { f().await });
            let stop_fut = signal::any_signal(signal::TERMINATION_SIGNALS);
            tokio::select! {
                res = run_fut => res,
                signal = stop_fut => match signal {
                    Ok(signal) => {
                        tracing::info!(?signal, "received termination signal");
                        Ok(())
                    }
                    Err(e) => Err(e.into()),
                }
            }
        })
}

const DEFAULT_TTL: u32 = 40; // seconds
