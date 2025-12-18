use std::collections::{BTreeMap, BTreeSet, hash_map};
use std::io::{IsTerminal, Read};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Url;
use serde::Serialize;
use tycho_block_util::config::{apply_price_factor, compute_gas_price_factor};
use tycho_crypto::ed25519;
use tycho_types::abi::extend_signature_with_id;
use tycho_types::boc::Boc;
use tycho_types::models::{
    Account, AccountState, BlockchainConfigParams, ConfigParam11, ExtInMsgInfo, GlobalCapability,
    MsgInfo, OwnedMessage, StdAddr, ValidatorSet,
};
use tycho_types::num::Tokens;
use tycho_types::prelude::*;
use tycho_util::cli::signal;
use tycho_util::time::now_sec;
use tycho_util::{FastHashMap, serde_helpers};

use crate::util::config::{ConfigContract, ConfigProposal};
use crate::util::jrpc_client::{AccountStateResponse, JrpcClient, LatestBlockchainConfig};
use crate::util::{FpTokens, parse_public_key, parse_secret_key, print_json};

/// Blockchain stuff
#[derive(clap::Parser)]
pub struct Cmd {
    #[clap(subcommand)]
    cmd: SubCmd,
}

impl Cmd {
    pub fn run(self) -> Result<()> {
        tracing_subscriber::fmt::init();

        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(signal::run_or_terminate(async move {
                match self.cmd {
                    SubCmd::GetParam(cmd) => cmd.run().await,
                    SubCmd::SetParam(cmd) => cmd.run().await,
                    SubCmd::SetMasterKey(cmd) => cmd.run().await,
                    SubCmd::SetElectorCode(cmd) => cmd.run().await,
                    SubCmd::SetConfigCode(cmd) => cmd.run().await,
                    SubCmd::ListProposals(cmd) => cmd.run().await,
                    SubCmd::GenProposal(cmd) => cmd.run().await,
                    SubCmd::GenProposalVote(cmd) => cmd.run().await,
                }
            }))
    }
}

#[derive(clap::Parser)]
enum SubCmd {
    GetParam(GetParamCmd),
    SetParam(SetParamCmd),
    SetMasterKey(SetMasterKeyCmd),
    SetElectorCode(SetElectorCode),
    SetConfigCode(SetConfigCode),
    ListProposals(CmdListProposals),
    GenProposal(CmdGenProposal),
    GenProposalVote(CmdGenProposalVote),
}

/// Get blockchain config parameter
#[derive(clap::Parser)]
pub struct GetParamCmd {
    /// parameter index
    param: i32,

    /// show value as a raw base64-encoded BOC
    #[clap(long)]
    raw_value: bool,

    /// RPC url
    #[clap(long)]
    rpc: Url,
}

impl GetParamCmd {
    async fn run(self) -> Result<()> {
        let client = JrpcClient::new(self.rpc)?;
        let res = client.get_config().await?;

        let item = if self.raw_value {
            res.config
                .params
                .get_raw_cell(self.param as u32)
                .context("invalid config")?
                .map(Boc::encode_base64)
                .map(serde_json::Value::String)
        } else if let Some(value) = res.config.get_raw_cell(self.param as u32)? {
            // Parse and serialize only the requested param to allow using this
            // tool for some common params while others have been modified.
            param_value_to_json(self.param as u32, &value).map(Some)?
        } else {
            None
        };

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

    /// treat value as a raw base64-encoded BOC
    #[clap(long)]
    raw_value: bool,

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

        let value = if self.raw_value {
            Boc::decode_base64(self.value).context("invalid param")?
        } else {
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
    #[clap(long)]
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
    #[clap(long)]
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
    #[clap(long)]
    rpc: Url,

    #[clap(flatten)]
    sign: KeyArgs,
}

impl SetConfigCode {
    async fn run(self) -> Result<()> {
        let client = JrpcClient::new(self.rpc)?;

        let code = std::fs::read(&self.code_path).context("failed to read config code file")?;
        let code = Boc::decode(code).context("invalid config code")?;

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

/// List active config proposals.
#[derive(clap::Parser)]
struct CmdListProposals {
    /// RPC url
    #[clap(long)]
    rpc: Url,

    /// do not parse proposals.
    #[clap(long)]
    raw: bool,
}

impl CmdListProposals {
    async fn run(self) -> Result<()> {
        let client = JrpcClient::new(self.rpc)?;
        let config_account = get_config_account(&client).await?;
        let blockchain_config = ConfigContract(&config_account).make_blockchain_config()?;
        let proposals = ConfigContract(&config_account).get_proposals()?;

        let mut validator_sets = FastHashMap::default();
        for idx in 32..38 {
            if let Some(vset) = blockchain_config.as_dict().get(idx)?
                && let hash_map::Entry::Vacant(entry) = validator_sets.entry(*vset.repr_hash())
            {
                entry.insert(vset.parse::<ValidatorSet>()?);
            }
        }

        let proposals = proposals
            .into_iter()
            .map(|(hash, proposal)| {
                let proposal = if self.raw {
                    serde_json::to_value(proposal)
                } else {
                    let validator_set = validator_sets.get(&proposal.vset_id);
                    serde_json::to_value(ParsedConfigProposal::from_raw(proposal, validator_set))
                }?;
                Ok::<_, anyhow::Error>((hash, proposal))
            })
            .collect::<Result<BTreeMap<HashBytes, serde_json::Value>, _>>()?;
        print_json(proposals)
    }
}

#[derive(Debug, Clone, Serialize)]
struct ParsedConfigProposal {
    is_critical: bool,
    action: ParsedConfigProposalValue,
    expire_at: u32,
    #[serde(with = "serde_helpers::string")]
    until_expire_at: humantime::FormattedDuration,
    prev_param_value_hash: Option<HashBytes>,
    vset_id: HashBytes,
    voters: BTreeSet<HashBytes>,
    weight_remaining: i64,
    rounds_remaining: u8,
    wins: u8,
    losses: u8,
}

impl ParsedConfigProposal {
    fn from_raw(proposal: ConfigProposal, validator_set: Option<&ValidatorSet>) -> Self {
        let action = ParsedConfigProposalValue::from_raw(proposal.param_idx, proposal.param_value);
        let voters = match validator_set {
            Some(vset) => {
                let mut voters = BTreeSet::new();
                for idx in proposal.voters {
                    if let Some(voter) = vset.list.get(idx as usize) {
                        voters.insert(voter.public_key);
                    }
                }
                voters
            }
            None => Default::default(),
        };

        let now = tycho_util::time::now_sec();
        let until_expire_at = Duration::from_secs(proposal.expire_at.saturating_sub(now) as _);

        Self {
            is_critical: proposal.is_critical,
            action,
            expire_at: proposal.expire_at,
            until_expire_at: humantime::format_duration(until_expire_at),
            prev_param_value_hash: proposal.prev_param_value_hash,
            vset_id: proposal.vset_id,
            voters,
            weight_remaining: proposal.weight_remaining,
            rounds_remaining: proposal.rounds_remaining,
            wins: proposal.wins,
            losses: proposal.losses,
        }
    }
}

fn param_value_to_json(idx: u32, value: &Cell) -> Result<serde_json::Value> {
    let mut dict = Dict::<u32, Cell>::new();
    dict.set(idx, value)?;
    let params = BlockchainConfigParams::from_raw(dict.into_root().unwrap());
    let params = serde_json::to_value(params)?;
    params
        .get(idx.to_string())
        .cloned()
        .context("param not found")
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
enum ParsedConfigProposalValue {
    RemoveParam {
        param: i32,
    },
    SetParam {
        param: i32,
        #[serde(with = "Boc")]
        value: Cell,
        #[serde(skip_serializing_if = "Option::is_none")]
        parsed: Option<serde_json::Value>,
    },
    SetPubkey {
        pubkey: HashBytes,
    },
    SetConfigCode {
        #[serde(with = "Boc")]
        args: Cell,
        #[serde(with = "Boc")]
        code: Cell,
    },
    SetElectorCode {
        #[serde(with = "Boc")]
        args: Option<Cell>,
        #[serde(with = "Boc")]
        code: Cell,
    },
    Invalid {
        param: i32,
        #[serde(with = "Boc")]
        value: Option<Cell>,
    },
}

impl ParsedConfigProposalValue {
    fn from_raw(idx: i32, value: Option<Cell>) -> Self {
        match idx {
            ConfigContract::PARAM_IDX_OWN_PUBKEY => {
                if let Some(value) = &value
                    && let Ok(pubkey) = value.as_slice_allow_exotic().load_u256()
                {
                    ParsedConfigProposalValue::SetPubkey { pubkey }
                } else {
                    ParsedConfigProposalValue::Invalid { param: idx, value }
                }
            }
            ConfigContract::PARAM_IDX_CONFIG_CODE => {
                if let Some(value) = &value
                    && let Ok(mut cs) = value.as_slice()
                    && let Ok(code) = cs.load_reference_cloned()
                {
                    ParsedConfigProposalValue::SetConfigCode {
                        args: CellBuilder::build_from(cs).unwrap(),
                        code,
                    }
                } else {
                    ParsedConfigProposalValue::Invalid { param: idx, value }
                }
            }
            ConfigContract::PARAM_IDX_ELECTOR_CODE => {
                if let Some(value) = &value
                    && let Ok(mut cs) = value.as_slice()
                    && let Ok(code) = cs.load_reference_cloned()
                {
                    ParsedConfigProposalValue::SetElectorCode {
                        args: if cs.is_empty() {
                            None
                        } else {
                            Some(CellBuilder::build_from(cs).unwrap())
                        },
                        code,
                    }
                } else {
                    ParsedConfigProposalValue::Invalid { param: idx, value }
                }
            }
            _ => match value {
                None => ParsedConfigProposalValue::RemoveParam { param: idx },
                Some(param_value) => {
                    let mut parsed = param_value_to_json(idx as u32, &param_value).ok();

                    if let Some(serde_json::Value::String(parsed_str)) = &parsed
                        && let Ok(parsed_cell) = Boc::decode_base64(parsed_str)
                        && parsed_cell == param_value
                    {
                        // Value was not actually parsed.
                        parsed = None;
                    }

                    ParsedConfigProposalValue::SetParam {
                        param: idx,
                        parsed,
                        value: param_value,
                    }
                }
            },
        }
    }
}

/// Create proposal payload and compute the required amount.
#[derive(clap::Parser)]
struct CmdGenProposal {
    /// proposal action
    #[clap(subcommand)]
    action: CmdGenProposalAction,
}

#[derive(clap::Args)]
struct CmdGenProposalArgs {
    /// RPC url
    #[clap(long)]
    rpc: Url,

    /// skip existing proposal check
    #[clap(long)]
    allow_duplicate: bool,

    /// compute price without additional send gas
    #[clap(long)]
    no_send_gas: bool,

    /// proposal TTL. Default: lower bound from the blockchain config
    #[clap(long, value_parser = humantime::parse_duration)]
    ttl: Option<Duration>,

    /// query ID. Default: current timestamp in milliseconds
    #[clap(long)]
    query_id: Option<u64>,
}

#[allow(clippy::enum_variant_names)]
#[derive(clap::Parser)]
enum CmdGenProposalAction {
    /// Set blockchain config parameter
    SetParam {
        /// parameter index
        param: i32,

        /// parameter value
        value: String,

        /// treat value as a raw base64-encoded BOC
        #[clap(long)]
        raw_value: bool,

        /// do not require the exact value to be changed
        #[clap(long)]
        ignore_prev_value: bool,

        #[clap(flatten)]
        args: CmdGenProposalArgs,
    },
    /// Set blockchain config key
    SetMasterKey {
        /// new public key
        pubkey: String,

        #[clap(flatten)]
        args: CmdGenProposalArgs,
    },
    /// Set elector contract code
    SetElectorCode {
        /// path to the elector code BOC
        code_path: PathBuf,

        /// optional parameters for `after_code_upgrade`
        #[clap(long)]
        upgrade_args: Option<String>,

        #[clap(flatten)]
        args: CmdGenProposalArgs,
    },
    /// Set config contract code
    SetConfigCode {
        /// path to the config code BOC
        code_path: PathBuf,

        /// optional parameters for `after_code_upgrade`
        #[clap(long)]
        upgrade_args: Option<String>,

        #[clap(flatten)]
        args: CmdGenProposalArgs,
    },
}

impl CmdGenProposal {
    async fn run(self) -> Result<()> {
        fn build_update_code_value(
            name: &str,
            code_path: PathBuf,
            upgrade_args: Option<String>,
        ) -> Result<Cell> {
            let code = std::fs::read(code_path)
                .with_context(|| format!("failed to read {name} code file"))?;
            let code = Boc::decode(code).with_context(|| format!("invalid {name} code"))?;

            let mut b = CellBuilder::new();
            b.store_reference(code)?;
            if let Some(args) = upgrade_args {
                let args = Boc::decode_base64(args).context("invalid upgrade params")?;
                b.store_slice(args.as_slice()?)?;
            }
            b.build().context("failed to build param value")
        }

        let mut use_prev_value_hash = false;
        let (param, value, args) = match self.action {
            CmdGenProposalAction::SetParam {
                param,
                value,
                raw_value,
                ignore_prev_value,
                args,
            } => {
                let index = param as u32;
                let value = if raw_value {
                    Some(Boc::decode_base64(value).context("invalid param")?)
                } else {
                    let value = serde_json::from_str::<serde_json::Value>(&value)
                        .context("invalid value")?;

                    let mut params = serde_json::Map::new();
                    params.insert(index.to_string(), value);
                    let params = serde_json::from_value::<BlockchainConfigParams>(params.into())?;
                    Some(params.as_dict().get(index)?.context("invalid param")?)
                };
                use_prev_value_hash = !ignore_prev_value;
                (param, value, args)
            }
            CmdGenProposalAction::SetMasterKey { pubkey, args } => {
                let pubkey = parse_public_key(pubkey.as_bytes(), false)?;
                let value = CellBuilder::build_from(HashBytes::wrap(pubkey.as_bytes()))?;
                (ConfigContract::PARAM_IDX_OWN_PUBKEY, Some(value), args)
            }
            CmdGenProposalAction::SetElectorCode {
                code_path,
                upgrade_args,
                args,
            } => {
                let value = build_update_code_value("elector", code_path, upgrade_args)?;
                (ConfigContract::PARAM_IDX_ELECTOR_CODE, Some(value), args)
            }
            CmdGenProposalAction::SetConfigCode {
                code_path,
                upgrade_args,
                args,
            } => {
                let value = build_update_code_value("config", code_path, upgrade_args)?;
                (ConfigContract::PARAM_IDX_CONFIG_CODE, Some(value), args)
            }
        };
        let index = param as u32;

        let client = JrpcClient::new(args.rpc)?;

        let config_account = get_config_account(&client).await?;
        let blockchain_config = ConfigContract(&config_account).make_blockchain_config()?;

        {
            let mandatory_params = blockchain_config.get_mandatory_params()?;
            anyhow::ensure!(
                value.is_some() || mandatory_params.get(index)?.is_none(),
                "cannot remove mandatory params"
            );
        }

        let is_critical = {
            let critical_params = blockchain_config.get_critical_params()?;
            critical_params.get(index)?.is_some()
        };

        let proposal_config = {
            let Some(config) = blockchain_config.get::<ConfigParam11>()? else {
                anyhow::bail!("blockchain config does not contain voting setup config (param 11)");
            };
            if is_critical {
                config.critical_params.load()
            } else {
                config.normal_params.load()
            }?
        };

        let ttl = if let Some(ttl) = args.ttl {
            match u32::try_from(ttl.as_secs()) {
                Ok(ttl) if ttl < proposal_config.min_store_sec => {
                    let min = Duration::from_secs(proposal_config.min_store_sec as _);
                    anyhow::bail!(
                        "TTL is too small, min is {} ({} seconds)",
                        humantime::format_duration(min),
                        proposal_config.min_store_sec
                    )
                }
                Ok(ttl) if ttl <= proposal_config.max_store_sec => ttl,
                _ => {
                    let max = Duration::from_secs(proposal_config.max_store_sec as _);
                    anyhow::bail!(
                        "TTL is too big, max is {} ({} seconds)",
                        humantime::format_duration(max),
                        proposal_config.max_store_sec
                    )
                }
            }
        } else {
            proposal_config.min_store_sec
        };

        let prev_param_value_hash = if use_prev_value_hash {
            let prev_value = blockchain_config.get_raw_cell_ref(index)?;
            prev_value.map(DynCell::repr_hash)
        } else {
            None
        };

        let send_price = if args.no_send_gas {
            Tokens::ZERO
        } else {
            let gas_price = blockchain_config.get_gas_prices(true)?.gas_price;
            let price_factor = compute_gas_price_factor(true, gas_price)?;
            Tokens::new(apply_price_factor(1_000_000_000, price_factor))
        };
        let raw_price =
            ConfigContract::compute_proposal_price(&proposal_config, ttl, value.as_deref())?;

        let raw_content =
            ConfigContract::create_proposal_raw_content(param, value, prev_param_value_hash);
        let proposal_hash = *raw_content.repr_hash();

        anyhow::ensure!(
            args.allow_duplicate
                || ConfigContract(&config_account)
                    .get_proposal(&proposal_hash)?
                    .is_none(),
            "proposal with the same content already exists"
        );

        let query_id = args.query_id.unwrap_or_else(tycho_util::time::now_millis);
        let payload =
            ConfigContract::create_proposal_payload(query_id, ttl, raw_content, is_critical);

        print_json(serde_json::json!({
            "hash": proposal_hash,
            "query_id": query_id.to_string(),
            "is_critical": is_critical,
            "payload": Boc::encode_base64(payload),
            "price": FpTokens((send_price + raw_price).into_inner()),
        }))
    }
}

/// Create proposal voting payload.
#[derive(clap::Parser)]
pub struct CmdGenProposalVote {
    /// RPC url
    #[clap(long)]
    rpc: Url,

    #[clap(flatten)]
    sign: KeyArgs,

    /// query ID. Default: current timestamp in milliseconds
    #[clap(long)]
    query_id: Option<u64>,

    /// Proposal hash.
    hash: HashBytes,
}

impl CmdGenProposalVote {
    async fn run(self) -> Result<()> {
        let keypair = self.sign.get_keypair()?;
        let client = JrpcClient::new(self.rpc)?;

        let LatestBlockchainConfig {
            global_id,
            config: blockchain_config,
            ..
        } = client.get_config().await?;

        let global_version = blockchain_config.get_global_version()?;
        let signature_id = global_version
            .capabilities
            .contains(GlobalCapability::CapSignatureWithId)
            .then_some(global_id);

        let (config_account, _) = client
            .get_account(&StdAddr::new(-1, blockchain_config.address))
            .await
            .context("failed to get config account state")?
            .expect_existing()?;

        let Some(proposal) = ConfigContract(&config_account).get_proposal(&self.hash)? else {
            anyhow::bail!("no active proposal found with hash {}", self.hash);
        };

        let validator_idx =
            proposal.resolve_validator_idx(&blockchain_config.params, &keypair.public_key)?;

        let query_id = self.query_id.unwrap_or_else(tycho_util::time::now_millis);
        let payload = ConfigContract::create_vote_payload(
            query_id,
            validator_idx,
            &self.hash,
            &keypair,
            signature_id,
        );

        print_json(serde_json::json!({
            "query_id": query_id.to_string(),
            "validator_idx": validator_idx,
            "payload": Boc::encode_base64(payload),
        }))
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
    send_config_action_ext(client, action, &keypair, sign.ttl).await
}

pub(crate) async fn send_config_action_ext(
    client: &JrpcClient,
    action: Action,
    keypair: &ed25519::KeyPair,
    ttl: u32,
) -> Result<()> {
    let res = client.get_config().await?;
    let config_addr = StdAddr::new(-1, res.config.address);

    let global_version = res.config.get_global_version()?;
    let signature_id = global_version
        .capabilities
        .contains(GlobalCapability::CapSignatureWithId)
        .then_some(res.global_id);

    let seqno = prepare_action(client, &config_addr, &keypair.public_key).await?;
    let (message, expire_at) =
        create_message(seqno, &config_addr, action, keypair, signature_id, ttl)?;

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

    let message = Box::new(OwnedMessage {
        info: MsgInfo::ExtIn(ExtInMsgInfo {
            dst: config_addr.clone().into(),
            ..Default::default()
        }),
        init: None,
        body: body.into(),
        layout: None,
    });

    Ok((message, expire_at))
}

async fn get_config_account(client: &JrpcClient) -> Result<Box<Account>> {
    let config = client.get_config().await?.config;

    let (account, _) = client
        .get_account(&StdAddr::new(-1, config.address))
        .await
        .context("failed to get config account state")?
        .expect_existing()?;

    Ok(account)
}

#[derive(Debug, Clone)]
pub(crate) enum Action {
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

const DEFAULT_TTL: u32 = 40; // seconds
