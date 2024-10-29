use std::future::Future;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use everscale_crypto::ed25519;
use everscale_types::abi::{
    extend_signature_with_id, AbiType, AbiValue, AbiVersion, FromAbi, IntoAbi, WithAbiType,
};
use everscale_types::models::{
    Account, AccountState, BlockchainConfig, ElectionTimings, ExtInMsgInfo, GlobalCapability,
    MsgInfo, OwnedMessage, StdAddr, ValidatorStakeParams,
};
use everscale_types::prelude::*;
use reqwest::Url;
use serde::Serialize;
use tycho_block_util::config::build_elections_data_to_sign;
use tycho_control::ControlClient;
use tycho_network::PeerId;
use tycho_util::cli::signal;
use tycho_util::futures::JoinTask;
use tycho_util::serde_helpers;
use tycho_util::time::now_millis;

use crate::node::NodeKeys;
use crate::util::elector::data::Ref;
use crate::util::elector::methods::ParticiateInElectionsInput;
use crate::util::jrpc_client::{self, JrpcClient};
use crate::util::{elector, print_json, wallet};
use crate::BaseArgs;

/// Participate in validator elections.
#[derive(Parser)]
pub struct Cmd {
    #[clap(subcommand)]
    cmd: SubCmd,
}

impl Cmd {
    pub fn run(self, args: BaseArgs) -> Result<()> {
        match self.cmd {
            SubCmd::Once(cmd) => cmd.run(args),
            SubCmd::Recover(cmd) => cmd.run(args),
            SubCmd::Withdraw(cmd) => cmd.run(args),
            SubCmd::GetState(cmd) => cmd.run(args),
        }
    }
}

#[derive(Subcommand)]
enum SubCmd {
    Once(CmdOnce),
    Recover(CmdRecover),
    Withdraw(CmdWithdraw),
    GetState(CmdGetState),
}

/// Participate in validator elections (once).
#[derive(Parser)]
struct CmdOnce {
    #[clap(flatten)]
    control: ControlArgs,

    /// Path to validator keys. Default: `$TYCHO_HOME/validator_keys.json`
    #[clap(long)]
    sign: Option<PathBuf>,

    /// Path to node keys. Default: `$TYCHO_HOME/node_keys.json`
    node_keys: Option<PathBuf>,

    /// Stake size in nano tokens.
    #[clap(long)]
    stake: u128,

    /// Max stake factor. Uses config by default.
    #[clap(long)]
    max_factor: Option<u32>,
}

impl CmdOnce {
    fn run(self, args: BaseArgs) -> Result<()> {
        // Prepare keys
        let wallet_keys = NodeKeys::from_file(args.validator_keys_path(self.sign.as_ref()))?;
        let node_keys = {
            let path = args.node_keys_path(self.node_keys.as_ref());
            let secret = NodeKeys::from_file(path)?.as_secret();
            Arc::new(ed25519::KeyPair::from(&secret))
        };
        let adnl_addr = PeerId::from(node_keys.public_key);

        // Use client
        self.control.rt(args, move |client| async move {
            let wallet = Wallet::new(client.clone(), &wallet_keys.as_secret());

            let config = client.get_blockchain_config().await?;
            config.check_stake(self.stake)?;
            let max_factor = match self.max_factor {
                None => config.stake_params.max_stake_factor,
                Some(factor) => {
                    config.check_max_factor(factor)?;
                    factor
                }
            };

            // Get current elections
            let elector_data = client.get_elector_data().await?;
            let Some(Ref(elections)) = elector_data.current_election else {
                return print_json(ParticipateStatus::NoElections);
            };

            // Build payload signature
            let adnl_addr = HashBytes(adnl_addr.to_bytes());
            let validator_key = adnl_addr;
            let signature = {
                let data_to_sign = build_elections_data_to_sign(
                    elections.elect_at,
                    max_factor,
                    &wallet.address.address,
                    &adnl_addr,
                );
                let data_to_sign = extend_signature_with_id(&data_to_sign, config.signature_id);
                node_keys.sign_raw(&data_to_sign).to_vec()
            };

            // Build elections payload
            let payload = elector::methods::participate_in_elections()
                .encode_internal_input(&[ParticiateInElectionsInput {
                    query_id: now_millis(),
                    validator_key,
                    stake_at: elections.elect_at,
                    max_factor,
                    adnl_addr,
                    signature,
                }
                .into_abi()
                .named("input")])?
                .build()?;

            // Send stake
            let sent = wallet
                .transfer(
                    &ELECTOR_ADDR,
                    Amount::Exact(self.stake + ONE_CC),
                    true,
                    payload,
                    config.signature_id,
                )
                .await?;

            // Done
            print_json(ParticipateStatus::Participating {
                message_hash: sent.hash,
                message_expire_at: sent.expire_at,
                elections_id: elections.elect_at,
                elections_end: elections.elect_close,
                stake: self.stake,
            })
        })
    }
}

#[derive(Serialize)]
#[serde(tag = "status")]
enum ParticipateStatus {
    NoElections,
    Participating {
        message_hash: HashBytes,
        message_expire_at: u32,
        elections_id: u32,
        elections_end: u32,
        #[serde(with = "serde_helpers::string")]
        stake: u128,
    },
}

/// Recover stake.
#[derive(Parser)]
struct CmdRecover {
    #[clap(flatten)]
    control: ControlArgs,

    /// Path to validator keys. Default: `$TYCHO_HOME/validator_keys.json`
    #[clap(long)]
    sign: Option<PathBuf>,
}

impl CmdRecover {
    fn run(self, args: BaseArgs) -> Result<()> {
        // Prepare keys
        let wallet_keys = NodeKeys::from_file(&args.validator_keys_path(self.sign.as_ref()))?;

        // Use client
        self.control.rt(args, move |client| async move {
            let wallet = Wallet::new(client.clone(), &wallet_keys.as_secret());

            let config = client.get_blockchain_config().await?;

            // Find reward
            let elector_data = client.get_elector_data().await?;
            let Some(to_recover) = elector_data.credits.get(&wallet.address.address) else {
                return print_json(RecoverStatus::NoReward);
            };

            // Build payload
            let payload = elector::methods::recover_stake()
                .encode_internal_input(&[now_millis().into_abi().named("query_id")])?
                .build()?;

            // Send recover message
            let sent = wallet
                .transfer(
                    &ELECTOR_ADDR,
                    Amount::Exact(ONE_CC),
                    true,
                    payload,
                    config.signature_id,
                )
                .await?;

            // Done
            print_json(RecoverStatus::Recovered {
                message_hash: sent.hash,
                message_expire_at: sent.expire_at,
                amount: to_recover.into_inner(),
            })
        })
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "status")]
enum RecoverStatus {
    NoReward,
    Recovered {
        message_hash: HashBytes,
        message_expire_at: u32,
        #[serde(with = "serde_helpers::string")]
        amount: u128,
    },
}

/// Withdraw funds from the validator wallet.
#[derive(Parser)]
struct CmdWithdraw {
    #[clap(flatten)]
    control: ControlArgs,

    /// Path to validator keys. Default: `$TYCHO_HOME/validator_keys.json`
    #[clap(long)]
    sign: Option<PathBuf>,

    /// Destination address.
    #[clap(short, long, allow_hyphen_values(true))]
    dest: StdAddr,

    /// Amount in nano tokens.
    #[clap(short, long, required_unless_present = "all")]
    amount: Option<u128>,

    /// Withdraw everything from the wallet.
    #[clap(long)]
    all: bool,

    /// Sets `bounce` message flag.
    #[clap(short, long)]
    bounce: bool,

    /// Withdrawal message payload as a base64-encoded BOC.
    #[clap(short, long)]
    payload: Option<String>,
}

impl CmdWithdraw {
    fn run(self, args: BaseArgs) -> Result<()> {
        // Prepare keys
        let wallet_keys = NodeKeys::from_file(&args.validator_keys_path(self.sign.as_ref()))?;

        // Parse paylout
        let payload = match self.payload {
            None => Cell::default(),
            Some(payload) => Boc::decode_base64(payload).context("invalid payload")?,
        };

        // Use client
        self.control.rt(args, move |client| async move {
            let wallet = Wallet::new(client.clone(), &wallet_keys.as_secret());

            let config = client.get_blockchain_config().await?;

            let amount = match self.amount {
                None => Amount::All,
                Some(amount) => Amount::Exact(amount),
            };
            let sent = wallet
                .transfer(
                    &self.dest,
                    amount,
                    self.bounce,
                    payload,
                    config.signature_id,
                )
                .await?;

            print_json(serde_json::json!({
                "message_hash": sent.hash,
                "message_expire_at": sent.expire_at,
            }))
        })
    }
}

/// Get elector contract state.
#[derive(Parser)]
struct CmdGetState {
    #[clap(flatten)]
    control: ControlArgs,
}

impl CmdGetState {
    fn run(self, args: BaseArgs) -> Result<()> {
        self.control.rt(args, move |client| async move {
            let elector_data = client.get_elector_data().await?;
            print_json(elector_data)
        })
    }
}

const ELECTOR_ADDR: StdAddr = StdAddr::new(-1, HashBytes([0x33; 32]));
const ONE_CC: u128 = 1_000_000_000;

// === Wallet Stuff ===

struct Wallet {
    client: Client,
    address: StdAddr,
    secret: ed25519_dalek::SigningKey,
    public: ed25519_dalek::VerifyingKey,
}

impl Wallet {
    fn new(client: Client, secret: &ed25519::SecretKey) -> Self {
        let address = wallet::compute_address(-1, &ed25519::PublicKey::from(secret));

        let secret = ed25519_dalek::SigningKey::from_bytes(secret.as_bytes());
        let public = ed25519_dalek::VerifyingKey::from(&secret);

        Self {
            client,
            address,
            secret,
            public,
        }
    }

    async fn transfer(
        &self,
        to: &StdAddr,
        amount: Amount,
        bounce: bool,
        payload: Cell,
        signature_id: Option<i32>,
    ) -> Result<SentMessage> {
        let account_state = self
            .client
            .get_account_state(&self.address)
            .await?
            .context("wallet account does not exist")?;

        let init = match account_state.state {
            AccountState::Active(_) => None,
            AccountState::Uninit => Some(wallet::make_state_init(
                &ed25519::PublicKey::from_bytes(self.public.to_bytes()).unwrap(),
            )),
            AccountState::Frozen(_) => anyhow::bail!("wallet account is frozen"),
        };

        let (value, flags) = match amount {
            Amount::Exact(value) => (value, wallet::MSG_FLAGS_SIMPLE_SEND),
            Amount::All => (0, wallet::MSG_FLAGS_SEND_ALL),
        };
        anyhow::ensure!(
            account_state.balance.tokens.into_inner() >= value,
            "insufficient balance"
        );

        let AbiValue::Tuple(inputs) = wallet::methods::SendTransactionInputs {
            dest: to.clone(),
            value,
            bounce,
            flags,
            payload,
        }
        .into_abi() else {
            unreachable!();
        };

        let now_ms = now_millis();
        let expire_at = (now_ms / 1000) as u32 + 40;
        let body = wallet::methods::send_transaction()
            .encode_external(&inputs)
            .with_address(&self.address)
            .with_time(now_ms)
            .with_expire_at(expire_at)
            .with_pubkey(&self.public)
            .build_input()?
            .sign(&self.secret, signature_id)?;
        let body_range = CellSliceRange::full(body.as_ref());

        let message = OwnedMessage {
            info: MsgInfo::ExtIn(ExtInMsgInfo {
                src: None,
                dst: self.address.clone().into(),
                ..Default::default()
            }),
            init,
            body: (body, body_range),
            layout: None,
        };
        let message_cell = CellBuilder::build_from(message)?;

        self.client.broadcast_message(message_cell.as_ref()).await?;

        Ok(SentMessage {
            hash: *message_cell.repr_hash(),
            expire_at,
        })
    }
}

#[derive(Debug, Clone, Copy)]
enum Amount {
    Exact(u128),
    All,
}

#[derive(Debug, Clone, Copy, Serialize)]
struct SentMessage {
    hash: HashBytes,
    expire_at: u32,
}

// === Control RT ===

#[derive(Clone, Args)]
struct ControlArgs {
    /// Path to the control socket. Default: `$TYCHO_HOME/control.sock`
    #[clap(long)]
    control_socket: Option<PathBuf>,

    /// RPC url
    #[clap(long)]
    rpc: Option<Url>,

    #[clap(long, requires = "rpc")]
    force_rpc: bool,
}

impl ControlArgs {
    fn rt<F, FT>(&self, args: BaseArgs, f: F) -> Result<()>
    where
        F: FnOnce(Client) -> FT + Send + 'static,
        FT: Future<Output = Result<()>> + Send,
    {
        self.rt_ext(args, move |factory| async move {
            let client = factory.create().await?;
            f(client).await
        })
    }

    fn rt_ext<F, FT>(&self, args: BaseArgs, f: F) -> Result<()>
    where
        F: FnOnce(ClientFactory) -> FT + Send + 'static,
        FT: Future<Output = Result<()>> + Send + 'static,
    {
        tracing_subscriber::fmt::init();

        let factory = ClientFactory {
            sock: args.control_socket_path(self.control_socket.as_ref()),
            rpc: self.rpc.clone(),
            force_rpc: self.force_rpc,
        };

        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(async move {
                let run_fut = JoinTask::new(f(factory));
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
}

// === Control/RPC client ===

struct ClientFactory {
    sock: PathBuf,
    rpc: Option<Url>,
    force_rpc: bool,
}

impl ClientFactory {
    async fn create(&self) -> Result<Client> {
        let inner = 'client: {
            if let Some(rpc) = &self.rpc {
                if self.force_rpc || !self.sock.exists() {
                    let rpc =
                        JrpcClient::new(rpc.clone()).context("failed to create JRPC client")?;
                    break 'client ClientImpl::Jrpc(rpc);
                }
            }

            let control = ControlClient::connect(&self.sock)
                .await
                .context("failed to connect to control server")?;
            ClientImpl::Control(control)
        };

        Ok(Client {
            inner: Arc::new(inner),
        })
    }
}

#[derive(Clone)]
struct Client {
    inner: Arc<ClientImpl>,
}

impl Client {
    async fn get_elector_data(&self) -> Result<elector::data::PartialElectorData> {
        static ELECTOR_ABI: OnceLock<AbiType> = OnceLock::new();

        let account_state = self
            .get_account_state(&ELECTOR_ADDR)
            .await?
            .context("elector contract not found")?;

        let elector_data = match account_state.state {
            AccountState::Active(state) => state.data.context("elector data is empty")?,
            _ => anyhow::bail!("invalid elector state"),
        };

        let abi_type = ELECTOR_ABI.get_or_init(elector::data::PartialElectorData::abi_type);
        AbiValue::load_partial(&abi_type, AbiVersion::V2_1, &mut elector_data.as_slice()?)
            .and_then(elector::data::PartialElectorData::from_abi)
    }

    async fn get_blockchain_config(&self) -> Result<ParsedBlockchainConfig> {
        match self.inner.as_ref() {
            ClientImpl::Jrpc(rpc) => {
                let res = rpc.get_config().await?;
                ParsedBlockchainConfig::new(res.global_id, res.seqno, res.config)
            }
            ClientImpl::Control(control) => {
                let res = control.get_blockchain_config().await?.parse()?;
                ParsedBlockchainConfig::new(res.global_id, res.mc_seqno, res.config)
            }
        }
    }

    async fn get_account_state(&self, addr: &StdAddr) -> Result<Option<Account>> {
        match self.inner.as_ref() {
            ClientImpl::Jrpc(rpc) => match rpc.get_account(addr).await? {
                jrpc_client::AccountStateResponse::Exists { account, .. } => Ok(Some(*account)),
                jrpc_client::AccountStateResponse::NotExists { .. } => Ok(None),
                jrpc_client::AccountStateResponse::Unchanged { .. } => {
                    anyhow::bail!("unexpected response")
                }
            },
            ClientImpl::Control(control) => {
                let res = control.get_account_state(addr).await?.parse()?;
                res.state.load_account().map_err(Into::into)
            }
        }
    }

    async fn broadcast_message(&self, message: &DynCell) -> Result<()> {
        match self.inner.as_ref() {
            ClientImpl::Jrpc(rpc) => rpc.send_message(message).await,
            ClientImpl::Control(control) => control
                .broadcast_external_message_raw(message)
                .await
                .map_err(Into::into),
        }
    }
}

enum ClientImpl {
    Control(ControlClient),
    Jrpc(JrpcClient),
}

struct ParsedBlockchainConfig {
    signature_id: Option<i32>,
    #[allow(unused)]
    election_timings: ElectionTimings,
    stake_params: ValidatorStakeParams,
}

impl ParsedBlockchainConfig {
    fn new(global_id: i32, _mc_seqno: u32, config: BlockchainConfig) -> Result<Self> {
        let version = config.get_global_version()?;
        let signature_id = version
            .capabilities
            .contains(GlobalCapability::CapSignatureWithId)
            .then_some(global_id);

        let election_timings = config.get_election_timings()?;
        let stake_params = config.get_validator_stake_params()?;

        Ok(Self {
            signature_id,
            election_timings,
            stake_params,
        })
    }

    fn check_stake(&self, stake: u128) -> Result<()> {
        anyhow::ensure!(
            stake >= self.stake_params.min_stake.into_inner(),
            "stake is too small"
        );
        anyhow::ensure!(
            stake <= self.stake_params.max_stake.into_inner(),
            "stake is too big"
        );
        Ok(())
    }

    fn check_max_factor(&self, max_factor: u32) -> Result<()> {
        anyhow::ensure!(max_factor <= (1 << 16), "max factor is too small");
        anyhow::ensure!(
            max_factor >= self.stake_params.max_stake_factor,
            "max factor is too big"
        );
        Ok(())
    }
}
