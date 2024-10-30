use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use everscale_crypto::ed25519;
use everscale_types::abi::{
    extend_signature_with_id, AbiType, AbiValue, AbiVersion, FromAbi, IntoAbi, WithAbiType,
};
use everscale_types::models::{
    Account, AccountState, BlockchainConfig, ExtInMsgInfo, GlobalCapability, MsgInfo, OwnedMessage,
    StateInit, StdAddr, ValidatorStakeParams,
};
use everscale_types::prelude::*;
use rand::Rng;
use reqwest::Url;
use serde::Serialize;
use tycho_block_util::config::build_elections_data_to_sign;
use tycho_control::ControlClient;
use tycho_network::PeerId;
use tycho_util::cli::logger::init_logger_simple;
use tycho_util::cli::signal;
use tycho_util::futures::JoinTask;
use tycho_util::serde_helpers;
use tycho_util::time::{now_millis, now_sec};

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
            SubCmd::Run(cmd) => cmd.run(args),
            SubCmd::Once(cmd) => cmd.run(args),
            SubCmd::Recover(cmd) => cmd.run(args),
            SubCmd::Withdraw(cmd) => cmd.run(args),
            SubCmd::GetState(cmd) => cmd.run(args),
        }
    }
}

#[derive(Subcommand)]
enum SubCmd {
    Run(CmdRun),
    Once(CmdOnce),
    Recover(CmdRecover),
    Withdraw(CmdWithdraw),
    GetState(CmdGetState),
}

/// Participate in validator elections.
#[derive(Parser)]
struct CmdRun {
    #[clap(flatten)]
    control: ControlArgs,

    /// Path to validator keys. Default: `$TYCHO_HOME/validator_keys.json`
    #[clap(long)]
    sign: Option<PathBuf>,

    /// Path to node keys. Default: `$TYCHO_HOME/node_keys.json`
    #[clap(long)]
    node_keys: Option<PathBuf>,

    /// Stake size in nano tokens.
    #[clap(short, long)]
    stake: u128,

    /// Max stake factor. Uses config by default.
    #[clap(long)]
    stake_factor: Option<u32>,

    /// Offset after stake unfreeze time.
    #[clap(long, value_parser = humantime::parse_duration, default_value = "10m")]
    stake_unfreeze_offset: Duration,

    /// Time to do nothing after the elections start.
    #[clap(long, value_parser = humantime::parse_duration, default_value = "10m")]
    elections_start_offset: Duration,

    /// Time to stop doing anything before the elections end.
    #[clap(long, value_parser = humantime::parse_duration, default_value = "2m")]
    elections_end_offset: Duration,

    /// Min retry interval in case of error.
    #[clap(long, value_parser = humantime::parse_duration, default_value = "10s")]
    min_retry_interval: Duration,

    /// Max retry interval in case of error.
    #[clap(long, value_parser = humantime::parse_duration, default_value = "10m")]
    max_retry_interval: Duration,

    /// Interval increase factor.
    #[clap(long, value_parser, default_value_t = 2.0)]
    retry_interval_factor: f64,

    /// Force stakes to be sent right after the elections start.
    #[clap(long)]
    disable_random_shift: bool,
}

impl CmdRun {
    fn run(mut self, args: BaseArgs) -> Result<()> {
        // Compute paths
        let validator_keys_path = args.validator_keys_path(self.sign.as_ref());
        let node_keys_path = args.node_keys_path(self.node_keys.as_ref());

        // Ensure that all timings are reasonable
        self.min_retry_interval = self.min_retry_interval.max(Duration::from_secs(1));
        self.max_retry_interval = self.max_retry_interval.max(self.min_retry_interval);
        self.retry_interval_factor = self.retry_interval_factor.max(1.0);

        // Use just the runtime (without creating a client yet)
        self.control.clone().rt_ext(args, move |f| async move {
            let mut interval = self.min_retry_interval.as_secs();
            loop {
                // Run the main loop (and reset the interval
                // each after successful iteration).
                let before_repeat = || interval = self.min_retry_interval.as_secs();
                if let Err(e) = self
                    .try_run(&f, &validator_keys_path, &node_keys_path, before_repeat)
                    .await
                {
                    tracing::error!("error occured: {e:?}");
                }

                let retrying_in = Duration::from_secs(interval);
                tracing::info!(retrying_in = %humantime::format_duration(retrying_in));
                tokio::time::sleep(retrying_in).await;

                interval = std::cmp::min(
                    self.max_retry_interval.as_secs(),
                    (interval as f64 * self.retry_interval_factor) as u64,
                );
            }
        })
    }

    async fn try_run<F>(
        &self,
        f: &ClientFactory,
        validator_keys_path: &Path,
        node_keys_path: &Path,
        mut before_repeat: F,
    ) -> Result<()>
    where
        F: FnMut(),
    {
        tracing::info!("started validation loop");

        let to_sec_or_max = |d: Duration| d.as_secs().try_into().unwrap_or(u32::MAX);
        let elections_start_offset = to_sec_or_max(self.elections_start_offset);
        let elections_end_offset = to_sec_or_max(self.elections_end_offset);

        let mut first_iteration = false;
        let mut random_shift = None;
        let mut interval = 0u32;
        'outer: loop {
            if !std::mem::take(&mut first_iteration) {
                // Notify the caller on each successful repeat.
                before_repeat();
            }

            // Sleep with the requested interval
            if interval > 0 {
                interval = std::cmp::max(interval, 10);
                tokio::time::sleep(Duration::from_secs(interval as u64)).await;
            }

            // Create a client
            let client = f.create().await?;

            // Compute elections timeline
            let (Timings { gen_utime }, elector_data) = client
                .get_elector_data()
                .await
                .context("failed to get elector data")?;
            let config = client
                .get_blockchain_config()
                .await
                .context("failed to get blockchain config")?;
            let timeline = config
                .compute_elections_timeline(gen_utime)
                .context("failed to compute elections timeline")?;
            tracing::info!(gen_utime, ?timeline);

            // Handle timeline
            let elections_end = match timeline {
                // If elections were not started yet, wait for the start (with an additional offset)
                Timeline::BeforeElections {
                    until_elections_start,
                } => {
                    random_shift = None; // Reset random shift before each election
                    tracing::info!("waiting for the elections to start");
                    interval = until_elections_start + elections_start_offset;
                    continue;
                }
                // If elections were started
                Timeline::Elections {
                    since_elections_start,
                    until_elections_end,
                    elections_end,
                } => {
                    let random_shift = match random_shift {
                        Some(shift) => shift,
                        None if self.disable_random_shift => *random_shift.insert(0),
                        None => {
                            // Compute the random offset in the first 1/4 of elections range
                            let range = (since_elections_start + until_elections_end)
                                .saturating_sub(elections_end_offset)
                                .saturating_sub(elections_start_offset)
                                / 4;
                            *random_shift.insert(rand::thread_rng().gen_range(0..range))
                        }
                    };

                    let start_offset = elections_start_offset + random_shift;

                    if let Some(offset) = start_offset.checked_sub(since_elections_start) {
                        if offset > 0 {
                            // Wait a bit after elections start
                            interval = offset;
                            continue;
                        }
                    } else if let Some(offset) =
                        elections_end_offset.checked_sub(until_elections_end)
                    {
                        // Elections will end soon, attempts are doomed
                        interval = offset;
                        continue;
                    }

                    // We can participate, remember elections end timestamp
                    elections_end
                }
                // Elections were already finished, wait for the next round
                Timeline::AfterElections { until_round_end } => 'after_end: {
                    // Check if there are still some unsuccessful elections
                    if elector_data.current_election.is_some() && until_round_end == 0 {
                        // Extend their lifetime
                        break 'after_end gen_utime + 600;
                    }

                    tracing::info!("waiting for the new round to start");
                    interval = until_round_end;
                    continue 'outer;
                }
            };

            // Participate in elections
            let Some(Ref(current_elections)) = &elector_data.current_election else {
                tracing::info!("no current elections in the elector state");
                interval = 1; // retry nearly immediate
                continue;
            };

            // Wait until stakes are unfrozen
            if let Some(mut unfreeze_at) = elector_data.nearest_unfreeze_at(elections_end) {
                unfreeze_at += to_sec_or_max(self.stake_unfreeze_offset);
                if unfreeze_at > elections_end.saturating_sub(elections_end_offset) {
                    tracing::warn!(
                        unfreeze_at,
                        elections_end,
                        "stakes will unfreeze after the end of current elections"
                    );
                } else if let Some(until_unfreeze) = unfreeze_at.checked_sub(now_sec()) {
                    if until_unfreeze > 0 {
                        tracing::info!(until_unfreeze, "waiting for stakes to unfreeze");
                        interval = until_unfreeze;
                        continue;
                    }
                }
            }

            // Try elect
            let elect_fut = SimpleValidatorParams {
                wallet_secret: NodeKeys::from_file(validator_keys_path)?.as_secret(),
                node_secret: NodeKeys::from_file(node_keys_path)?.as_secret(),
                stake_per_round: self.stake,
                stake_factor: self.stake_factor,
            }
            .elect(ElectionsContext {
                client: &client,
                elector_data: &elector_data,
                current: current_elections,
                config: &config,
                recover_stake: true,
            });

            let deadline = Duration::from_secs(
                elections_end
                    .saturating_sub(elections_end_offset)
                    .saturating_sub(now_sec()) as u64,
            );

            match tokio::time::timeout(deadline, elect_fut).await {
                Ok(Ok(())) => tracing::info!("elections successful"),
                Ok(Err(e)) => return Err(e),
                Err(_) => tracing::warn!("elections deadline reached"),
            }

            // Done
            interval = elections_end.saturating_sub(now_sec());
        }
    }
}

/// Manually participate in validator elections (once).
#[derive(Parser)]
struct CmdOnce {
    #[clap(flatten)]
    control: ControlArgs,

    /// Path to validator keys. Default: `$TYCHO_HOME/validator_keys.json`
    #[clap(long)]
    sign: Option<PathBuf>,

    /// Path to node keys. Default: `$TYCHO_HOME/node_keys.json`
    #[clap(long)]
    node_keys: Option<PathBuf>,

    /// Stake size in nano tokens.
    #[clap(short, long)]
    stake: u128,

    /// Max stake factor. Uses config by default.
    #[clap(long)]
    stake_factor: Option<u32>,

    /// Skip waiting for the message delivery.
    #[clap(long)]
    no_wait: bool,
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
            let config = client.get_blockchain_config().await?;
            let wallet = Wallet::new(&client, &wallet_keys.as_secret(), config.signature_id);

            config.check_stake(self.stake)?;
            let stake_factor = config.compute_stake_factor(self.stake_factor)?;

            // Get current elections
            let (_, elector_data) = client.get_elector_data().await?;
            let Some(Ref(elections)) = elector_data.current_election else {
                return print_json(ParticipateStatus::NoElections);
            };

            // Check for an existing stake
            let adnl_addr = HashBytes(adnl_addr.to_bytes());
            let validator_key = adnl_addr;

            let mut existing_stake = 0;
            let mut update_member = true;
            if let Some(member) = elections.members.get(&validator_key) {
                anyhow::ensure!(
                    member.src_addr == wallet.address.address,
                    "can make stakes for a public key from one address only"
                );

                existing_stake = member.msg_value.into_inner();
                update_member = adnl_addr != member.adnl_addr || stake_factor != member.max_factor;
            }

            let stake_diff = self.stake.saturating_sub(existing_stake);
            anyhow::ensure!(
                (stake_diff << 12) >= elections.total_stake.into_inner(),
                "stake diff is too small"
            );

            let message = if stake_diff > 0 || update_member {
                // Send stake
                let message = wallet
                    .transfer(ElectionsContext::make_elector_message(
                        &wallet.address,
                        &node_keys,
                        elections.elect_at,
                        stake_diff,
                        stake_factor,
                        config.signature_id,
                    ))
                    .await?;

                if !self.no_wait {
                    wallet.wait_delivered(&message).await?;
                }

                Some(message)
            } else {
                None
            };

            // Done
            print_json(ParticipateStatus::Participating {
                message,
                elections_id: elections.elect_at,
                elections_end: elections.elect_close,
                public: validator_key,
                adnl_addr,
                stake: existing_stake + stake_diff,
                max_factor: stake_factor,
            })
        })
    }
}

#[derive(Serialize)]
#[serde(tag = "status")]
enum ParticipateStatus {
    NoElections,
    Participating {
        message: Option<SentMessage>,
        elections_id: u32,
        elections_end: u32,
        public: HashBytes,
        adnl_addr: HashBytes,
        #[serde(with = "serde_helpers::string")]
        stake: u128,
        max_factor: u32,
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

    /// Skip waiting for the message delivery.
    #[clap(long)]
    no_wait: bool,
}

impl CmdRecover {
    fn run(self, args: BaseArgs) -> Result<()> {
        // Prepare keys
        let wallet_keys = NodeKeys::from_file(&args.validator_keys_path(self.sign.as_ref()))?;

        // Use client
        self.control.rt(args, move |client| async move {
            let config = client.get_blockchain_config().await?;
            let wallet = Wallet::new(&client, &wallet_keys.as_secret(), config.signature_id);

            // Find reward
            let (_, elector_data) = client.get_elector_data().await?;
            let Some(to_recover) = elector_data.credits.get(&wallet.address.address) else {
                return print_json(RecoverStatus::NoReward);
            };

            // Send recover message
            let message = wallet
                .transfer(ElectionsContext::make_recover_msg())
                .await?;

            if !self.no_wait {
                wallet.wait_delivered(&message).await?;
            }

            // Done
            print_json(RecoverStatus::Recovered {
                message,
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
        message: SentMessage,
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

    /// Skip waiting for the message delivery.
    #[clap(long)]
    no_wait: bool,
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
            let config = client.get_blockchain_config().await?;
            let wallet = Wallet::new(&client, &wallet_keys.as_secret(), config.signature_id);

            let amount = match self.amount {
                None => Amount::All,
                Some(amount) => Amount::Exact(amount),
            };
            let message = wallet
                .transfer(InternalMessage {
                    to: self.dest,
                    amount,
                    bounce: self.bounce,
                    payload,
                })
                .await?;

            if !self.no_wait {
                wallet.wait_delivered(&message).await?;
            }

            print_json(serde_json::json!({
                "message": message,
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
            let (_, elector_data) = client.get_elector_data().await?;
            print_json(elector_data)
        })
    }
}

const ELECTOR_ADDR: StdAddr = StdAddr::new(-1, HashBytes([0x33; 32]));
const ONE_CC: u128 = 1_000_000_000;

// === Elections Logic ===

struct ElectionsContext<'a> {
    client: &'a Client,
    elector_data: &'a elector::data::PartialElectorData,
    current: &'a elector::data::CurrentElectionData,
    config: &'a ParsedBlockchainConfig,
    recover_stake: bool,
}

impl ElectionsContext<'_> {
    fn make_elector_message(
        wallet: &StdAddr,
        node_keys: &ed25519::KeyPair,
        election_id: u32,
        stake: u128,
        stake_factor: u32,
        signature_id: Option<i32>,
    ) -> InternalMessage {
        let adnl_addr = HashBytes(node_keys.public_key.to_bytes());
        let validator_key = adnl_addr;

        // Build payload signature
        let signature = {
            let data_to_sign = build_elections_data_to_sign(
                election_id,
                stake_factor,
                &wallet.address,
                &adnl_addr,
            );

            let data_to_sign = extend_signature_with_id(&data_to_sign, signature_id);
            node_keys.sign_raw(&data_to_sign).to_vec()
        };

        // Build elections payload
        let payload = elector::methods::participate_in_elections()
            .encode_internal_input(&[ParticiateInElectionsInput {
                query_id: now_millis(),
                validator_key,
                stake_at: election_id,
                max_factor: stake_factor,
                adnl_addr,
                signature,
            }
            .into_abi()
            .named("input")])
            .unwrap()
            .build()
            .unwrap();

        // Final message
        InternalMessage {
            to: ELECTOR_ADDR,
            amount: Amount::Exact(stake + ONE_CC),
            bounce: true,
            payload,
        }
    }

    fn make_recover_msg() -> InternalMessage {
        let payload = elector::methods::recover_stake()
            .encode_internal_input(&[now_millis().into_abi().named("query_id")])
            .unwrap()
            .build()
            .unwrap();

        InternalMessage {
            to: ELECTOR_ADDR,
            amount: Amount::Exact(ONE_CC),
            bounce: true,
            payload,
        }
    }
}

struct SimpleValidatorParams {
    wallet_secret: ed25519::SecretKey,
    node_secret: ed25519::SecretKey,
    stake_per_round: u128,
    stake_factor: Option<u32>,
}

impl SimpleValidatorParams {
    async fn elect(self, ctx: ElectionsContext<'_>) -> Result<()> {
        ctx.config.check_stake(self.stake_per_round)?;
        let stake_factor = ctx.config.compute_stake_factor(self.stake_factor)?;
        let wallet = Wallet::new(&ctx.client, &self.wallet_secret, ctx.config.signature_id);

        // Try to recover tokens
        if ctx.recover_stake {
            if let Some(stake) = ctx.elector_data.credits.get(&wallet.address.address) {
                // TODO: Lock some guard

                tracing::info!(stake = stake.into_inner(), "recovering stake");

                // TODO: Wait for balance
                async {
                    let message = wallet
                        .transfer(ElectionsContext::make_recover_msg())
                        .await?;
                    wallet.wait_delivered(&message).await
                }
                .await
                .context("failed to recover stake")?;
            }
        }

        // Check for an existing stake
        let node_keys = ed25519::KeyPair::from(&self.node_secret);
        let validator_key = HashBytes(node_keys.public_key.to_bytes());

        if let Some(member) = ctx.current.members.get(&validator_key) {
            tracing::info!(
                election_id = ctx.current.elect_at,
                ?member,
                "validator already elected"
            );
            return Ok(());
        }

        // Send stake
        tracing::info!(
            election_id = ctx.current.elect_at,
            address = %wallet.address,
            stake = self.stake_per_round,
            stake_factor,
            "electing as single",
        );

        // TODO: Wait for balance
        async {
            let message = wallet
                .transfer(ElectionsContext::make_elector_message(
                    &wallet.address,
                    &node_keys,
                    ctx.current.elect_at,
                    self.stake_per_round,
                    stake_factor,
                    ctx.config.signature_id,
                ))
                .await?;
            wallet.wait_delivered(&message).await
        }
        .await
        .context("failed to send stake")?;

        // Done
        tracing::info!("sent validator stake");
        Ok(())
    }
}

// === Wallet Stuff ===

struct Wallet {
    client: Client,
    address: StdAddr,
    secret: ed25519_dalek::SigningKey,
    public: ed25519_dalek::VerifyingKey,
    signature_id: Option<i32>,
}

impl Wallet {
    fn new(client: &Client, secret: &ed25519::SecretKey, signature_id: Option<i32>) -> Self {
        let address = wallet::compute_address(-1, &ed25519::PublicKey::from(secret));

        let secret = ed25519_dalek::SigningKey::from_bytes(secret.as_bytes());
        let public = ed25519_dalek::VerifyingKey::from(&secret);

        Self {
            client: client.clone(),
            address,
            secret,
            public,
            signature_id,
        }
    }

    async fn transfer(&self, msg: InternalMessage) -> Result<SentMessage> {
        let account = {
            let (_, account) = self.client.get_account_state(&self.address).await?;
            account.context("wallet account does not exist")?
        };

        let init = match account.state {
            AccountState::Active(_) => None,
            AccountState::Uninit => Some(wallet::make_state_init(
                &ed25519::PublicKey::from_bytes(self.public.to_bytes()).unwrap(),
            )),
            AccountState::Frozen(_) => anyhow::bail!("wallet account is frozen"),
        };

        let (value, flags) = match msg.amount {
            Amount::Exact(value) => (value, wallet::MSG_FLAGS_SIMPLE_SEND),
            Amount::All => (0, wallet::MSG_FLAGS_SEND_ALL),
        };
        anyhow::ensure!(
            account.balance.tokens.into_inner() >= value,
            "insufficient balance"
        );

        let AbiValue::Tuple(inputs) = wallet::methods::SendTransactionInputs {
            dest: msg.to,
            value,
            bounce: msg.bounce,
            flags,
            payload: msg.payload,
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
            .sign(&self.secret, self.signature_id)?;
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
            timestamp: now_ms,
            hash: *message_cell.repr_hash(),
            expire_at,
        })
    }

    // TODO: Use some better way to determine the delivery status.
    async fn wait_delivered(&self, message: &SentMessage) -> Result<()> {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;

            let (timings, account) = self.client.get_account_state(&self.address).await?;

            'check: {
                let Some(account) = account else {
                    // Account not found
                    break 'check;
                };

                let AccountState::Active(StateInit {
                    data: Some(data), ..
                }) = account.state
                else {
                    // Only deployed account accepts messages
                    break 'check;
                };

                let Ok(data) = data.as_slice() else {
                    break 'check;
                };

                if let Ok(stored_timestamp) = data.get_u64(256) {
                    if stored_timestamp >= message.timestamp {
                        return Ok(());
                    }
                }
            }

            if timings.gen_utime > message.expire_at {
                anyhow::bail!("message was not delivered");
            }
        }
    }
}

#[derive(Debug, Clone)]
struct InternalMessage {
    to: StdAddr,
    amount: Amount,
    bounce: bool,
    payload: Cell,
}

#[derive(Debug, Clone, Copy)]
enum Amount {
    Exact(u128),
    All,
}

#[derive(Debug, Clone, Copy, Serialize)]
struct SentMessage {
    #[serde(skip)]
    timestamp: u64,
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

    /// Use rpc even when the control socket file exists.
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
        init_logger_simple("info");

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
    async fn get_elector_data(&self) -> Result<(Timings, elector::data::PartialElectorData)> {
        static ELECTOR_ABI: OnceLock<AbiType> = OnceLock::new();

        let (timings, account) = self.get_account_state(&ELECTOR_ADDR).await?;
        let account = account.context("elector account not found")?;

        let elector_data = match account.state {
            AccountState::Active(state) => state.data.context("elector data is empty")?,
            _ => anyhow::bail!("invalid elector state"),
        };

        let abi_type = ELECTOR_ABI.get_or_init(elector::data::PartialElectorData::abi_type);
        let data =
            AbiValue::load_partial(&abi_type, AbiVersion::V2_1, &mut elector_data.as_slice()?)
                .and_then(elector::data::PartialElectorData::from_abi)?;

        Ok((timings, data))
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

    async fn get_account_state(&self, addr: &StdAddr) -> Result<(Timings, Option<Account>)> {
        match self.inner.as_ref() {
            ClientImpl::Jrpc(rpc) => match rpc.get_account(addr).await? {
                jrpc_client::AccountStateResponse::Exists {
                    account, timings, ..
                } => {
                    let timings = Timings {
                        gen_utime: timings.gen_utime,
                    };
                    Ok((timings, Some(*account)))
                }
                jrpc_client::AccountStateResponse::NotExists { timings } => {
                    let timings = Timings {
                        gen_utime: timings.gen_utime,
                    };
                    Ok((timings, None))
                }
                jrpc_client::AccountStateResponse::Unchanged { .. } => {
                    anyhow::bail!("unexpected response")
                }
            },
            ClientImpl::Control(control) => {
                let res = control.get_account_state(addr).await?.parse()?;
                let account = res.state.load_account()?;
                let timings = Timings {
                    gen_utime: res.gen_utime,
                };
                Ok((timings, account))
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

struct Timings {
    gen_utime: u32,
}

struct ParsedBlockchainConfig {
    signature_id: Option<i32>,
    stake_params: ValidatorStakeParams,
    config: BlockchainConfig,
}

impl ParsedBlockchainConfig {
    fn new(global_id: i32, _mc_seqno: u32, config: BlockchainConfig) -> Result<Self> {
        let version = config.get_global_version()?;
        let signature_id = version
            .capabilities
            .contains(GlobalCapability::CapSignatureWithId)
            .then_some(global_id);

        let stake_params = config.get_validator_stake_params()?;

        Ok(Self {
            signature_id,
            stake_params,
            config,
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

    fn compute_stake_factor(&self, stake_factor: Option<u32>) -> Result<u32> {
        let stake_factor = stake_factor.unwrap_or(self.stake_params.max_stake_factor);
        anyhow::ensure!(stake_factor >= (1 << 16), "max factor is too small");
        anyhow::ensure!(
            stake_factor <= self.stake_params.max_stake_factor,
            "max factor is too big"
        );
        Ok(stake_factor)
    }

    fn compute_elections_timeline(&self, now: u32) -> Result<Timeline> {
        let timings = self.config.get_election_timings()?;
        let current_vset = self.config.get_current_validator_set()?;

        let round_end = current_vset.utime_until;
        let elections_start = round_end.saturating_sub(timings.elections_start_before);
        let elections_end = round_end.saturating_sub(timings.elections_end_before);

        if let Some(until_elections) = elections_start.checked_sub(now) {
            return Ok(Timeline::BeforeElections {
                until_elections_start: until_elections,
            });
        }

        if let Some(until_end) = elections_end.checked_sub(now) {
            return Ok(Timeline::Elections {
                since_elections_start: now.saturating_sub(elections_start),
                until_elections_end: until_end,
                elections_end,
            });
        }

        Ok(Timeline::AfterElections {
            until_round_end: round_end.saturating_sub(now),
        })
    }
}

#[derive(Debug, Clone, Copy)]
enum Timeline {
    BeforeElections {
        until_elections_start: u32,
    },
    Elections {
        since_elections_start: u32,
        until_elections_end: u32,
        elections_end: u32,
    },
    AfterElections {
        until_round_end: u32,
    },
}
