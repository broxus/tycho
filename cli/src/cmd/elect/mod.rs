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
use tycho_block_util::config::{
    apply_price_factor, build_elections_data_to_sign, compute_gas_price_factor,
};
use tycho_control::ControlClient;
use tycho_network::PeerId;
use tycho_util::cli::logger::init_logger_simple;
use tycho_util::cli::signal;
use tycho_util::futures::JoinTask;
use tycho_util::time::{now_millis, now_sec};

use crate::node::{ElectionsConfig, NodeKeys};
use crate::util::elector::data::Ref;
use crate::util::elector::methods::ParticiateInElectionsInput;
use crate::util::jrpc_client::{self, JrpcClient};
use crate::util::{elector, wallet};
use tycho_cli_models::FpTokens;
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
// TODO: Move offset and other params to `elections.json`.
#[derive(Parser)]
struct CmdRun {
    #[clap(flatten)]
    control: ControlArgs,

    /// Path to elections config. Default: `$TYCHO_HOME/elections.json`
    #[clap(short, long)]
    config: Option<PathBuf>,

    /// Path to node keys. Default: `$TYCHO_HOME/node_keys.json`
    #[clap(long)]
    node_keys: Option<PathBuf>,

    /// Overwrite the stake size.
    #[clap(short, long)]
    stake: Option<FpTokens>,

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
        let elections_config_path = args.elections_config_path(self.config.as_ref());
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
                    .try_run(&f, &elections_config_path, &node_keys_path, before_repeat)
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
        elections_config_path: &Path,
        node_keys_path: &Path,
        mut before_repeat: F,
    ) -> Result<()>
    where
        F: FnMut(),
    {
        tracing::info!("started validation loop");

        // Max time diff considered ok.
        const MAX_TIME_DIFF: u32 = 600; // Seconds

        // Interval between failed elections end polling attempts.
        const FAILED_ELECTIONS_INTERVAL: u32 = 300; // Seconds

        // Interval between polling attempts on empty elections state.
        const EMPTY_ELECTIONS_INTERVAL: u32 = 60; // Seconds

        // Minimal timeout for participation future.
        const MIN_ELECTION_DEADLINE: u32 = 60; // Seconds

        // Minimal interval after participation future.
        const MIN_ELECTION_INTERVAL: u32 = 60; // Seconds

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
            interval = std::cmp::max(interval, 10);
            tokio::time::sleep(Duration::from_secs(interval as u64)).await;

            // Create a client
            let client = f.create().await?;

            // Compute elections timeline
            let config = client
                .get_blockchain_config()
                .await
                .context("failed to get blockchain config")?;
            let (Timings { gen_utime }, elector_data) = client
                .get_elector_data(&config.elector_addr)
                .await
                .context("failed to get elector data")?;

            let now = now_sec();
            let time_diff = now.saturating_sub(gen_utime).min(MAX_TIME_DIFF);
            let timeline = config
                .compute_elections_timeline(now)
                .context("failed to compute elections timeline")?;

            tracing::info!(gen_utime, time_diff, ?timeline);

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
                        break 'after_end gen_utime + time_diff + FAILED_ELECTIONS_INTERVAL;
                    }

                    tracing::info!("waiting for the new round to start");
                    interval = until_round_end;
                    continue 'outer;
                }
            };

            // Participate in elections
            let Some(Ref(current_elections)) = &elector_data.current_election else {
                tracing::info!("no current elections in the elector state");
                interval = std::cmp::max(time_diff, EMPTY_ELECTIONS_INTERVAL); // Wait for sync
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
                        interval = until_unfreeze + time_diff; // Wait for time diff to be able to recover the stake
                        continue;
                    }
                }
            }

            // Try elect
            let node_keys =
                NodeKeys::from_file(node_keys_path).context("failed to load node keys")?;
            let params = match ElectionsConfig::from_file(elections_config_path)
                .context("failed to load elections config")?
            {
                ElectionsConfig::Simple(simple) => SimpleValidatorParams {
                    wallet_secret: ed25519::SecretKey::from_bytes(simple.wallet_secret.0),
                    node_secret: node_keys.as_secret(),
                    stake_per_round: self.stake.or(simple.stake).context("no stake specified")?,
                    stake_factor: self.stake_factor.or(simple.stake_factor),
                },
            };

            let elect_fut = params.elect(ElectionsContext {
                client: &client,
                elector_data: &elector_data,
                current: current_elections,
                config: &config,
                recover_stake: true,
            });

            let deadline = Duration::from_secs(
                elections_end
                    .saturating_sub(elections_end_offset)
                    .saturating_sub(now)
                    .max(MIN_ELECTION_DEADLINE) as u64,
            );

            match tokio::time::timeout(deadline, elect_fut).await {
                Ok(Ok(())) => tracing::info!("elections successful"),
                Ok(Err(e)) => return Err(e),
                Err(_) => tracing::warn!("elections deadline reached"),
            }

            // Done
            interval = elections_end
                .saturating_sub(now_sec())
                .max(MIN_ELECTION_INTERVAL);
        }
    }
}

/// Manually participate in validator elections (once).
#[derive(Parser)]
struct CmdOnce {
    #[clap(flatten)]
    control: ControlArgs,

    /// Path to elections config. Default: `$TYCHO_HOME/elections.json`
    #[clap(short, long)]
    config: Option<PathBuf>,

    /// Path to node keys. Default: `$TYCHO_HOME/node_keys.json`
    #[clap(long)]
    node_keys: Option<PathBuf>,

    /// Overwrite the stake size.
    #[clap(short, long)]
    stake: Option<FpTokens>,

    /// Max stake factor. Uses config by default.
    #[clap(long)]
    stake_factor: Option<u32>,

    #[clap(flatten)]
    transfer: TransferArgs,
}

impl CmdOnce {
    fn run(self, args: BaseArgs) -> Result<()> {
        // Prepare keys
        let node_keys = {
            let path = args.node_keys_path(self.node_keys.as_ref());
            let keys = NodeKeys::from_file(path).context("failed to load node keys")?;
            Arc::new(ed25519::KeyPair::from(&keys.as_secret()))
        };
        let adnl_addr = PeerId::from(node_keys.public_key);

        let ElectionsConfig::Simple(simple) = {
            let path = args.elections_config_path(self.config.as_ref());
            ElectionsConfig::from_file(path).context("failed to load elections config")?
        };

        let stake = self.stake.or(simple.stake).context("no stake specified")?;
        let stake_factor = self.stake_factor.or(simple.stake_factor);

        // Use client
        self.control.rt(args, move |client| async move {
            let config = client.get_blockchain_config().await?;
            let price_factor = config.compute_price_factor(true)?;
            let wallet = Wallet::new(&client, &simple.as_secret(), config.signature_id);

            config.check_stake(*stake)?;
            let stake_factor = config.compute_stake_factor(stake_factor)?;

            // Get current elections
            let (_, elector_data) = client.get_elector_data(&config.elector_addr).await?;
            let Some(Ref(elections)) = elector_data.current_election else {
                return tycho_cli_models::print_json(ParticipateStatus::NoElections);
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

                existing_stake = *member.msg_value;
                update_member =
                    adnl_addr != member.adnl_addr || stake_factor != member.stake_factor;
            }

            let stake_diff = stake.saturating_sub(existing_stake);
            let message = if stake_diff > 0 || update_member {
                anyhow::ensure!(
                    (stake_diff << 12) >= *elections.total_stake,
                    "stake diff is too small"
                );

                // Send stake
                let internal = ElectionsContext::make_elector_message(ElectorMessage {
                    elector_addr: &config.elector_addr,
                    wallet: &wallet.address,
                    node_keys: &node_keys,
                    election_id: elections.elect_at,
                    stake: stake_diff,
                    stake_factor,
                    price_factor,
                    signature_id: config.signature_id,
                });
                wallet
                    .transfer(internal, self.transfer.into_params(price_factor))
                    .await
                    .map(Some)?
            } else {
                None
            };

            // Done
            tycho_cli_models::print_json(ParticipateStatus::Participating {
                message,
                elections_id: elections.elect_at,
                elections_end: elections.elect_close,
                public: validator_key,
                adnl_addr,
                stake: (existing_stake + stake_diff).into(),
                stake_factor,
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
        stake: FpTokens,
        stake_factor: u32,
    },
}

/// Recover stake.
#[derive(Parser)]
struct CmdRecover {
    #[clap(flatten)]
    control: ControlArgs,

    /// Path to elections config. Default: `$TYCHO_HOME/elections.json`
    #[clap(short, long)]
    config: Option<PathBuf>,

    #[clap(flatten)]
    transfer: TransferArgs,
}

impl CmdRecover {
    fn run(self, args: BaseArgs) -> Result<()> {
        // Prepare keys
        let ElectionsConfig::Simple(simple) = {
            let path = args.elections_config_path(self.config.as_ref());
            ElectionsConfig::from_file(path).context("failed to load elections config")?
        };

        // Use client
        self.control.rt(args, move |client| async move {
            let config = client.get_blockchain_config().await?;
            let price_factor = config.compute_price_factor(true)?;
            let wallet = Wallet::new(&client, &simple.as_secret(), config.signature_id);

            // Find reward
            let (_, elector_data) = client.get_elector_data(&config.elector_addr).await?;
            let Some(to_recover) = elector_data.credits.get(&wallet.address.address) else {
                return tycho_cli_models::print_json(RecoverStatus::NoReward);
            };

            // Send recover message
            let internal = ElectionsContext::make_recover_msg(&config.elector_addr, price_factor);
            let message = wallet
                .transfer(internal, self.transfer.into_params(price_factor))
                .await?;

            // Done
            tycho_cli_models::print_json(RecoverStatus::Recovered {
                message,
                amount: *to_recover,
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
        amount: FpTokens,
    },
}

/// Withdraw funds from the validator wallet.
#[derive(Parser)]
struct CmdWithdraw {
    #[clap(flatten)]
    control: ControlArgs,

    /// Path to elections config. Default: `$TYCHO_HOME/elections.json`
    #[clap(short, long)]
    config: Option<PathBuf>,

    /// Destination address.
    #[clap(short, long, allow_hyphen_values(true))]
    dest: StdAddr,

    /// Amount in tokens.
    #[clap(short, long, required_unless_present = "all")]
    amount: Option<FpTokens>,

    /// Withdraw everything from the wallet.
    #[clap(long)]
    all: bool,

    /// Withdraw everything from the wallet reserving at least an `amount` of tokens.
    #[clap(long, conflicts_with = "all")]
    all_but: bool,

    /// Sets `bounce` message flag.
    #[clap(short, long)]
    bounce: bool,

    /// Withdrawal message payload as a base64-encoded BOC.
    #[clap(short, long)]
    payload: Option<String>,

    #[clap(flatten)]
    transfer: TransferArgs,
}

impl CmdWithdraw {
    fn run(self, args: BaseArgs) -> Result<()> {
        // Prepare keys
        let ElectionsConfig::Simple(simple) = {
            let path = args.elections_config_path(self.config.as_ref());
            ElectionsConfig::from_file(path).context("failed to load elections config")?
        };

        // Parse paylout
        let payload = match self.payload {
            None => Cell::default(),
            Some(payload) => Boc::decode_base64(payload).context("invalid payload")?,
        };

        // Use client
        self.control.rt(args, move |client| async move {
            let config = client.get_blockchain_config().await?;
            let price_factor = config.compute_price_factor(true)?;
            let wallet = Wallet::new(&client, &simple.as_secret(), config.signature_id);

            let amount = match self.amount {
                None => Amount::All,
                Some(amount) if self.all_but => Amount::AllBut(*amount),
                Some(amount) => Amount::Exact(*amount),
            };

            let internal = InternalMessage {
                to: self.dest,
                amount,
                bounce: self.bounce,
                payload,
            };
            let message = wallet
                .transfer(internal, self.transfer.into_params(price_factor))
                .await?;

            tycho_cli_models::print_json(serde_json::json!({
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
            let config = client.get_blockchain_config().await?;
            let (_, elector_data) = client.get_elector_data(&config.elector_addr).await?;
            tycho_cli_models::print_json(elector_data)
        })
    }
}

const ONE_CC: u128 = 1_000_000_000;

// === Elections Logic ===

struct ElectionsContext<'a> {
    client: &'a Client,
    elector_data: &'a elector::data::PartialElectorData,
    current: &'a elector::data::CurrentElectionData,
    config: &'a ParsedBlockchainConfig,
    recover_stake: bool,
}

struct ElectorMessage<'a> {
    elector_addr: &'a StdAddr,
    wallet: &'a StdAddr,
    node_keys: &'a ed25519::KeyPair,
    election_id: u32,
    stake: u128,
    stake_factor: u32,
    price_factor: u64,
    signature_id: Option<i32>,
}

impl ElectionsContext<'_> {
    fn make_elector_message(ctx: ElectorMessage<'_>) -> InternalMessage {
        let ElectorMessage {
            elector_addr,
            wallet,
            node_keys,
            election_id,
            stake,
            stake_factor,
            price_factor,
            signature_id,
        } = ctx;

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
                stake_factor,
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
            to: elector_addr.clone(),
            amount: Amount::Exact(stake + apply_price_factor(ONE_CC, price_factor)),
            bounce: true,
            payload,
        }
    }

    fn make_recover_msg(elector_addr: &StdAddr, price_factor: u64) -> InternalMessage {
        let payload = elector::methods::recover_stake()
            .encode_internal_input(&[now_millis().into_abi().named("query_id")])
            .unwrap()
            .build()
            .unwrap();

        InternalMessage {
            to: elector_addr.clone(),
            amount: Amount::Exact(apply_price_factor(ONE_CC, price_factor)),
            bounce: true,
            payload,
        }
    }
}

struct SimpleValidatorParams {
    wallet_secret: ed25519::SecretKey,
    node_secret: ed25519::SecretKey,
    stake_per_round: FpTokens,
    stake_factor: Option<u32>,
}

impl SimpleValidatorParams {
    async fn elect(self, ctx: ElectionsContext<'_>) -> Result<()> {
        ctx.config.check_stake(*self.stake_per_round)?;
        let price_factor = ctx.config.compute_price_factor(true)?;
        let stake_factor = ctx.config.compute_stake_factor(self.stake_factor)?;
        let wallet = Wallet::new(ctx.client, &self.wallet_secret, ctx.config.signature_id);

        // Try to recover tokens
        if ctx.recover_stake {
            if let Some(stake) = ctx.elector_data.credits.get(&wallet.address.address) {
                // TODO: Lock some guard

                tracing::info!(%stake, "recovering stake");
                let message =
                    ElectionsContext::make_recover_msg(&ctx.config.elector_addr, price_factor);
                wallet
                    .transfer(message, TransferParams::reliable(price_factor))
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
            stake = %self.stake_per_round,
            stake_factor,
            "electing as single",
        );

        let message = ElectionsContext::make_elector_message(ElectorMessage {
            elector_addr: &ctx.config.elector_addr,
            wallet: &wallet.address,
            node_keys: &node_keys,
            election_id: ctx.current.elect_at,
            stake: *self.stake_per_round,
            stake_factor,
            price_factor,
            signature_id: ctx.config.signature_id,
        });
        wallet
            .transfer(message, TransferParams::reliable(price_factor))
            .await
            .context("failed to send stake")?;

        // Done
        tracing::info!("sent validator stake");
        Ok(())
    }
}

// === Transfer Args ===

#[derive(Parser, Clone, Copy)]
struct TransferArgs {
    /// Wait for the account balance to be enough.
    #[clap(long)]
    wait_balance: bool,

    /// Skip waiting for the message delivery.
    #[clap(short, long)]
    ignore_delivery: bool,

    /// Message TTL.
    #[clap(long, value_parser = humantime::parse_duration, default_value = "40s")]
    ttl: Duration,
}

impl TransferArgs {
    fn into_params(self, price_factor: u64) -> TransferParams {
        TransferParams {
            reserve: if self.wait_balance {
                apply_price_factor(TransferParams::DEFAULT_RESERVE, price_factor)
            } else {
                0
            },
            timeout: self.ttl,
            wait_for_balance: self.wait_balance,
            wait_for_delivery: self.ignore_delivery,
        }
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

    async fn transfer(&self, msg: InternalMessage, params: TransferParams) -> Result<SentMessage> {
        const BALANCE_POLL_INTERVAL: Duration = Duration::from_secs(5);

        enum TargetBalance {
            Sufficient { to_send: u128 },
            Insufficient { target_balance: u128 },
        }

        #[derive(Clone, Copy)]
        enum Value {
            Exact(u128),
            AllBut(u128),
        }

        impl Value {
            fn target_balance(self, account_balance: u128, mut reserve: u128) -> TargetBalance {
                match self {
                    Self::Exact(to_send) => {
                        let target_balance = to_send.saturating_add(reserve);
                        if account_balance < target_balance {
                            TargetBalance::Insufficient { target_balance }
                        } else {
                            TargetBalance::Sufficient { to_send }
                        }
                    }
                    Self::AllBut(all_but) => {
                        reserve = reserve.saturating_add(all_but);
                        match account_balance.checked_sub(reserve) {
                            Some(to_send) => TargetBalance::Sufficient { to_send },
                            None => TargetBalance::Insufficient {
                                target_balance: reserve,
                            },
                        }
                    }
                }
            }
        }

        let (value, flags) = match msg.amount {
            Amount::Exact(value) => (Value::Exact(value), wallet::MSG_FLAGS_SIMPLE_SEND),
            Amount::AllBut(value) => (Value::AllBut(value), wallet::MSG_FLAGS_SEPARATE_SEND),
            Amount::All => (Value::Exact(0), wallet::MSG_FLAGS_SEND_ALL),
        };

        let mut prev_balance = None;
        let (value, init) = loop {
            let account = {
                let (_, account) = self.client.get_account_state(&self.address).await?;
                account.context("wallet account does not exist")?
            };
            let account_balance = account.balance.tokens.into_inner();

            match value.target_balance(account_balance, params.reserve) {
                TargetBalance::Insufficient { target_balance } => {
                    if !params.wait_for_balance {
                        anyhow::bail!(
                            "insufficient balance (account_balance={}, target_balance: {})",
                            FpTokens(account_balance),
                            FpTokens(target_balance),
                        );
                    }

                    if prev_balance != Some(account_balance) {
                        tracing::warn!(
                            account_balance = %FpTokens(account_balance),
                            target_balance = %FpTokens(target_balance),
                            "insufficient balance, waiting for refill"
                        );
                    }
                    prev_balance = Some(account_balance);

                    tokio::time::sleep(BALANCE_POLL_INTERVAL).await;
                }
                TargetBalance::Sufficient { to_send } => {
                    break match account.state {
                        AccountState::Active(_) => (to_send, None),
                        AccountState::Uninit => {
                            let init = Some(wallet::make_state_init(
                                &ed25519::PublicKey::from_bytes(self.public.to_bytes()).unwrap(),
                            ));
                            (to_send, init)
                        }
                        AccountState::Frozen(_) => anyhow::bail!("wallet account is frozen"),
                    }
                }
            }
        };

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

        let ttl = params.timeout.as_secs().clamp(1, 60) as u32;

        let now_ms = now_millis();
        let expire_at = (now_ms / 1000) as u32 + ttl;
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

        let message = SentMessage {
            timestamp: now_ms,
            hash: *message_cell.repr_hash(),
            expire_at,
        };

        if params.wait_for_delivery {
            self.wait_delivered(&message).await?;
        }

        Ok(message)
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

#[derive(Default, Debug, Clone)]
struct TransferParams {
    reserve: u128,
    timeout: Duration,
    wait_for_balance: bool,
    wait_for_delivery: bool,
}

impl TransferParams {
    const DEFAULT_RESERVE: u128 = ONE_CC / 2;
    const DEFAULT_TIMEOUT: Duration = Duration::from_secs(40);

    fn reliable(price_factor: u64) -> Self {
        Self {
            reserve: apply_price_factor(Self::DEFAULT_RESERVE, price_factor),
            timeout: Self::DEFAULT_TIMEOUT,
            wait_for_balance: true,
            wait_for_delivery: true,
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
    AllBut(u128),
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
        init_logger_simple("info,tarpc=error");

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
    async fn get_elector_data(
        &self,
        elector_addr: &StdAddr,
    ) -> Result<(Timings, elector::data::PartialElectorData)> {
        static ELECTOR_ABI: OnceLock<AbiType> = OnceLock::new();

        let (timings, account) = self.get_account_state(elector_addr).await?;
        let account = account.context("elector account not found")?;

        let elector_data = match account.state {
            AccountState::Active(state) => state.data.context("elector data is empty")?,
            _ => anyhow::bail!("invalid elector state"),
        };

        let abi_type = ELECTOR_ABI.get_or_init(elector::data::PartialElectorData::abi_type);
        let data =
            AbiValue::load_partial(abi_type, AbiVersion::V2_1, &mut elector_data.as_slice()?)
                .and_then(elector::data::PartialElectorData::from_abi)?;

        Ok((timings, data))
    }

    async fn get_blockchain_config(&self) -> Result<ParsedBlockchainConfig> {
        match self.inner.as_ref() {
            ClientImpl::Jrpc(rpc) => {
                let res = rpc.get_config().await?;
                ParsedBlockchainConfig::new(res.global_id, res.config)
            }
            ClientImpl::Control(control) => {
                let res = control.get_blockchain_config().await?.parse()?;
                ParsedBlockchainConfig::new(res.global_id, res.config)
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
    elector_addr: StdAddr,
    signature_id: Option<i32>,
    stake_params: ValidatorStakeParams,
    config: BlockchainConfig,
}

impl ParsedBlockchainConfig {
    fn new(global_id: i32, config: BlockchainConfig) -> Result<Self> {
        let elector_addr = StdAddr::new(-1, config.get_elector_address()?);
        let version = config.get_global_version()?;
        let signature_id = version
            .capabilities
            .contains(GlobalCapability::CapSignatureWithId)
            .then_some(global_id);

        let stake_params = config.get_validator_stake_params()?;

        Ok(Self {
            elector_addr,
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

    fn compute_price_factor(&self, is_masterchain: bool) -> Result<u64> {
        let gas_price = self.config.get_gas_prices(is_masterchain)?.gas_price;
        compute_gas_price_factor(is_masterchain, gas_price)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn price_factor() {
        const BASE_GAS_PRICE: u64 = 10_000 << 16;

        fn compute_price_factor(new_gas_price: u64) -> u64 {
            new_gas_price.checked_shl(16).unwrap() / BASE_GAS_PRICE
        }

        // Base fee.
        let price_factor = compute_price_factor(BASE_GAS_PRICE);
        assert_eq!(price_factor, 1 << 16);
        assert_eq!(apply_price_factor(ONE_CC, price_factor), ONE_CC);

        // Increased fee.
        let price_factor = compute_price_factor(60 * BASE_GAS_PRICE);
        assert_eq!(apply_price_factor(ONE_CC, price_factor), ONE_CC * 60);

        // Increased fee (with decimals).
        let price_factor = compute_price_factor((30.5f64 * BASE_GAS_PRICE as f64) as u64);
        assert_eq!(
            apply_price_factor(ONE_CC, price_factor),
            (ONE_CC as f64 * 30.5f64) as u128
        );

        // Decreased fee (with decimals).
        let price_factor = compute_price_factor(BASE_GAS_PRICE / 2);
        assert_eq!(apply_price_factor(ONE_CC, price_factor), ONE_CC / 2);
    }
}
