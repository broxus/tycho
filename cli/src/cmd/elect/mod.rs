use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use everscale_crypto::ed25519;
use everscale_types::abi::{AbiValue, AbiVersion, FromAbi, IntoAbi, WithAbiType};
use everscale_types::models::{
    Account, AccountState, ExtInMsgInfo, MsgInfo, OwnedMessage, StdAddr,
};
use everscale_types::prelude::*;
use reqwest::Url;
use tycho_control::ControlClient;
use tycho_util::cli::signal;
use tycho_util::futures::JoinTask;
use tycho_util::time::now_millis;

use crate::node::NodeKeys;
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
            SubCmd::Recover(cmd) => cmd.run(args),
            SubCmd::GetState(cmd) => cmd.run(args),
        }
    }
}

#[derive(Subcommand)]
enum SubCmd {
    Recover(CmdRecover),
    GetState(CmdGetState),
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

/// Recover stake.
#[derive(Parser)]
struct CmdRecover {
    #[clap(flatten)]
    control: ControlArgs,

    /// Path to the validator keys. Default: `$TYCHO_HOME/validator_keys.json`
    #[clap(long)]
    keys: Option<PathBuf>,
}

impl CmdRecover {
    fn run(self, args: BaseArgs) -> Result<()> {
        let keys_path = args.validator_keys_path(self.keys.as_ref());
        let keys = NodeKeys::from_file(&keys_path)?;

        self.control.rt(args, move |client| async move {
            let elector_data = client.get_elector_data().await?;
            let wallet = Wallet::new(client, &keys.as_secret());

            let Some(to_recover) = elector_data.credits.get(&wallet.address.address) else {
                return print_json(serde_json::json!({
                    "status": "NoReward",
                }));
            };

            let payload = elector::methods::recover_stake()
                .encode_internal_input(&[now_millis().into_abi().named("query_id")])?
                .build()?;
            wallet
                .transfer(&ELECTOR_ADDR, 1_000_000_000, true, payload)
                .await?;

            print_json(serde_json::json!({
                "status": "Recovered",
                "amount": to_recover.into_inner().to_string(),
            }))
        })
    }
}

const ELECTOR_ADDR: StdAddr = StdAddr::new(-1, HashBytes([0x33; 32]));

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
        tracing_subscriber::fmt::init();

        let sock = args.control_socket_path(self.control_socket.as_ref());

        let rpc = self.rpc.clone();
        let force_rpc = self.force_rpc;

        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(async move {
                let run_fut = JoinTask::new(async move {
                    let inner = 'client: {
                        if let Some(rpc) = rpc {
                            if force_rpc || !sock.exists() {
                                let rpc = JrpcClient::new(rpc)?;
                                break 'client ClientImpl::Jrpc(rpc);
                            }
                        }

                        let control = ControlClient::connect(sock)
                            .await
                            .context("failed to connect to control server")?;
                        ClientImpl::Control(control)
                    };

                    f(Client {
                        inner: Arc::new(inner),
                    })
                    .await
                });
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
        amount: u128,
        bounce: bool,
        payload: Cell,
    ) -> Result<()> {
        let account_state = self
            .client
            .get_account_state(&self.address)
            .await?
            .context("wallet account does not exist")?;

        anyhow::ensure!(
            account_state.balance.tokens.into_inner() >= amount,
            "insufficient balance"
        );

        let init = match account_state.state {
            AccountState::Active(_) => None,
            AccountState::Uninit => Some(wallet::make_state_init(
                &ed25519::PublicKey::from_bytes(self.public.to_bytes()).unwrap(),
            )),
            AccountState::Frozen(_) => anyhow::bail!("wallet account is frozen"),
        };

        let AbiValue::Tuple(inputs) = wallet::methods::SendTransactionInputs {
            dest: to.clone(),
            value: amount,
            bounce,
            flags: wallet::MSG_FLAGS,
            payload,
        }
        .into_abi() else {
            unreachable!();
        };

        let now_ms = now_millis();
        let expire_at = (now_ms / 1000) as u32 + 40;

        // TODO: Add support for signature id
        let signature_id = None;

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

        self.client.broadcast_message(message).await?;

        Ok(())
    }
}

#[derive(Clone)]
struct Client {
    inner: Arc<ClientImpl>,
}

impl Client {
    async fn get_elector_data(&self) -> Result<elector::data::PartialElectorData> {
        let account_state = self
            .get_account_state(&ELECTOR_ADDR)
            .await?
            .context("elector contract not found")?;

        let elector_data = match account_state.state {
            AccountState::Active(state) => state.data.context("elector data is empty")?,
            _ => anyhow::bail!("invalid elector state"),
        };

        AbiValue::load_partial(
            &elector::data::PartialElectorData::abi_type(),
            AbiVersion::V2_1,
            &mut elector_data.as_slice()?,
        )
        .and_then(elector::data::PartialElectorData::from_abi)
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

    async fn broadcast_message(&self, message: OwnedMessage) -> Result<()> {
        match self.inner.as_ref() {
            ClientImpl::Jrpc(rpc) => {
                let cell = CellBuilder::build_from(&message)?;
                rpc.send_message(cell.as_ref()).await
            }
            ClientImpl::Control(control) => control
                .broadcast_external_message(message)
                .await
                .map_err(Into::into),
        }
    }
}

enum ClientImpl {
    Control(ControlClient),
    Jrpc(JrpcClient),
}
