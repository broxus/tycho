use std::future::Future;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use everscale_types::abi::{AbiValue, AbiVersion, FromAbi, WithAbiType};
use everscale_types::models::{Account, AccountState, StdAddr};
use everscale_types::prelude::*;
use reqwest::Url;
use tycho_control::ControlClient;
use tycho_util::cli::signal;
use tycho_util::futures::JoinTask;

use crate::util::jrpc_client::{self, JrpcClient};
use crate::util::{elector, print_json};
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
            SubCmd::GetState(cmd) => cmd.run(args),
        }
    }
}

#[derive(Subcommand)]
enum SubCmd {
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
            let account_state = client
                .get_account_state(&ELECTOR_ADDR)
                .await?
                .context("elector contract not found")?;

            let elector_data = match account_state.state {
                AccountState::Active(state) => state.data.context("elector data is empty")?,
                _ => anyhow::bail!("invalid elector state"),
            };

            let elector_data = AbiValue::load_partial(
                &elector::data::PartialElectorData::abi_type(),
                AbiVersion::V2_1,
                &mut elector_data.as_slice()?,
            )
            .and_then(elector::data::PartialElectorData::from_abi)?;

            print_json(elector_data)
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

                    let client = Client { inner };
                    f(client).await
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

struct Client {
    inner: ClientImpl,
}

impl Client {
    async fn get_account_state(&self, addr: &StdAddr) -> Result<Option<Account>> {
        match &self.inner {
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
}

enum ClientImpl {
    Control(ControlClient),
    Jrpc(JrpcClient),
}
