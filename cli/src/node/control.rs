use std::future::Future;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Parser;
use tycho_control::ControlClient;
use tycho_util::futures::JoinTask;

use crate::util::signal;

/// Ping the control server.
#[derive(Parser)]
pub struct CmdPing {
    #[clap(short, long)]
    sock: Option<PathBuf>,
}

impl CmdPing {
    pub fn run(self) -> Result<()> {
        control_rt(self.sock, |client| async move {
            let timestamp = client.ping().await?;
            println!("{}", timestamp);
            Ok(())
        })
    }
}

fn control_rt<P, F, FT>(sock: Option<P>, f: F) -> Result<()>
where
    P: AsRef<Path>,
    F: FnOnce(ControlClient) -> FT + Send + 'static,
    FT: Future<Output = Result<()>> + Send,
{
    let sock = match sock {
        Some(sock) => sock.as_ref().to_owned(),
        None => match std::env::var("TYCHO_CONTROL_SOCK") {
            Ok(sock) => PathBuf::from(sock),
            Err(_) => PathBuf::from(tycho_control::DEFAULT_SOCKET_PATH),
        },
    };

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async move {
            let run_fut = JoinTask::new(async move {
                let client = ControlClient::connect(sock)
                    .await
                    .context("failed to connect to control server")?;
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
