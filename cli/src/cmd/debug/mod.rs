use anyhow::Result;
use clap::{Parser, Subcommand};

mod mempool;

/// Debug node parts.
#[derive(Parser)]
pub struct Cmd {
    #[clap(subcommand)]
    cmd: SubCmd,
}

impl Cmd {
    pub fn run(self) -> Result<()> {
        match self.cmd {
            SubCmd::Mempool(cmd) => cmd.run(),
        }
    }
}

#[derive(Subcommand)]
enum SubCmd {
    Mempool(mempool::CmdRun),
}
