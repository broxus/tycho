use anyhow::Result;
use clap::{Parser, Subcommand};

pub(crate) mod bc;
mod dump_state;
mod gen_account;
mod gen_dht;
mod gen_key;
mod gen_zerostate;

/// Work with blockchain stuff.
#[derive(Parser)]
pub struct Cmd {
    #[clap(subcommand)]
    cmd: SubCmd,
}

impl Cmd {
    pub fn run(self) -> Result<()> {
        match self.cmd {
            SubCmd::GenDht(cmd) => cmd.run(),
            SubCmd::GenKey(cmd) => cmd.run(),
            SubCmd::GenZerostate(cmd) => cmd.run(),
            SubCmd::GenAccount(cmd) => cmd.run(),
            SubCmd::Bc(cmd) => cmd.run(),
            SubCmd::DumpState(cmd) => cmd.run(),
        }
    }
}

#[derive(Subcommand)]
enum SubCmd {
    GenDht(gen_dht::Cmd),
    GenKey(gen_key::Cmd),
    GenZerostate(gen_zerostate::Cmd),
    GenAccount(gen_account::Cmd),
    Bc(bc::Cmd),
    DumpState(dump_state::Cmd),
}
