use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use everscale_types::abi::{AbiValue, AbiVersion, FromAbi, WithAbiType};
use everscale_types::models::{AccountState, StdAddr};
use everscale_types::prelude::*;

use crate::util::{elector, print_json};
use crate::{BaseArgs, ControlArgs};

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
    // TODO: Add support for using a public RPC here.
    fn run(self, args: BaseArgs) -> Result<()> {
        self.control.rt(args, move |client| async move {
            let res = client.get_account_state(&ELECTOR_ADDR).await?.parse()?;
            let account_state = res
                .state
                .load_account()?
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
