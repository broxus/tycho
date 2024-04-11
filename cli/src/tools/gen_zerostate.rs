use std::path::PathBuf;

use anyhow::Result;

/// Generate a zero state for a network.
#[derive(clap::Parser)]
pub struct Cmd {
    /// dump the template of the zero state config
    #[clap(short = 'i', long, exclusive = true)]
    init_config: Option<String>,

    /// path to the zero state config
    #[clap(required_unless_present = "init_config")]
    config: Option<PathBuf>,

    /// path to the output file
    #[clap(short, long)]
    output: PathBuf,

    /// explicit unix timestamp of the zero state
    #[clap(long)]
    now: Option<u32>,
}

impl Cmd {
    pub fn run(self) -> Result<()> {
        // todo
        Ok(())
    }
}
