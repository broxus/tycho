use anyhow::Result;
use clap::Parser;

use crate::backend::Helm;
use crate::config::SimulatorConfig;

#[derive(Parser)]
pub struct StartCommand {
    /// Quiet is the opposite to debug output, enabled by default
    #[clap(short, long, action)]
    pub quiet: bool,
}

impl StartCommand {
    pub fn run(self, config: &SimulatorConfig) -> Result<()> {
        Helm::upgrade_install(config, Helm::TYCHO_CHART, self.quiet)?;

        Ok(())
    }
}
