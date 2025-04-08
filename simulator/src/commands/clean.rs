use clap::Parser;

use crate::config::SimulatorConfig;
use crate::helm::{ClusterType, HelmRunner};

#[derive(Parser)]
pub struct CleanCommand {
    #[clap(short, long)]
    pub cluster_type: Option<ClusterType>,
}

impl CleanCommand {
    pub fn run(self, config: &SimulatorConfig) -> bool {
        println!("starting clean");

        let mut is_ok = true;

        match HelmRunner::uninstall(config) {
            Ok(_) => println!("\nOK: nodes stopped"),
            Err(err) => {
                println!("\nWARN during stop node: {err}");
                is_ok = false;
            }
        }

        match std::fs::remove_dir_all(&config.project_root.scratch.dir) {
            Ok(_) => println!("\nOK: scratch dir removed"),
            Err(err) => {
                println!("\nWARN during remove scratch dir: {err}");
                is_ok = false;
            }
        }

        println!("finished clean");

        is_ok
    }
}
