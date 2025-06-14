use clap::Parser;

use crate::backend::Helm;
use crate::config::SimulatorConfig;

#[derive(Parser)]
pub struct CleanCommand;

impl CleanCommand {
    pub fn run(config: &SimulatorConfig) -> bool {
        println!("starting clean");

        let mut is_ok = true;

        match Helm::uninstall(config) {
            Ok(_) => println!("\nOK: nodes stopped"),
            Err(err) => {
                println!("\nWARN during stop node: {err}");
                is_ok = false;
            }
        }

        let tycho_values = &config.project_root.simulator.helm.tycho.values;
        match std::fs::exists(tycho_values) {
            Ok(false) => {}
            Ok(true) => match std::fs::remove_file(tycho_values) {
                Ok(_) => println!("\nOK: removed `tycho` chart values file"),
                Err(err) => {
                    println!("\nWARN during remove `tycho` chart values file: {err}");
                    is_ok = false;
                }
            },
            Err(err) => {
                println!("\nERR cannot access `tycho` chart values file: {err}");
                is_ok = false;
            }
        }

        println!("finished clean");

        is_ok
    }
}
