use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{ArgGroup, Args};
use serde::{Deserialize, Serialize};
use tycho_util::cli;
use tycho_util::config::PartialConfig;
use tycho_util::serde_helpers::{load_json_from_file, save_json_to_file};

use crate::block_strider::ColdBootType;
use crate::blockchain_rpc::NoopBroadcastListener;
use crate::global_config::GlobalConfig;
use crate::node::{NodeBase, NodeBaseConfig, NodeBootArgs, NodeKeys};

/// Run the Tycho node.
#[derive(Args)]
#[clap(group(ArgGroup::new(Self::RUN_GROUP).multiple(true)))]
pub struct CmdRunArgs {
    /// dump the node config template
    #[clap(short, long, conflicts_with = Self::RUN_GROUP)]
    pub init_config: Option<PathBuf>,

    /// include all fields when dumping a node config template
    #[clap(long, short, conflicts_with = Self::RUN_GROUP)]
    pub all: bool,

    /// overwrite the existing config
    #[clap(short, long, conflicts_with = Self::RUN_GROUP)]
    pub force: bool,

    /// path to the node config
    #[clap(short, long, required_unless_present = Self::INIT_CONFIG, group = Self::RUN_GROUP)]
    pub config: Option<PathBuf>,

    /// path to the global config
    #[clap(short, long, required_unless_present = Self::INIT_CONFIG, group = Self::RUN_GROUP)]
    pub global_config: Option<PathBuf>,

    /// path to node keys
    #[clap(short, long, required_unless_present = Self::INIT_CONFIG, group = Self::RUN_GROUP)]
    pub keys: Option<PathBuf>,

    /// path to the logger config
    #[clap(short, long, group = Self::RUN_GROUP)]
    pub logger_config: Option<PathBuf>,

    /// list of zerostate files to import
    #[clap(short = 'z', long, group = Self::RUN_GROUP)]
    pub import_zerostate: Option<Vec<PathBuf>>,

    /// Overwrite cold boot type. Default: `latest-persistent`
    #[clap(short = 'b', long, group = Self::RUN_GROUP)]
    pub cold_boot: Option<ColdBootType>,
}

impl CmdRunArgs {
    pub const INIT_CONFIG: &str = "init_config";
    pub const RUN_GROUP: &str = "_run_args";

    /// Either initialized config or prepares the node to run.
    pub fn init_config_or_run<C>(self) -> Result<CmdRunStatus>
    where
        C: Default + PartialConfig + Serialize,
    {
        let Some(config_path) = &self.init_config else {
            return Ok(CmdRunStatus::Run(CmdRunOnlyArgs {
                config: self.config.context("no config")?,
                global_config: self.global_config.context("no global config")?,
                keys: self.keys.context("no keys")?,
                logger_config: self.logger_config,
                import_zerostate: self.import_zerostate,
                cold_boot: self.cold_boot,
            }));
        };

        if config_path.exists() && !self.force {
            anyhow::bail!("config file already exists, use --force to overwrite");
        }

        let config = C::default();
        if self.all {
            save_json_to_file(config, config_path)?;
        } else {
            save_json_to_file(config.into_partial(), config_path)?;
        }
        Ok(CmdRunStatus::ConfigCreated)
    }

    /// Either initialized config or prepares an environment for the most generic light node.
    pub fn init_config_or_run_light_node<F, Fut, C>(self, f: F) -> Result<()>
    where
        F: FnOnce(LightNodeContext<C>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send,
        C: LightNodeConfig + Send + Sync + 'static,
    {
        match self.init_config_or_run::<C>()? {
            CmdRunStatus::Run(args) => args.run_light_node(f),
            CmdRunStatus::ConfigCreated => Ok(()),
        }
    }
}

/// The outcome of [`CmdRun::init_config_or_run`].
pub enum CmdRunStatus {
    Run(CmdRunOnlyArgs),
    ConfigCreated,
}

/// Run the Tycho node.
#[derive(Args)]
#[group(id = CmdRunArgs::RUN_GROUP, multiple = true)]
pub struct CmdRunOnlyArgs {
    /// path to the node config
    #[clap(short, long)]
    pub config: PathBuf,

    /// path to the global config
    #[clap(short, long)]
    pub global_config: PathBuf,

    /// path to node keys (the file will be created if missing).
    #[clap(short, long)]
    pub keys: PathBuf,

    /// path to the logger config
    #[clap(short, long)]
    pub logger_config: Option<PathBuf>,

    /// list of zerostate files to import
    #[clap(short = 'z', long)]
    pub import_zerostate: Option<Vec<PathBuf>>,

    /// Overwrite cold boot type. Default: `latest-persistent`
    #[clap(short = 'b', long)]
    pub cold_boot: Option<ColdBootType>,
}

impl CmdRunOnlyArgs {
    /// Prepares an environment for the most generic light node.
    pub fn run_light_node<F, Fut, C>(self, f: F) -> Result<()>
    where
        F: FnOnce(LightNodeContext<C>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send,
        C: LightNodeConfig + Send + Sync + 'static,
    {
        let node_config = self.load_config::<C>()?;

        if let Some(logger_config) = node_config.logger() {
            cli::logger::init_logger(logger_config, self.logger_config.clone())?;
            cli::logger::set_abort_with_tracing();
        }

        node_config
            .threads()
            .init_all_and_run(cli::signal::run_or_terminate(async move {
                if let Some(metrics) = node_config.metrics() {
                    tycho_util::cli::metrics::init_metrics(metrics)?;
                }

                // Build node.
                let keys = self.load_keys()?;
                let global_config = self.load_global_config()?;
                let public_addr = node_config.base().resolve_public_ip().await?;

                let node = NodeBase::builder(node_config.base(), &global_config)
                    .init_network(public_addr, &keys.as_secret())?
                    .init_storage()
                    .await?
                    .init_blockchain_rpc(NoopBroadcastListener, NoopBroadcastListener)?
                    .build()?;

                f(LightNodeContext {
                    keys,
                    global_config,
                    node,
                    boot_args: self.make_boot_args(),
                    config: node_config,
                })
                .await
            }))
    }

    /// Tries to load node config.
    pub fn load_config<T>(&self) -> Result<T>
    where
        for<'de> T: Deserialize<'de>,
    {
        load_json_from_file(&self.config).context("failed to load node config")
    }

    /// Tries to load global config.
    pub fn load_global_config(&self) -> Result<GlobalConfig> {
        GlobalConfig::from_file(&self.global_config).context("failed to load global config")
    }

    /// Tries to load node keys.
    ///
    /// Generates and saves new keys if the file doesn't exist.
    pub fn load_keys(&self) -> Result<NodeKeys> {
        NodeKeys::load_or_create(&self.keys)
    }

    /// Creates default [`NodeBootArgs`] using the provided CLI arguments.
    pub fn make_boot_args(&self) -> NodeBootArgs {
        NodeBootArgs {
            boot_type: self.cold_boot.unwrap_or(ColdBootType::LatestPersistent),
            zerostates: self.import_zerostate.clone(),
            queue_state_handler: None,
            ignore_states: false,
        }
    }
}

pub struct LightNodeContext<C> {
    pub keys: NodeKeys,
    pub global_config: GlobalConfig,
    pub node: NodeBase,
    pub boot_args: NodeBootArgs,
    pub config: C,
}

pub trait LightNodeConfig: Default + PartialConfig + Serialize + for<'de> Deserialize<'de> {
    fn base(&self) -> &NodeBaseConfig;
    fn threads(&self) -> &cli::config::ThreadPoolConfig;
    fn metrics(&self) -> Option<&cli::metrics::MetricsConfig>;
    fn logger(&self) -> Option<&cli::logger::LoggerConfig>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_base_args() {
        test_args::<CmdRunArgs>("run", [
            Ok("--init-config config.json"),
            Err("--init-config config.json --config not-expected.json"),
            Err("--config config.json --global-config global-config.json"),
            Ok("--config config.json --global-config global-config.json --keys keys.json"),
            Ok(
                "--config config.json --global-config global-config.json --keys keys.json \
                --cold-boot latest-persistent",
            ),
        ]);
    }

    #[test]
    fn parses_extended_args() {
        #[derive(clap::Parser)]
        struct ExtendedArgs {
            #[clap(flatten)]
            base_args: CmdRunArgs,

            #[clap(
                long,
                required_unless_present = CmdRunArgs::INIT_CONFIG,
                group = CmdRunArgs::RUN_GROUP
            )]
            required: Option<String>,

            #[clap(long, group = CmdRunArgs::RUN_GROUP)]
            not_required_first: bool,
            #[clap(long, group = CmdRunArgs::RUN_GROUP)]
            not_required_second: bool,
        }

        test_args::<ExtendedArgs>("run", [
            Ok("--init-config config.json"),
            Err("--init-config config.json --not-required-first"),
            Err("--config config.json --global-config global-config.json --keys keys.json"),
            Ok(
                "--config config.json --global-config global-config.json --keys keys.json \
                --required testtest",
            ),
            Ok(
                "--config config.json --global-config global-config.json --keys keys.json \
                --required testtest --not-required-first --not-required-second",
            ),
            Err("--not-required-first --not-required-second"),
        ]);
    }

    #[test]
    fn parses_run_only_base_args() {
        test_args::<CmdRunOnlyArgs>("run", [
            Err("--config config.json --global-config global-config.json"),
            Ok("--config config.json --global-config global-config.json --keys keys.json"),
            Ok(
                "--config config.json --global-config global-config.json --keys keys.json \
                --cold-boot latest-persistent",
            ),
        ]);
    }

    #[test]
    fn parses_run_only_extended_args() {
        #[derive(clap::Parser)]
        struct ExtendedArgs {
            #[clap(flatten)]
            base_args: CmdRunOnlyArgs,

            #[clap(long, group = CmdRunArgs::RUN_GROUP)]
            required: String,

            #[clap(long, group = CmdRunArgs::RUN_GROUP)]
            not_required_first: bool,
            #[clap(long, group = CmdRunArgs::RUN_GROUP)]
            not_required_second: bool,
        }

        test_args::<ExtendedArgs>("run", [
            Err("--config config.json --global-config global-config.json --keys keys.json"),
            Ok(
                "--config config.json --global-config global-config.json --keys keys.json \
                --required testtest",
            ),
            Ok(
                "--config config.json --global-config global-config.json --keys keys.json \
                --required testtest --not-required-first --not-required-second",
            ),
            Err("--not-required-first --not-required-second"),
        ]);
    }

    fn test_args<T: clap::Args>(
        command_name: &'static str,
        cases: impl IntoIterator<Item = Result<&'static str, &'static str>>,
    ) {
        let mut command = T::augment_args(clap::Command::new(command_name));
        for case in cases {
            let (should_succeed, args) = match case {
                Ok(args) => (true, args),
                Err(args) => (false, args),
            };
            let res = command.try_get_matches_from_mut(
                std::iter::once(command_name).chain(args.split_whitespace()),
            );
            if should_succeed {
                res.inspect_err(|_| println!("args: {args}")).unwrap();
            } else {
                res.inspect(|_| println!("args: {args}")).unwrap_err();
            }
        }
    }
}
