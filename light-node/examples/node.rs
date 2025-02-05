use anyhow::Context;
use clap::Parser;
use tycho_core::block_strider::{
    ArchiveBlockProvider, BlockProviderExt, BlockchainBlockProvider, ColdBootType, PrintSubscriber,
    StorageBlockProvider,
};
use tycho_light_node::CmdRun;

type Config = tycho_light_node::NodeConfig<()>;

#[derive(Parser)]
struct TestArgs {
    #[clap(flatten)]
    node: CmdRun,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    std::panic::set_hook(Box::new(|info| {
        use std::io::Write;
        let backtrace = std::backtrace::Backtrace::capture();

        tracing::error!("{info}\n{backtrace}");
        std::io::stderr().flush().ok();
        std::io::stdout().flush().ok();
        std::process::exit(1);
    }));

    let args = TestArgs::parse();

    let import_zerostate = args.node.import_zerostate.clone();

    let config: Config =
        tycho_light_node::NodeConfig::from_file(args.node.config.as_ref().context("no config")?)?;

    let mut node = args.node.create(config.clone()).await?;

    let archive_block_provider = ArchiveBlockProvider::new(
        node.blockchain_rpc_client().clone(),
        node.storage().clone(),
        config.archive_block_provider.clone(),
    );

    let storage_block_provider = StorageBlockProvider::new(node.storage().clone());

    let blockchain_block_provider = BlockchainBlockProvider::new(
        node.blockchain_rpc_client().clone(),
        node.storage().clone(),
        config.blockchain_block_provider.clone(),
    )
    .with_fallback(archive_block_provider.clone());

    let init_block_id = node
        .init(ColdBootType::LatestPersistent, import_zerostate)
        .await?;
    node.update_validator_set(&init_block_id).await?;
    node.run(
        archive_block_provider.chain((blockchain_block_provider, storage_block_provider)),
        PrintSubscriber,
    )
    .await?;

    Ok(tokio::signal::ctrl_c().await?)
}
