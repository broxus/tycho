use anyhow::Context;
use clap::Parser;
use tycho_core::block_strider::PrintSubscriber;
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

    let config: Config =
        tycho_light_node::NodeConfig::from_file(args.node.config.as_ref().context("no config")?)?;

    args.node.run(config, PrintSubscriber).await?;

    Ok(tokio::signal::ctrl_c().await?)
}
