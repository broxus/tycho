#![allow(clippy::exit)]

use tracing_subscriber::EnvFilter;

pub fn init_logger(test_name: &str, filter: &str) {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new(filter).expect("tracing directives"))
        .try_init()
        .ok();

    tracing::info!("{test_name}");

    std::panic::set_hook(Box::new(|info| {
        use std::io::Write;

        tracing::error!("{}", info);
        std::io::stderr().flush().ok();
        std::io::stdout().flush().ok();
        std::process::exit(1);
    }));
}
