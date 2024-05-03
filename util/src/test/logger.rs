#![allow(clippy::exit)]

pub fn init_logger(test_name: &str) {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new("debug"))
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
