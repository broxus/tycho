#![allow(clippy::exit)]

use std::borrow::Cow;

use tracing_subscriber::EnvFilter;

pub fn init_logger(test_name: &str, filter: &str) {
    let mut filter = Cow::Borrowed(filter);
    if let Ok(env) = std::env::var(EnvFilter::DEFAULT_ENV) {
        filter = Cow::Owned(env);
    }

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new(filter).expect("tracing directives"))
        .try_init()
        .ok();

    tracing::info!("{test_name}");

    std::panic::set_hook(Box::new(|info| {
        use std::io::Write;
        let backtrace = std::backtrace::Backtrace::capture();

        tracing::error!("{info}\n{backtrace}");
        std::io::stderr().flush().ok();
        std::io::stdout().flush().ok();
        std::process::exit(1);
    }));
}
