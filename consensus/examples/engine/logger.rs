#![allow(clippy::exit)]
#![allow(dead_code)]

use tracing_flame::FlameLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

pub fn spans(test_name: &str, filter: &str) {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new(filter).expect("tracing directives"))
        .with_thread_names(true)
        .with_target(false)
        .with_file(false)
        .with_line_number(false)
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

pub fn flame(test_name: &str, filter: &str) {
    let filter_layer = EnvFilter::try_new(filter).expect("tracing directives");
    let fmt_layer = tracing_subscriber::fmt::layer().with_target(false);

    std::fs::create_dir_all("./.temp")
        .expect("failed to create temp dir for `tracing-flame` output");
    let (flame_layer, guard) = FlameLayer::with_file("./.temp/tracing.folded").unwrap();

    tracing_subscriber::registry()
        .with(flame_layer)
        .with(filter_layer)
        .with(fmt_layer)
        .init();

    tracing::info!("{test_name}");

    std::panic::set_hook(Box::new(move |info| {
        use std::io::Write;
        let backtrace = std::backtrace::Backtrace::capture();

        if let Err(err) = guard.flush() {
            tracing::error!("flame layer not flushed: {err}");
        }
        tracing::error!("{info}\n{backtrace}");
        std::io::stderr().flush().ok();
        std::io::stdout().flush().ok();
        std::process::exit(1);
    }));
}
