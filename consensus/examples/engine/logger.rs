#![allow(clippy::exit)]

use tracing_flame::FlameLayer;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::fmt::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

pub fn spans(test_name: &str, filter: &str) {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new(filter).expect("tracing directives"))
        .with_file(false)
        .with_level(true)
        .with_line_number(false)
        .with_span_events(FmtSpan::NONE)
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(true)
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

pub fn flame(test_name: &str) {
    std::fs::remove_dir_all("./.temp").ok();
    std::fs::create_dir_all("./.temp")
        .expect("failed to create temp dir for `tracing-flame` output");
    let (flame_layer, guard) = FlameLayer::with_file("./.temp/tracing.folded").unwrap();

    tracing_subscriber::registry()
        .with(Layer::default())
        .with(flame_layer)
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
