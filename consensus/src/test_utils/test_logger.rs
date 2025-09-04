use std::io::Write;
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;
use tracing_subscriber::layer::SubscriberExt;

pub fn spans(test_name: &str, filter: &str) {
    use tracing_subscriber::Layer;

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::NONE)
        .event_format(
            tracing_subscriber::fmt::format()
                .with_file(false)
                .with_level(true)
                .with_line_number(false)
                .with_target(false)
                .with_thread_ids(false)
                .with_thread_names(true),
        )
        .with_filter(tracing_subscriber::EnvFilter::try_new(filter).expect("tracing directives"));

    let subscriber = tracing_subscriber::registry().with(fmt_layer);

    tracing::subscriber::set_global_default(subscriber).ok();

    tracing::info!("{test_name}");

    set_print_panic_hook(false);
}
static FIRST_PANIC: OnceLock<()> = OnceLock::new();

pub fn set_print_panic_hook(with_exit: bool) {
    std::panic::set_hook(Box::new(move |info| {
        if FIRST_PANIC.set(()).is_ok() {
            let backtrace = std::backtrace::Backtrace::force_capture();
            tracing::error!("root panic: {info}\n{backtrace}");
            if with_exit {
                std::io::stderr().flush().ok();
                std::io::stdout().flush().ok();
                #[allow(clippy::exit, reason = "requires 'test' feature")]
                std::process::exit(1);
            }
        }
        tracing::error!("induced panic: {info}");
        // flush at the end of main thread, after all threads are joined
    }));
}

pub fn flame(test_name: &str) {
    std::fs::remove_dir_all("./.temp").ok();
    std::fs::create_dir_all("./.temp")
        .expect("failed to create temp dir for `tracing-flame` output");
    let (flame_layer, flame_guard) =
        tracing_flame::FlameLayer::with_file("./.temp/tracing.folded").unwrap();

    use tracing_subscriber::util::SubscriberInitExt;

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::Layer::default())
        .with(flame_layer)
        .init();

    tracing::info!("{test_name}");

    let first_panic = Arc::new(Mutex::new(true));

    std::panic::set_hook(Box::new(move |info| {
        let mut guard = first_panic.lock();
        if *guard {
            let backtrace = std::backtrace::Backtrace::force_capture();
            tracing::error!("root panic: {info}\n{backtrace}");
            if let Err(err) = flame_guard.flush() {
                tracing::error!("flame layer not flushed: {err}");
            }
            *guard = false;
        }
        drop(guard);
        tracing::error!("induced panic: {info}");
        // flush at the end of main thread, after all threads are joined
    }));
}
