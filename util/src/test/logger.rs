pub fn init_logger(test_name: &str) {
    tracing::info!("{test_name}");

    std::panic::set_hook(Box::new(|info| {
        use std::io::Write;

        tracing::error!("{}", info);
        std::io::stderr().flush().ok();
        std::io::stdout().flush().ok();
        std::process::exit(1);
    }));
}
