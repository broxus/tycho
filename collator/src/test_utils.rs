pub fn try_init_test_tracing(level_filter: tracing_subscriber::filter::LevelFilter) {
    use std::io::IsTerminal;
    tracing_subscriber::fmt()
        .with_ansi(std::io::stdout().is_terminal())
        .with_writer(std::io::stdout)
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(level_filter.into())
                .from_env_lossy(),
        )
        .with_file(true)
        .with_line_number(true)
        .with_target(true)
        //.with_thread_ids(true)
        .compact()
        .try_init()
        .ok();
}
