// until implemented:
// https://github.com/tokio-rs/tracing/issues/372
// https://github.com/tokio-rs/tracing/issues/2730
#[macro_export]
macro_rules! dyn_event {
     // Name / target / parent.
    (name: $name:expr, target: $target:expr, parent: $parent:expr, $lvl:ident, $($arg:tt)+) => {
        match $lvl {
            ::tracing::Level::TRACE => ::tracing::event!(name: $name, target: $target, parent: $parent, ::tracing::Level::TRACE, $($arg)+),
            ::tracing::Level::DEBUG => ::tracing::event!(name: $name, target: $target, parent: $parent, ::tracing::Level::DEBUG, $($arg)+),
            ::tracing::Level::INFO => ::tracing::event!(name: $name, target: $target, parent: $parent, ::tracing::Level::INFO, $($arg)+),
            ::tracing::Level::WARN => ::tracing::event!(name: $name, target: $target, parent: $parent, ::tracing::Level::WARN, $($arg)+),
            ::tracing::Level::ERROR => ::tracing::event!(name: $name, target: $target, parent: $parent, ::tracing::Level::ERROR, $($arg)+),
        }
    };
    // Name / target.
    (name: $name:expr, target: $target:expr, $lvl:ident, $($arg:tt)+) => {
        match $lvl {
            ::tracing::Level::TRACE => ::tracing::event!(name: $name, target: $target, ::tracing::Level::TRACE, $($arg)+),
            ::tracing::Level::DEBUG => ::tracing::event!(name: $name, target: $target, ::tracing::Level::DEBUG, $($arg)+),
            ::tracing::Level::INFO => ::tracing::event!(name: $name, target: $target, ::tracing::Level::INFO, $($arg)+),
            ::tracing::Level::WARN => ::tracing::event!(name: $name, target: $target, ::tracing::Level::WARN, $($arg)+),
            ::tracing::Level::ERROR => ::tracing::event!(name: $name, target: $target, ::tracing::Level::ERROR, $($arg)+),
        }
    };
    // Target / parent.
    (target: $target:expr, parent: $parent:expr, $lvl:ident, $($arg:tt)+) => {
        match $lvl {
            ::tracing::Level::TRACE => ::tracing::event!(target: $target, parent: $parent, ::tracing::Level::TRACE, $($arg)+),
            ::tracing::Level::DEBUG => ::tracing::event!(target: $target, parent: $parent, ::tracing::Level::DEBUG, $($arg)+),
            ::tracing::Level::INFO => ::tracing::event!(target: $target, parent: $parent, ::tracing::Level::INFO, $($arg)+),
            ::tracing::Level::WARN => ::tracing::event!(target: $target, parent: $parent, ::tracing::Level::WARN, $($arg)+),
            ::tracing::Level::ERROR => ::tracing::event!(target: $target, parent: $parent, ::tracing::Level::ERROR, $($arg)+),
        }
    };
    // Name / parent.
    // broken in tracing v0.1.40 https://github.com/tokio-rs/tracing/issues/2984
    // (name: $name:expr, parent: $parent:expr, $lvl:ident, $($arg:tt)+) => {
    //     match $lvl {
    //         ::tracing::Level::TRACE => ::tracing::event!(name: $name, parent: $parent, ::tracing::Level::TRACE, $($arg)+),
    //         ::tracing::Level::DEBUG => ::tracing::event!(name: $name, parent: $parent, ::tracing::Level::DEBUG, $($arg)+),
    //         ::tracing::Level::INFO => ::tracing::event!(name: $name, parent: $parent, ::tracing::Level::INFO, $($arg)+),
    //         ::tracing::Level::WARN => ::tracing::event!(name: $name, parent: $parent, ::tracing::Level::WARN, $($arg)+),
    //         ::tracing::Level::ERROR => ::tracing::event!(name: $name, parent: $parent, ::tracing::Level::ERROR, $($arg)+),
    //     }
    // };
    // Name.
    (name: $name:expr, $lvl:ident, $($arg:tt)+) => {
        match $lvl {
            ::tracing::Level::TRACE => ::tracing::event!(name: $name, ::tracing::Level::TRACE, $($arg)+),
            ::tracing::Level::DEBUG => ::tracing::event!(name: $name, ::tracing::Level::DEBUG, $($arg)+),
            ::tracing::Level::INFO => ::tracing::event!(name: $name, ::tracing::Level::INFO, $($arg)+),
            ::tracing::Level::WARN => ::tracing::event!(name: $name, ::tracing::Level::WARN, $($arg)+),
            ::tracing::Level::ERROR => ::tracing::event!(name: $name, ::tracing::Level::ERROR, $($arg)+),
        }
    };
    // Target.
    (target: $target:expr, $lvl:ident, $($arg:tt)+) => {
        match $lvl {
            ::tracing::Level::TRACE => ::tracing::event!(target: $target, ::tracing::Level::TRACE, $($arg)+),
            ::tracing::Level::DEBUG => ::tracing::event!(target: $target, ::tracing::Level::DEBUG, $($arg)+),
            ::tracing::Level::INFO => ::tracing::event!(target: $target, ::tracing::Level::INFO, $($arg)+),
            ::tracing::Level::WARN => ::tracing::event!(target: $target, ::tracing::Level::WARN, $($arg)+),
            ::tracing::Level::ERROR => ::tracing::event!(target: $target, ::tracing::Level::ERROR, $($arg)+),
        }
    };
    // Parent.
    (parent: $parent:expr, $lvl:ident, $($arg:tt)+) => {
        match $lvl {
            ::tracing::Level::TRACE => ::tracing::event!(parent: $parent, ::tracing::Level::TRACE, $($arg)+),
            ::tracing::Level::DEBUG => ::tracing::event!(parent: $parent, ::tracing::Level::DEBUG, $($arg)+),
            ::tracing::Level::INFO => ::tracing::event!(parent: $parent, ::tracing::Level::INFO, $($arg)+),
            ::tracing::Level::WARN => ::tracing::event!(parent: $parent, ::tracing::Level::WARN, $($arg)+),
            ::tracing::Level::ERROR => ::tracing::event!(parent: $parent, ::tracing::Level::ERROR, $($arg)+),
        }
    };
    // ...
    ($lvl:ident, $($arg:tt)+) => {
        match $lvl {
            ::tracing::Level::TRACE => ::tracing::event!(::tracing::Level::TRACE, $($arg)+),
            ::tracing::Level::DEBUG => ::tracing::event!(::tracing::Level::DEBUG, $($arg)+),
            ::tracing::Level::INFO => ::tracing::event!(::tracing::Level::INFO, $($arg)+),
            ::tracing::Level::WARN => ::tracing::event!(::tracing::Level::WARN, $($arg)+),
            ::tracing::Level::ERROR => ::tracing::event!(::tracing::Level::ERROR, $($arg)+),
        }
    };
}

#[cfg(test)]
pub mod test {
    use tycho_util::test::init_logger;

    #[test]
    pub fn check_macro() {
        init_logger("check_macro", "info");
        do_log();
        let _super = tracing::info_span!("super").entered();
        do_log();
    }

    fn do_log() {
        let lvl = tracing::Level::INFO;
        let parent = tracing::info_span!("parent");

        // Name / target / parent.
        dyn_event!(name: "name", target: "target", parent: &parent, lvl, "works");
        dyn_event!(name: "name", target: "target", parent: &parent, lvl, "format {}", "works");
        dyn_event!(name: "name", target: "target", parent: &parent, lvl, "with {field}", field = "field");
        dyn_event!(name: "name", target: "target", parent: &parent, lvl, "with {field1} and {field2}", field1 = "field1", field2 = "field2");

        // Name / target.
        dyn_event!(name: "name", target: "target", lvl, "works");
        dyn_event!(name: "name", target: "target", lvl, "format {}", "works");
        dyn_event!(name: "name", target: "target", lvl, "with {field}", field = "field");
        dyn_event!(name: "name", target: "target", lvl, "with {field1} and {field2}", field1 = "field1", field2 = "field2");

        // Target / parent.
        dyn_event!(target: "target", parent: &parent, lvl, "works");
        dyn_event!(target: "target", parent: &parent, lvl, "format {}", "works");
        dyn_event!(target: "target", parent: &parent, lvl, "with {field}", field = "field");
        dyn_event!(target: "target", parent: &parent, lvl, "with {field1} and {field2}", field1 = "field1", field2 = "field2");

        // Name / parent.
        // dyn_event!(name: "name", parent: &parent, lvl, "works");
        // dyn_event!(name: "name", parent: &parent, lvl, "format {}", "works");
        // dyn_event!(name: "name", parent: &parent, lvl, "with {field}", field = "field");
        // dyn_event!(name: "name", parent: &parent, lvl, "with {field1} and {field2}", field1 = "field1", field2 = "field2");

        // Name.
        dyn_event!(name: "name", lvl, "works");
        dyn_event!(name: "name", lvl, "format {}", "works");
        dyn_event!(name: "name", lvl, "with {field}", field = "field");
        dyn_event!(name: "name", lvl, "with {field1} and {field2}", field1 = "field1", field2 = "field2");

        // Target.
        dyn_event!(target: "target", lvl, "works");
        dyn_event!(target: "target", lvl, "format {}", "works");
        dyn_event!(target: "target", lvl, "with {field}", field = "field");
        dyn_event!(target: "target", lvl, "with {field1} and {field2}", field1 = "field1", field2 = "field2");

        // Parent.
        dyn_event!(parent: &parent, lvl, "works");
        dyn_event!(parent: &parent, lvl, "format {}", "works");
        dyn_event!(parent: &parent, lvl, "with {field}", field = "field");
        dyn_event!(parent: &parent, lvl, "with {field1} and {field2}", field1 = "field1", field2 = "field2");

        // ...
        dyn_event!(lvl, "works");
        dyn_event!(lvl, "format {}", "works");
        dyn_event!(lvl, "with {field}", field = "field");
        dyn_event!(
            lvl,
            "with {field1} and {field2}",
            field1 = "field1",
            field2 = "field2"
        );
    }
}
