pub use tycho_util_proc::PartialConfig;

/// Allows defining a subset of preserved fields in a config.
///
/// ---
///
/// By default all fields are "non-important", e.g.:
/// ```
/// # use tycho_util::config::PartialConfig;
/// #[derive(Default, PartialConfig)]
/// struct EmptyConfig {
///     field1: usize,
///     field2: usize,
/// }
/// ```
/// will generate an empty config:
/// ```
/// struct PartialEmptyConfig {}
/// ```
///
/// ---
///
/// Use `#[important]` to include some fields as is, e.g.:
/// ```
/// # use tycho_util::config::PartialConfig;
/// #[derive(Default, PartialConfig)]
/// struct SimpleConfig {
///     field1: usize,
///     #[important]
///     field2: usize,
///     #[important]
///     field3: usize,
/// }
/// ```
/// will generate a config with marked fields:
/// ```
/// struct PartialSimpleConfig {
///     field2: usize,
///     field3: usize,
/// }
/// ```
///
/// ---
///
/// Partial configs can be nested, e.g.:
/// ```
/// # use tycho_util::config::PartialConfig;
/// #[derive(Default, PartialConfig)]
/// struct RootConfig {
///     field1: usize,
///     #[important]
///     field2: usize,
///     #[partial]
///     field3: ChildConfig,
/// }
///
/// #[derive(Default, PartialConfig)]
/// struct ChildConfig {
///     field1: usize,
///     #[important]
///     field2: usize,
/// }
/// ```
/// will generate a partial config using new types:
/// ```
/// # use tycho_util::config::PartialConfig;
/// #[derive(Default, PartialConfig)]
/// # struct ChildConfig {}
/// struct PartialRootConfig {
///     field2: usize,
///     field3: <ChildConfig as PartialConfig>::Partial,
/// }
///
/// struct PartialChildConfig {
///     field2: usize,
/// }
/// ```
pub trait PartialConfig: Sized {
    type Partial: serde::Serialize;

    fn into_partial(self) -> Self::Partial;
}

impl<T: PartialConfig> PartialConfig for Option<T> {
    type Partial = Option<T::Partial>;

    #[inline]
    fn into_partial(self) -> Self::Partial {
        self.map(T::into_partial)
    }
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use super::*;

    #[derive(Clone, Copy, PartialConfig, Serialize, PartialEq, Eq)]
    struct RootConfig {
        #[important]
        important_field: usize,
        default_field: usize,
        #[partial]
        subconfig: SubConfig,
    }

    impl Default for RootConfig {
        fn default() -> Self {
            Self {
                important_field: 123,
                default_field: 456,
                subconfig: Default::default(),
            }
        }
    }

    #[derive(Clone, Copy, PartialConfig, Serialize, PartialEq, Eq)]
    struct SubConfig {
        #[important]
        important_field2: usize,
        default_field2: usize,
    }

    impl Default for SubConfig {
        fn default() -> Self {
            Self {
                important_field2: 789,
                default_field2: 12,
            }
        }
    }

    #[test]
    fn partial_config_works() {
        let root_config = RootConfig::default();

        // Compiler assert on fields
        #[allow(unused)]
        let RootConfigPartial {
            important_field,
            subconfig: SubConfigPartial { important_field2 },
        } = root_config.into_partial();

        let json = serde_json::to_string(&root_config.into_partial()).unwrap();

        // NOTE: `serde_json` is used with `preserve_order` feature in dev dependencies.
        assert_eq!(
            json,
            r#"{"important_field":123,"subconfig":{"important_field2":789}}"#
        );
    }
}
