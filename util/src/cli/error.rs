pub trait ResultExt<T> {
    fn wrap_err(self, context: impl Into<String>) -> anyhow::Result<T>;
}

impl<T, E: Into<anyhow::Error>> ResultExt<T> for Result<T, E> {
    fn wrap_err(self, context: impl Into<String>) -> anyhow::Result<T> {
        self.map_err(|e| {
            ErrorWithContext {
                context: context.into(),
                source: e.into(),
            }
            .into()
        })
    }
}

#[derive(thiserror::Error, Debug)]
#[error("{context}: {source}")]
pub struct ErrorWithContext {
    context: String,
    #[source]
    source: anyhow::Error,
}
