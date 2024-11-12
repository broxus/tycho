use everscale_types::models::GlobalVersion;

#[async_trait::async_trait]
pub trait Collator: Send + Sync + 'static {
    // TODO: Add methods
    //   - restart collator
    //   - clear collator cache
    //   - get running sessions

    async fn get_global_version(&self) -> GlobalVersion;
}
