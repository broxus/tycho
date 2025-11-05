use std::future::Future;
use std::path::Path;

use tycho_util::sync::CancellationFlag;
use weedb::{MigrationError, Semver, Tables, VersionProvider, WeeDb, WeeDbBuilder};

pub trait NamedTables: Tables + Sized {
    const NAME: &'static str;
}

pub trait WeeDbExt<T: Tables>: Sized {
    fn builder_prepared<P: AsRef<Path>>(path: P, context: T::Context) -> WeeDbBuilder<T>;
}

impl<T: NamedTables + 'static> WeeDbExt<T> for WeeDb<T> {
    fn builder_prepared<P: AsRef<Path>>(
        path: P,
        context: <T as Tables>::Context,
    ) -> WeeDbBuilder<T> {
        WeeDbBuilder::new(path, context).with_name(T::NAME)
    }
}

pub trait ApplyMigrations {
    fn apply_migrations(&self) -> impl Future<Output = Result<(), MigrationError>> + Send;
}

impl<T> ApplyMigrations for WeeDb<T>
where
    T: NamedTables + WithMigrations + 'static,
{
    #[tracing::instrument(skip_all, fields(db = T::NAME))]
    async fn apply_migrations(&self) -> Result<(), MigrationError> {
        let cancelled = CancellationFlag::new();

        tracing::info!("started");
        scopeguard::defer! {
            cancelled.cancel();
        }

        let span = tracing::Span::current();

        let this = self.clone();
        let cancelled = cancelled.clone();
        tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            let guard = scopeguard::guard((), |_| {
                tracing::warn!("cancelled");
            });

            let mut migrations = weedb::Migrations::with_target_version_and_provider(
                T::VERSION,
                T::new_version_provider(),
            );
            T::register_migrations(&mut migrations, cancelled)?;
            this.apply(migrations)?;

            scopeguard::ScopeGuard::into_inner(guard);
            tracing::info!("finished");
            Ok(())
        })
        .await
        .map_err(|e| MigrationError::Custom(e.into()))?
    }
}

pub trait WithMigrations: Tables + Sized {
    const VERSION: Semver;

    type VersionProvider: VersionProvider;

    fn new_version_provider() -> Self::VersionProvider;

    fn register_migrations(
        migrations: &mut Migrations<Self::VersionProvider, Self>,
        cancelled: CancellationFlag,
    ) -> Result<(), MigrationError>;
}

pub type Migrations<P, T> = weedb::Migrations<P, WeeDb<T>>;
