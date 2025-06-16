use std::future::Future;
use std::path::Path;

use tycho_util::sync::CancellationFlag;
use weedb::{MigrationError, Semver, Tables, VersionProvider, WeeDb, WeeDbBuilder, WeeDbRaw};

pub mod refcount;
pub mod tables;

pub trait WeeDbExt<T: Tables>: Sized {
    fn builder_prepared<P: AsRef<Path>>(path: P, context: T::Context) -> WeeDbBuilder<T>;

    fn apply_migrations(&self) -> impl Future<Output = Result<(), MigrationError>> + Send;
}

impl<T: Tables + 'static> WeeDbExt<T> for WeeDb<T>
where
    T: WithMigrations + 'static,
{
    fn builder_prepared<P: AsRef<Path>>(
        path: P,
        context: <T as Tables>::Context,
    ) -> WeeDbBuilder<T> {
        WeeDbBuilder::new(path, context).with_name(T::NAME)
    }

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
                StateVersionProvider { db_name: T::NAME },
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

// === Migrations stuff ===

pub trait WithMigrations: Tables + Sized {
    const NAME: &'static str;
    const VERSION: Semver;

    fn register_migrations(
        migrations: &mut Migrations<Self>,
        cancelled: CancellationFlag,
    ) -> Result<(), MigrationError>;
}

pub type Migrations<T> = weedb::Migrations<StateVersionProvider, WeeDb<T>>;

pub struct StateVersionProvider {
    pub db_name: &'static str,
}

impl StateVersionProvider {
    const DB_NAME_KEY: &'static [u8] = b"__db_name";
    const DB_VERSION_KEY: &'static [u8] = b"__db_version";
}

impl VersionProvider for StateVersionProvider {
    fn get_version(&self, db: &WeeDbRaw) -> Result<Option<Semver>, MigrationError> {
        let state = db.instantiate_table::<tables::State>();

        if let Some(db_name) = state.get(Self::DB_NAME_KEY)? {
            if db_name.as_ref() != self.db_name.as_bytes() {
                return Err(MigrationError::Custom(
                    format!(
                        "expected db name: {}, got: {}",
                        self.db_name,
                        String::from_utf8_lossy(db_name.as_ref())
                    )
                    .into(),
                ));
            }
        }

        let value = state.get(Self::DB_VERSION_KEY)?;
        match value {
            Some(version) => {
                let slice = version.as_ref();
                slice
                    .try_into()
                    .map_err(|_e| MigrationError::InvalidDbVersion)
                    .map(Some)
            }
            None => Ok(None),
        }
    }

    fn set_version(&self, db: &WeeDbRaw, version: Semver) -> Result<(), MigrationError> {
        let state = db.instantiate_table::<tables::State>();

        state.insert(Self::DB_NAME_KEY, self.db_name.as_bytes())?;
        state.insert(Self::DB_VERSION_KEY, version)?;
        Ok(())
    }
}
