use std::marker::PhantomData;

use weedb::{ColumnFamily, MigrationError, Semver, VersionProvider, WeeDbRaw};

use super::NamedTables;

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
#[repr(transparent)]
pub struct InstanceId(pub [u8; 16]);

impl InstanceId {
    #[inline]
    pub fn from_slice(slice: &[u8]) -> Self {
        Self(slice.try_into().expect("slice with incorrect length"))
    }
}

impl rand::distr::Distribution<InstanceId> for rand::distr::StandardUniform {
    #[inline]
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> InstanceId {
        InstanceId(rng.random())
    }
}

impl AsRef<[u8]> for InstanceId {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

pub struct StateVersionProvider<C> {
    db_name: &'static str,
    cf: PhantomData<C>,
}

impl<C> StateVersionProvider<C> {
    pub const DB_NAME_KEY: &'static [u8] = b"__db_name";
    pub const DB_VERSION_KEY: &'static [u8] = b"__db_version";

    pub fn new<T: NamedTables>() -> Self
    where
        C: ColumnFamily,
    {
        Self {
            db_name: T::NAME,
            cf: PhantomData,
        }
    }
}

impl<C: ColumnFamily> VersionProvider for StateVersionProvider<C> {
    fn get_version(&self, db: &WeeDbRaw) -> Result<Option<Semver>, MigrationError> {
        let state = db.instantiate_table::<C>();

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
        let state = db.instantiate_table::<C>();
        state.insert(Self::DB_NAME_KEY, self.db_name.as_bytes())?;
        state.insert(Self::DB_VERSION_KEY, version)?;
        Ok(())
    }
}
