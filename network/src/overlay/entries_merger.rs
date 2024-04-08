use crate::dht::{DhtValueMerger, DhtValueSource, StorageError};
use crate::proto::dht::{MergedValue, MergedValueRef};

/// Allows only local values to be stored.
/// Always overwrites the stored value with the new value.
#[derive(Debug, Default, Clone, Copy)]
pub struct PublicOverlayEntriesMerger;

impl DhtValueMerger for PublicOverlayEntriesMerger {
    fn check_value(
        &self,
        source: DhtValueSource,
        _: &MergedValueRef<'_>,
    ) -> Result<(), StorageError> {
        if source != DhtValueSource::Local {
            return Err(StorageError::InvalidSource);
        }

        Ok(())
    }

    fn merge_value(
        &self,
        source: DhtValueSource,
        new: &MergedValueRef<'_>,
        stored: &mut MergedValue,
    ) -> bool {
        if source != DhtValueSource::Local {
            return false;
        }

        *stored = new.as_owned();
        true
    }
}
