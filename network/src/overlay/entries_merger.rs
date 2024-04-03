use crate::dht::{DhtValueMerger, DhtValueSource, StorageError};
use crate::proto::dht::{MergedValue, MergedValueRef};

pub struct PublicOverlayEntriesMerger {}

impl DhtValueMerger for PublicOverlayEntriesMerger {
    fn check_value(
        &self,
        source: DhtValueSource,
        new: &MergedValueRef<'_>,
    ) -> Result<(), StorageError> {
        if source != DhtValueSource::Remote {
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
        todo!()
    }
}
