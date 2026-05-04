use anyhow::{Result, bail};
use tycho_types::models::ShardIdent;

use crate::collator::{Collator, CollatorFactory};
use crate::manager::{ActiveCollator, CollationManager, CollatorState};
use crate::validator::Validator;

impl<CF, V> CollationManager<CF, V>
where
    CF: CollatorFactory,
    V: Validator,
{
    pub(super) fn set_collator(&self, collator: Box<CF::Collator>) -> Result<CollatorState> {
        match self.active_collators.get_mut(collator.shard_id()) {
            Some(mut active_collator) => {
                active_collator.collator = Some(collator);
                Ok(active_collator.state)
            }
            None => bail!(
                "active_collators does not contain collator state for {}",
                collator.shard_id(),
            ),
        }
    }

    pub(super) fn take_collator(&self, shard_id: &ShardIdent) -> Result<Box<CF::Collator>> {
        self.take_collator_and_set_state(shard_id, |_| {})
    }

    pub(super) fn take_collator_and_set_state<F>(
        &self,
        shard_id: &ShardIdent,
        f: F,
    ) -> Result<Box<CF::Collator>>
    where
        F: Fn(&mut ActiveCollator<Box<CF::Collator>>),
    {
        match self.active_collators.get_mut(shard_id) {
            Some(mut active_collator) => {
                let collator = match active_collator.collator.take() {
                    Some(collator) => Ok(collator),
                    None => bail!("collator for {} is already extracted", shard_id),
                };
                f(&mut active_collator);
                collator
            }
            None => bail!("collator for {} not started", shard_id),
        }
    }

    pub(super) fn set_collator_state<F>(&self, shard_id: &ShardIdent, f: F) -> Option<CollatorState>
    where
        F: Fn(&mut ActiveCollator<Box<CF::Collator>>),
    {
        match self.active_collators.get_mut(shard_id) {
            Some(mut active_collator) => {
                f(&mut active_collator);
                Some(active_collator.state)
            }
            None => None,
        }
    }

    pub(super) fn set_collators_state<Filter, F>(&self, mut filter: Filter, f: F)
    where
        Filter: FnMut(&ShardIdent, &ActiveCollator<Box<CF::Collator>>) -> bool,
        F: Fn(&mut ActiveCollator<Box<CF::Collator>>),
    {
        for mut active_collator in self.active_collators.iter_mut() {
            if filter(active_collator.key(), active_collator.value()) {
                f(&mut active_collator);
            }
        }
    }

    pub(super) fn request_cancel_collations(&self) -> bool {
        let mut has_active = false;
        for active_collator in self.active_collators.iter().filter(|ac| {
            ac.state == CollatorState::Active || ac.state == CollatorState::CancelPending
        }) {
            active_collator.cancel_collation.notify_one();
            has_active = true;
        }
        has_active
    }

    pub(super) fn has_active_collator(&self) -> bool {
        self.active_collators
            .iter()
            .any(|ac| ac.state == CollatorState::Active || ac.state == CollatorState::CancelPending)
    }
}
