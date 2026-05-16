use anyhow::{Result, bail};
use futures_util::FutureExt;
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

    pub(super) fn set_collator_and_state<SetState>(
        &self,
        collator: Box<CF::Collator>,
        set_state: SetState,
    ) -> Result<CollatorState>
    where
        SetState: FnOnce(&mut ActiveCollator<Box<CF::Collator>>),
    {
        match self.active_collators.get_mut(collator.shard_id()) {
            Some(mut active_collator) => {
                active_collator.collator = Some(collator);
                set_state(&mut active_collator);
                Ok(active_collator.state)
            }
            None => bail!(
                "active_collators does not contain collator state for {}",
                collator.shard_id(),
            ),
        }
    }

    pub(super) fn take_collator_and_set_state_if<Check, SetState>(
        &self,
        shard_id: &ShardIdent,
        check: Check,
        set_state: SetState,
    ) -> Result<Option<Box<CF::Collator>>>
    where
        Check: FnOnce(&ActiveCollator<Box<CF::Collator>>) -> bool,
        SetState: FnOnce(&mut ActiveCollator<Box<CF::Collator>>),
    {
        match self.active_collators.get_mut(shard_id) {
            Some(mut active_collator) => {
                if !check(&active_collator) {
                    return Ok(None);
                }

                let collator = match active_collator.collator.take() {
                    Some(collator) => Ok(collator),
                    None => bail!("collator for {} is already extracted", shard_id),
                };

                set_state(&mut active_collator);

                // drain a stale cancel permit before starting another collation attempt
                let _ = active_collator.cancel_collation.notified().now_or_never();

                Some(collator).transpose()
            }
            None => bail!("collator for {} not started", shard_id),
        }
    }

    pub(super) fn set_collator_state<SetState>(
        &self,
        shard_id: &ShardIdent,
        set_state: SetState,
    ) -> Option<CollatorState>
    where
        SetState: FnOnce(&mut ActiveCollator<Box<CF::Collator>>),
    {
        match self.active_collators.get_mut(shard_id) {
            Some(mut active_collator) => {
                set_state(&mut active_collator);
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

    pub(super) fn get_collator_state(&self, shard_id: &ShardIdent) -> Option<CollatorState> {
        self.active_collators.get(shard_id).map(|ac| ac.state)
    }

    pub(super) fn request_cancel_collations(&self) -> bool {
        let mut has_active = false;
        for mut active_collator in self.active_collators.iter_mut().filter(|ac| {
            ac.state == CollatorState::Active || ac.state == CollatorState::CancelPending
        }) {
            active_collator.state = CollatorState::CancelPending;
            active_collator.cancel_collation.notify_one();
            has_active = true;
        }
        has_active
    }

    pub(super) fn has_active_collations(&self) -> bool {
        self.active_collators
            .iter()
            .any(|ac| ac.state == CollatorState::Active || ac.state == CollatorState::CancelPending)
    }
}
