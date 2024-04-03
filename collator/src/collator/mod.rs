#[allow(clippy::module_inception)]
mod collator;
pub mod collator_processor;
mod do_collate;
mod types;

pub(crate) use collator::*;
