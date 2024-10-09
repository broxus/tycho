use std::collections::VecDeque;
use std::fmt::{Debug, Formatter, Write};

use crate::effects::{AltFmt, AltFormat};
use crate::models::{PointInfo, Round};

pub struct EnqueuedAnchor {
    pub anchor: PointInfo,
    pub proof: PointInfo,
    pub direct_trigger: Option<PointInfo>,
}

#[derive(Default)]
pub struct AnchorChain {
    // from the oldest to the last determined without gaps
    queue: VecDeque<EnqueuedAnchor>,
}

impl AnchorChain {
    pub fn top(&self) -> Option<&EnqueuedAnchor> {
        self.queue.back()
    }

    pub fn top_proof_round(&self) -> Option<Round> {
        Some(self.queue.back()?.proof.round())
    }

    pub fn enqueue(&mut self, last: EnqueuedAnchor) {
        self.queue.push_back(last);
    }

    pub fn next(&mut self) -> Option<EnqueuedAnchor> {
        self.queue.pop_front()
    }

    pub fn undo_next(&mut self, next: EnqueuedAnchor) {
        self.queue.push_front(next);
    }

    pub fn drain_upto(
        &mut self,
        bottom_round: Round,
    ) -> impl DoubleEndedIterator<Item = EnqueuedAnchor> + '_ {
        let outdated = self
            .queue
            .iter()
            .take_while(|e| e.anchor.round() < bottom_round)
            .count();
        self.queue.drain(..outdated)
    }
}

impl AltFormat for EnqueuedAnchor {}
impl Debug for AltFmt<'_, EnqueuedAnchor> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let inner = AltFormat::unpack(self);
        let trigger_id = inner.direct_trigger.as_ref().map(|info| info.id());
        f.debug_struct("CommitStage")
            .field("anchor", &inner.anchor.id().alt())
            .field("proof", &inner.proof.id().alt())
            .field("direct_trigger", &trigger_id.as_ref().map(|id| id.alt()))
            .finish()
    }
}

impl AltFormat for AnchorChain {}
impl Debug for AltFmt<'_, AnchorChain> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let inner = AltFormat::unpack(self);
        f.write_str(", chain:[ ")?;
        for el in &inner.queue {
            f.write_str("{ ")?;
            write!(
                f,
                "anchor {} @ {} # {},",
                el.anchor.data().author.alt(),
                el.anchor.round().0,
                el.anchor.digest().alt()
            )?;
            f.write_char(' ')?;
            write!(
                f,
                "proof {} @ {} # {},",
                el.proof.data().author.alt(),
                el.proof.round().0,
                el.proof.digest().alt()
            )?;
            f.write_char(' ')?;
            match &el.direct_trigger {
                None => write!(f, "trigger None")?,
                Some(tr) => write!(
                    f,
                    "trigger {} @ {} # {}",
                    tr.data().author.alt(),
                    tr.round().0,
                    tr.digest().alt()
                )?,
            }
            f.write_str(" }, ")?;
        }
        f.write_str("]")
    }
}
