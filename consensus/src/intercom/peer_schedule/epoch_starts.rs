use std::fmt::{Debug, Formatter};

use crate::models::Round;

#[derive(Clone)]
pub struct EpochStarts {
    started: [Round; 3],
    pub next: Option<Round>,
}

impl Default for EpochStarts {
    fn default() -> Self {
        Self {
            started: [Round::BOTTOM, Round::BOTTOM, Round::BOTTOM],
            next: None,
        }
    }
}

impl EpochStarts {
    pub fn oldest(&self) -> Round {
        self.started[0]
    }

    pub fn prev(&self) -> Round {
        self.started[1]
    }

    pub fn curr(&self) -> Round {
        self.started[2]
    }

    pub fn arr_idx(&self, round: Round) -> Option<usize> {
        if self.next.is_some_and(|r| round >= r) {
            Some(3)
        } else if round >= self.started[2] {
            Some(2)
        } else if round >= self.started[1] {
            Some(1)
        } else if round >= self.started[0] {
            Some(0)
        } else {
            None
        }
    }

    pub fn rotate(&mut self) {
        // make next from oldest
        let next = (self.next)
            .ok_or_else(|| format!("{self:?}"))
            .expect("attempt to change epoch, but next epoch start is not set");

        assert!(next > self.curr(), "next start is not in future {self:?}");

        self.next = None; // makes next epoch peers inaccessible for reads
        self.started[0] = next;
        self.started.rotate_left(1);
    }
}

impl Debug for EpochStarts {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EpochStarts")
            .field("oldest", &self.oldest().0)
            .field("prev", &self.prev().0)
            .field("curr", &self.curr().0)
            .field("next", &self.next.map(|r| r.0))
            .finish()
    }
}
