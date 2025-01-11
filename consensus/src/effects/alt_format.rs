use std::fmt::{Debug, Display, Formatter, Result};

use tycho_network::PeerId;

use crate::engine::NodeConfig;
use crate::models::{Digest, PointId, Signature};

/// Display implementations to be used as fields in structured logs,
/// while Debug is a temporary convenience
pub struct AltFmt<'a, T: ?Sized>(&'a T);

pub trait AltFormat {
    fn alt(&self) -> AltFmt<'_, Self> {
        AltFmt(self)
    }
    // it must be not handy to access private inner type, thus not a method
    fn unpack<'a>(packed: &'a AltFmt<'a, Self>) -> &'a Self {
        packed.0
    }
}

impl AltFormat for PeerId {}
impl Display for AltFmt<'_, PeerId> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match NodeConfig::get().log_truncate_long_values {
            false => write!(f, "{}", self.0),
            true => write!(f, "{:.4}", self.0),
        }
    }
}

impl AltFormat for [PeerId] {}
impl Display for AltFmt<'_, [PeerId]> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str("[")?;
        if let Some((last, others)) = self.0.split_last() {
            for el in others {
                write!(f, " {},", &el.alt())?;
            }
            write!(f, " {} ", &last.alt())?;
        }
        f.write_str("]")
    }
}

impl AltFormat for Digest {}
impl Display for AltFmt<'_, Digest> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match NodeConfig::get().log_truncate_long_values {
            false => write!(f, "{}", self.0),
            true => write!(f, "{:.4}", self.0),
        }
    }
}

impl AltFormat for Signature {}
impl Display for AltFmt<'_, Signature> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match NodeConfig::get().log_truncate_long_values {
            false => write!(f, "{}", self.0),
            true => write!(f, "{:.4}", self.0),
        }
    }
}

impl AltFormat for PointId {}
impl Debug for AltFmt<'_, PointId> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match NodeConfig::get().log_truncate_long_values {
            false => write!(f, "{:?}", self.0),
            true => write!(
                f,
                "PointId( {:.4} @ {} # {:.4} )",
                self.0.author, self.0.round.0, self.0.digest
            ),
        }
    }
}
