use std::fmt::{Debug, Display, Formatter, Result};

use tycho_network::PeerId;

use crate::models::{DagPoint, Digest, PointId, Signature};
use crate::{LogFlavor, MempoolConfig};

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
        match MempoolConfig::LOG_FLAVOR {
            LogFlavor::Full => write!(f, "{}", self.0),
            LogFlavor::Truncated => write!(f, "{:.4}", self.0),
        }
    }
}

impl AltFormat for Digest {}
impl Display for AltFmt<'_, Digest> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match MempoolConfig::LOG_FLAVOR {
            LogFlavor::Full => write!(f, "{}", self.0),
            LogFlavor::Truncated => write!(f, "{:.4}", self.0),
        }
    }
}

impl AltFormat for Signature {}
impl Display for AltFmt<'_, Signature> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match MempoolConfig::LOG_FLAVOR {
            LogFlavor::Full => write!(f, "{}", self.0),
            LogFlavor::Truncated => write!(f, "{:.4}", self.0),
        }
    }
}

impl AltFormat for PointId {}
impl Debug for AltFmt<'_, PointId> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match MempoolConfig::LOG_FLAVOR {
            LogFlavor::Full => write!(f, "{:?}", self.0),
            LogFlavor::Truncated => write!(
                f,
                "PointId( {:.4} @ {} # {:.4} )",
                self.0.location.author, self.0.location.round.0, self.0.digest
            ),
        }
    }
}

impl AltFormat for DagPoint {}
impl Display for AltFmt<'_, DagPoint> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match AltFormat::unpack(self) {
            DagPoint::Trusted(_) => "Trusted",
            DagPoint::Suspicious(_) => "Suspicious",
            DagPoint::Invalid(_) => "Invalid",
            DagPoint::NotExists(_) => "NotExists",
        })
    }
}