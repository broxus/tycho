use std::fmt::{Debug, Formatter};

use tycho_network::PeerId;

use crate::models::{DagPoint, Location, Point, PointId};

pub struct UglyPrint<'a, T>(pub &'a T);

pub trait Ugly {
    fn ugly(&self) -> UglyPrint<'_, Self>
    where
        Self: Sized;
}

impl<T> Ugly for T
where
    T: Sized,
    for<'a> UglyPrint<'a, T>: Debug,
{
    fn ugly(&self) -> UglyPrint<'_, T> {
        UglyPrint(self)
    }
}

impl Debug for UglyPrint<'_, PeerId> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:.4?}", self.0)
    }
}

impl Debug for UglyPrint<'_, Location> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:.4?} @ {:?}", self.0.author, self.0.round.0)
    }
}

impl Debug for UglyPrint<'_, PointId> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PointId( {:.4} @ {} # {:.4} )",
            self.0.location.author, self.0.location.round.0, self.0.digest
        )
    }
}

impl Debug for UglyPrint<'_, Point> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Point {{ Id( {:.4} @ {} # {:.4} ), .. }}",
            self.0.body.location.author, self.0.body.location.round.0, self.0.digest
        )
    }
}

impl Debug for UglyPrint<'_, DagPoint> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            DagPoint::Trusted(_) => f.write_str("Trusted(")?,
            DagPoint::Suspicious(_) => f.write_str("Suspicious(")?,
            DagPoint::Invalid(_) => f.write_str("Invalid(")?,
            DagPoint::NotExists(_) => f.write_str("NotExists(")?,
        };
        write!(
            f,
            "Point {{ Id( {:.4} @ {} # {:.4} ), .. }}",
            self.0.location().author,
            self.0.location().round.0,
            self.0.digest()
        )?;
        f.write_str(")")
    }
}
