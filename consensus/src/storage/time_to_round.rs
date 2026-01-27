use tl_proto::{TlPacket, TlRead, TlResult, TlWrite};

use crate::models::{Round, UnixTime};

#[cfg_attr(any(test, feature = "test"), derive(PartialEq))]
pub struct TimeToRound {
    pub time: UnixTime,
    pub round: Round,
}

impl TimeToRound {
    pub const MAX_TL_BYTES: usize = UnixTime::MAX_TL_BYTES + Round::MAX_TL_SIZE;

    pub fn new(time: UnixTime, round: Round) -> Self {
        Self { time, round }
    }

    pub fn fill(&self, buf: &mut [u8; Self::MAX_TL_BYTES]) {
        buf[..UnixTime::MAX_TL_BYTES].copy_from_slice(&self.time.millis().to_be_bytes());
        buf[UnixTime::MAX_TL_BYTES..].copy_from_slice(&self.round.0.to_be_bytes());
    }

    pub fn read_round(bytes: &[u8]) -> Option<Round> {
        if bytes.len() != Self::MAX_TL_BYTES {
            return None;
        }
        let mut round = [0; _];
        round.copy_from_slice(&bytes[UnixTime::MAX_TL_BYTES..]);
        Some(Round(u32::from_be_bytes(round)))
    }
}

impl TlWrite for TimeToRound {
    type Repr = tl_proto::Bare;

    fn max_size_hint(&self) -> usize {
        Self::MAX_TL_BYTES
    }

    fn write_to<P>(&self, packet: &mut P)
    where
        P: TlPacket,
    {
        self.time.millis().to_be_bytes().write_to(packet);
        self.round.0.to_be_bytes().write_to(packet);
    }
}

impl<'tl> TlRead<'tl> for TimeToRound {
    type Repr = tl_proto::Bare;

    fn read_from(packet: &mut &'tl [u8]) -> TlResult<Self> {
        Ok(Self {
            time: UnixTime::from_millis(u64::from_be_bytes(<_>::read_from(packet)?)),
            round: Round(u32::from_be_bytes(<_>::read_from(packet)?)),
        })
    }
}

#[cfg(test)]
mod test {
    use anyhow::{Context, Result};

    use super::*;

    #[test]
    fn test() -> Result<()> {
        let key = TimeToRound::new(UnixTime::now(), Round(1));
        let kvec = tl_proto::serialize(&key);

        let key_2 = TimeToRound::read_from(&mut &kvec[..])?;

        anyhow::ensure!(key == key_2, "must match after tl write");

        let mut kar = [0; _];
        key.fill(&mut kar);

        anyhow::ensure!(kvec == kar.to_vec(), "bytes must match");

        let round = TimeToRound::read_round(&kar).context("round")?;

        anyhow::ensure!(key.round == round, "round must match");

        Ok(())
    }
}
