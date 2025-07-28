use tokio::sync::mpsc::UnboundedSender;
use tycho_network::PeerId;
use tycho_slasher_traits::{MempoolEventsListener, MempoolPeerStats};
use tycho_util::FastHashMap;

use crate::models::{MempoolStatsOutput, Round};

pub struct StatsSender {
    pub sender: UnboundedSender<MempoolStatsOutput>,
}

impl MempoolEventsListener for StatsSender {
    fn put_stats(&self, anchor_round: u32, data: FastHashMap<PeerId, MempoolPeerStats>) {
        let reconstructed = MempoolStatsOutput {
            anchor_round: Round(anchor_round),
            data,
        };
        self.sender
            .send(reconstructed)
            .expect("stats channel must not be closed");
    }
}
