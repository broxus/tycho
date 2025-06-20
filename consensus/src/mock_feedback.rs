use std::time::Duration;

use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use itertools::Itertools;
use tl_proto::{TlRead, TlWrite};
use tokio::time::MissedTickBehavior;
use tycho_network::{PeerId, Request};

use crate::engine::round_watch::{RoundWatch, TopKnownAnchor};
use crate::intercom::{Dispatcher, InitPeers, WeakPeerSchedule};
use crate::models::Round;

#[derive(TlRead, TlWrite)]
#[tl(
    boxed,
    id = "roundBoxed",
    scheme_inline = "roundBoxed round:int = RoundBoxed;"
)]
pub struct RoundBoxed {
    pub round: Round,
}

pub struct MockFeedbackSender {
    dispatcher: Dispatcher,
    peer_schedule: WeakPeerSchedule,
    top_known_anchor: RoundWatch<TopKnownAnchor>,
    window: Window,
}

impl MockFeedbackSender {
    pub fn new(
        dispatcher: Dispatcher,
        peer_schedule: WeakPeerSchedule,
        top_known_anchor: RoundWatch<TopKnownAnchor>,
        init_peers: &InitPeers,
        peer_id: &PeerId,
    ) -> Self {
        let Some((index, _)) = (init_peers.curr_v_subset.iter()).find_position(|a| *a == peer_id)
        else {
            panic!("local peer id not found in init peer set")
        };
        Self {
            dispatcher,
            peer_schedule,
            top_known_anchor,
            window: Window {
                v_set_size: init_peers.curr_v_subset.len() as u32,
                group_size: (init_peers.curr_v_subset.len() as u32 / 10) + 1,
                index: index as u32,
            },
        }
    }

    pub async fn run(self) {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut send_futures = FuturesUnordered::new();

        loop {
            tokio::select! {
                next = send_futures.next() => match next {
                    Some(_) => continue,
                    None => {
                        interval.tick().await;
                    },
                },
                _ = interval.tick() => {},
            }

            if !self.window.is_active(tycho_util::time::now_sec()) {
                continue;
            }

            let request = Request::from_tl(RoundBoxed {
                round: self.top_known_anchor.get(),
            });
            let Some(peer_schedule) = self.peer_schedule.upgrade() else {
                return;
            };
            let receivers = {
                let guard = peer_schedule.read();
                guard.data.broadcast_receivers().clone()
            };

            send_futures.clear();

            for peer_id in &receivers {
                let future = self.dispatcher.send_feedback(peer_id, &request);
                send_futures.push(future);
            }
        }
    }
}

struct Window {
    v_set_size: u32,
    group_size: u32,
    index: u32,
}

impl Window {
    /// wrapping groups of both `group_size` size and step, over `v_set_size` set
    fn is_active(&self, now: u32) -> bool {
        let group_start = ((now % self.v_set_size) * self.group_size) % self.v_set_size;
        let group_end = (group_start + self.group_size - 1) % self.v_set_size;
        if group_start <= group_end {
            group_start <= self.index && self.index <= group_end
        } else {
            self.index <= group_end || group_start <= self.index
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_wrap() {
        let window = Window {
            v_set_size: 5,
            group_size: 3,
            index: 0,
        };

        assert!(window.is_active(20)); //  0,1,2
        assert!(window.is_active(21)); //  3,4,0
        assert!(!window.is_active(22)); // 1,2,3
        assert!(window.is_active(23)); //  4,0,1
        assert!(!window.is_active(24)); // 2,3,4
    }
}
