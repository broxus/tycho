use rand::distributions::{Distribution, WeightedIndex};
use rand::thread_rng;
use tycho_core::overlay_client::neighbours::{Neighbours, NeighboursOptions};
use tycho_network::{OverlayId, PeerId, PublicOverlay};

#[tokio::test]
pub async fn test() {
    let initial_peers = vec![
        PeerId([0u8; 32]),
        PeerId([1u8; 32]),
        PeerId([2u8; 32]),
        PeerId([3u8; 32]),
        PeerId([4u8; 32]),
    ];
    let public_overlay =  PublicOverlay::builder(OverlayId([0u8;32]))
        .build(PingPongService);
    let neighbours = Neighbours::new(
        public_overlay,
        NeighboursOptions::default(),

    )
    .await;

    let first_success_rate = [0.2, 0.8];
    let second_success_rate = [1.0, 0.0];
    let third_success_rate = [0.5, 0.5];
    let fourth_success_rate = [0.8, 0.2];
    let fifth_success_rate = [0.0, 1.0];

    let indices = vec![
        WeightedIndex::new(&first_success_rate).unwrap(),
        WeightedIndex::new(&second_success_rate).unwrap(),
        WeightedIndex::new(&third_success_rate).unwrap(),
        WeightedIndex::new(&fourth_success_rate).unwrap(),
        WeightedIndex::new(&fifth_success_rate).unwrap(),
    ];

    let mut i = 0;
    let mut rng = thread_rng();
    let slice = initial_peers.as_slice();
    while i < 100 {
        if let Some(n) = neighbours.choose().await {
            let index = slice.iter().position(|&r| r == n.peer_id()).unwrap();
            let answer = indices[index].sample(&mut rng);
            if answer == 0 {
                println!("Success request to peer: {}", n.peer_id());
                n.track_request(200, true)
            } else {
                println!("Failed request to peer: {}", n.peer_id());
                n.track_request(200, false)
            }
            neighbours.update_selection_index().await
        }
        i = i + 1;
    }
    let peers = neighbours
        .get_sorted_neighbours()
        .await
        .iter()
        .map(|(n, w)| (*n.peer_id(), *w))
        .collect::<Vec<_>>();

    for i in &peers {
        println!("Peer: {:?}", i);
    }


   assert_ne!(peers.len(), 5);
}
