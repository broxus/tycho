use rand::distributions::{Distribution, WeightedIndex};
use rand::thread_rng;
use std::time::Instant;
use tl_proto::{TlRead, TlWrite};
use tycho_core::overlay_client::neighbour::{Neighbour, NeighbourOptions};
use tycho_core::overlay_client::neighbours::Neighbours;
use tycho_core::overlay_client::public_overlay_client::Peer;
use tycho_core::overlay_client::settings::NeighboursOptions;
use tycho_network::{PeerId};

#[derive(TlWrite, TlRead)]
#[tl(boxed, id = 0x11223344)]
struct TestResponse;

#[tokio::test]
pub async fn test() {
    let options = NeighboursOptions::default();
    let initial_peers = vec![
        PeerId([0u8; 32]),
        PeerId([1u8; 32]),
        PeerId([2u8; 32]),
        PeerId([3u8; 32]),
        PeerId([4u8; 32]),
    ]
    .iter()
    .map(|x| Peer {
        id: *x,
        expires_at: u32::MAX,
    })
    .collect::<Vec<_>>();
    println!("{}", initial_peers.len());

    let neighbours = Neighbours::new(initial_peers.clone(), options.clone()).await;
    println!("{}", neighbours.get_active_neighbours().await.len());


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
    while i < 1000 {
        //let start = Instant::now();
        let n_opt = neighbours.choose().await;
        //let end = Instant::now();

        if let Some(n) = n_opt {
            let index = slice.iter().position(|r| r.id == n.peer_id()).unwrap();
            let answer = indices[index].sample(&mut rng);
            if answer == 0 {
                println!("Success request to peer: {}", n.peer_id());
                n.track_request(200, true)
            } else {
                println!("Failed request to peer: {}", n.peer_id());
                n.track_request(200, false)
            }

            neighbours.update_selection_index().await;
        }
        i = i + 1;
    }

    let new_peers = vec![
        PeerId([5u8; 32]),
        PeerId([6u8; 32]),
        PeerId([7u8; 32]),
        PeerId([8u8; 32]),
        PeerId([9u8; 32]),
    ]
    .iter()
    .map(|x| Peer {
        id: *x,
        expires_at: u32::MAX,
    })
    .collect::<Vec<_>>();

    let new_neighbours = new_peers
        .iter()
        .map(|x| {
            Neighbour::new(
                x.id,
                x.expires_at,
                NeighbourOptions {
                    default_roundtrip_ms: options.default_roundtrip_ms,
                },
            )
        })
        .collect::<Vec<_>>();

    neighbours.update(new_neighbours).await;

    let active = neighbours.get_active_neighbours().await;
    println!("active neighbours {}", active.len());
    for i in active {
        println!("peer {} score {}", i.peer_id(), i.get_stats().score);
    }

    //assert_ne!(peers.len(), 5);
}
