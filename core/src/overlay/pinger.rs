use std::time::{Duration, Instant};
use crate::overlay::public_overlay_client::PublicOverlayClient;

async fn ping_neighbours(client: PublicOverlayClient) {
    let mut interval = tokio::time::interval(Duration::from_secs(2));

    loop {
        interval.tick().await;
        let _ = client.ping_random_neighbour().await;
    }
}