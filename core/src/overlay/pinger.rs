use crate::overlay::public_overlay_client::PublicOverlayClient;
use std::time::{Duration, Instant};

async fn ping_neighbours(client: PublicOverlayClient) {
    let mut interval = tokio::time::interval(Duration::from_secs(2));

    loop {
        interval.tick().await;
        if let Err(e) = client.ping_random_neighbour().await {
            tracing::error!("Failed to ping random neighbour. Error: {e:?}")
        }
    }
}
