use crate::overlay_client::public_overlay_client::PublicOverlayClient;
use std::time::Duration;

async fn start_neighbours_ping(client: PublicOverlayClient) {
    let mut interval = tokio::time::interval(Duration::from_millis(client.update_interval()));

    loop {
        interval.tick().await;
        if let Err(e) = client.ping_random_neighbour().await {
            tracing::error!("Failed to ping random neighbour. Error: {e:?}");
        }
    }
}

async fn start_neighbours_update(client: PublicOverlayClient) {
    let mut interval = tokio::time::interval(Duration::from_millis(client.update_interval()));
    loop {
        interval.tick().await;
        client.update_neighbours().await;
    }
}


