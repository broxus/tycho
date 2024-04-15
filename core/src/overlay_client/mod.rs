use std::time::Duration;
use crate::overlay_client::public_overlay_client::PublicOverlayClient;

pub mod neighbour;
pub mod neighbours;
pub mod public_overlay_client;
pub mod settings;


async fn start_neighbours_ping(client: PublicOverlayClient) {
    let mut interval = tokio::time::interval(Duration::from_millis(client.neighbour_update_interval_ms()));

    loop {
        interval.tick().await;
        if let Err(e) = client.ping_random_neighbour().await {
            tracing::error!("Failed to ping random neighbour. Error: {e:?}");
        }
    }
}

async fn start_neighbours_update(client: PublicOverlayClient) {
    let mut interval = tokio::time::interval(Duration::from_millis(client.neighbour_update_interval_ms()));
    loop {
        interval.tick().await;
        client.update_neighbours().await;
    }
}

async fn wait_update_neighbours(client: PublicOverlayClient) {
    loop {
        client.entries_removed().await;
        client.remove_outdated_neighbours().await;
    }
}