use crate::overlay::public_overlay_client::PublicOverlayClient;
use std::time::{Duration, Instant};

async fn ping_neighbours(client: PublicOverlayClient) {
    let mut interval = tokio::time::interval(Duration::from_secs(2));

    loop {
        interval.tick().await;

        let Some(neighbour) = client.neighbours().choose().await else {
            tracing::error!("no neighbours found");
            return;
        };

        tracing::info!(
            peer_id = %neighbour.peer_id(),
            stats = ?neighbour.get_stats(),
            "selected neighbour",
        );

        let timer = Instant::now();
        let res = client.get_capabilities(&neighbour).await;
        let roundtrip = timer.elapsed().as_millis() as u64;

        let success = match res {
            Ok(capabilities) => {
                tracing::info!(?capabilities, peer_id = %neighbour.peer_id());
                true
            }
            Err(e) => {
                tracing::error!(peer_id = %neighbour.peer_id(), "failed to receive capabilities: {e:?}");
                false
            }
        };

        neighbour.track_adnl_request(roundtrip, success);
        client.neighbours().update_selection_index().await;
    }
}
