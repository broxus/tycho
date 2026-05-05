use std::time::{Duration, Instant};

use reqwest::Client;
use serde::Serialize;

const RPC_URL: &str = "http://127.0.0.1:8080/rpc";

#[derive(Serialize)]
struct JrpcRequest {
    jsonrpc: &'static str,
    id: u32,
    method: &'static str,
    params: serde_json::Value,
}

fn request_body() -> JrpcRequest {
    JrpcRequest {
        jsonrpc: "2.0",
        id: 1,
        method: "getStatus",
        params: serde_json::json!({}),
    }
}

async fn send(client: &Client, url: &str) -> u16 {
    client
        .post(url)
        .json(&request_body())
        .send()
        .await
        .expect("request failed")
        .status()
        .as_u16()
}

async fn concurrent_burst(client: &Client, url: &str, count: usize) -> (usize, usize) {
    let futs: Vec<_> = (0..count)
        .map(|_| {
            let client = client.clone();
            let url = url.to_owned();
            tokio::spawn(async move { send(&client, &url).await })
        })
        .collect();

    let mut ok = 0;
    let mut rejected = 0;
    for fut in futs {
        match fut.await.unwrap() {
            200 => ok += 1,
            429 => rejected += 1,
            other => panic!("unexpected status: {other}"),
        }
    }
    (ok, rejected)
}

#[tokio::test]
#[ignore = "requires a running node"]
async fn rpc_rate_limits() {
    let url = std::env::var("RPC_URL").unwrap_or_else(|_| RPC_URL.to_owned());
    let client = Client::new();

    // concurrent burst — should hit the limit
    let n = 100;
    let start = Instant::now();
    let (ok, rejected) = concurrent_burst(&client, &url, n).await;
    let elapsed = start.elapsed();

    println!("concurrent burst {n}: ok={ok}, rejected={rejected}, elapsed={elapsed:?}");
    assert!(rejected > 0, "no rate limiting observed at burst={n}");
    println!("rate limit kicked in after {ok} requests");

    // wait for cooldown to expire (default: 30s)
    println!("waiting 31s for cooldown to expire...");
    tokio::time::sleep(Duration::from_secs(31)).await;

    // single request should succeed again
    let status = send(&client, &url).await;
    println!("after cooldown: {status}");
    assert_eq!(status, 200, "tokens did not refill after cooldown");

    // sustained 1 rps — should be fine
    let mut ok_count = 0;
    for _ in 0..5 {
        if send(&client, &url).await == 200 {
            ok_count += 1;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    println!("sustained 1rps: {ok_count}/5 OK");
    assert_eq!(ok_count, 5, "sustained 1 rps should not be limited");
}
