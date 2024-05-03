use std::net::Ipv4Addr;
use std::time::Duration;

use everscale_crypto::ed25519;
use everscale_types::boc::Boc;
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockId, ShardStateUnsplit};
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use sha2::Digest;
use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};

use tycho_network::{DhtConfig, DhtService, Network, OverlayService, PeerId, Router};
use tycho_storage::{BlockMetaData, Db, DbOptions, Storage};

use crate::types::NodeNetwork;

pub fn try_init_test_tracing(level_filter: tracing_subscriber::filter::LevelFilter) {
    use std::io::IsTerminal;
    tracing_subscriber::fmt()
        .with_ansi(std::io::stdout().is_terminal())
        .with_writer(std::io::stdout)
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(level_filter.into())
                .from_env_lossy(),
        )
        .with_file(true)
        .with_line_number(true)
        .with_target(true)
        //.with_thread_ids(true)
        .compact()
        .try_init()
        .ok();
}

pub fn create_node_network() -> NodeNetwork {
    let random_secret_key = ed25519::SecretKey::generate(&mut rand::thread_rng());
    let keypair = ed25519::KeyPair::from(&random_secret_key);
    let local_id = PeerId::from(keypair.public_key);
    let (_, overlay_service) = OverlayService::builder(local_id).build();

    let router = Router::builder().route(overlay_service.clone()).build();
    let network = Network::builder()
        .with_private_key(random_secret_key.to_bytes())
        .with_service_name("test-service")
        .build((Ipv4Addr::LOCALHOST, 0), router)
        .unwrap();

    let (_, dht_service) = DhtService::builder(local_id)
        .with_config(DhtConfig {
            local_info_announce_period: Duration::from_secs(1),
            local_info_announce_period_max_jitter: Duration::from_secs(1),
            routing_table_refresh_period: Duration::from_secs(1),
            routing_table_refresh_period_max_jitter: Duration::from_secs(1),
            ..Default::default()
        })
        .build();

    let dht_client = dht_service.make_client(&network);
    let peer_resolver = dht_service.make_peer_resolver().build(&network);

    NodeNetwork {
        overlay_service,
        peer_resolver,
        dht_client,
    }
}

pub async fn prepare_test_storage() -> anyhow::Result<Storage> {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().to_path_buf(), DbOptions::default()).unwrap();
    let storage = Storage::new(db, temp.path().join("file"), 1_000_000).unwrap();
    let tracker = MinRefMcStateTracker::default();

    // master state
    let master_bytes = include_bytes!("../src/state_node/tests/data/test_state_2_master.boc");
    let master_file_hash: HashBytes = sha2::Sha256::digest(master_bytes).into();
    let master_root = Boc::decode(master_bytes)?;
    let master_root_hash = *master_root.repr_hash();
    let master_state = master_root.parse::<ShardStateUnsplit>()?;

    let mc_state_extra = master_state.load_custom()?;
    let mc_state_extra = mc_state_extra.unwrap();
    let mut shard_info_opt = None;
    for shard_info in mc_state_extra.shards.iter() {
        shard_info_opt = Some(shard_info?);
        break;
    }
    let shard_info = shard_info_opt.unwrap();

    let master_id = BlockId {
        shard: master_state.shard_ident,
        seqno: master_state.seqno,
        root_hash: master_root_hash,
        file_hash: master_file_hash,
    };
    let master_state_stuff =
        ShardStateStuff::from_state_and_root(master_id, master_state, master_root, &tracker)?;

    let (handle, _) = storage.block_handle_storage().create_or_load_handle(
        &master_id,
        BlockMetaData {
            is_key_block: mc_state_extra.after_key_block,
            gen_utime: master_state_stuff.state().gen_utime,
            mc_ref_seqno: 0,
        },
    );

    storage
        .shard_state_storage()
        .store_state(&handle, &master_state_stuff)
        .await?;

    // shard state
    let shard_bytes = include_bytes!("../src/state_node/tests/data/test_state_2_0:80.boc");
    let shard_root = Boc::decode(shard_bytes)?;
    let shard_state = shard_root.parse::<ShardStateUnsplit>()?;
    let shard_id = BlockId {
        shard: shard_info.0,
        seqno: shard_info.1.seqno,
        root_hash: shard_info.1.root_hash,
        file_hash: shard_info.1.file_hash,
    };
    let shard_state_stuff =
        ShardStateStuff::from_state_and_root(shard_id, shard_state, shard_root, &tracker)?;

    let (handle, _) = storage.block_handle_storage().create_or_load_handle(
        &shard_id,
        BlockMetaData {
            is_key_block: false,
            gen_utime: shard_state_stuff.state().gen_utime,
            mc_ref_seqno: 0,
        },
    );

    storage
        .shard_state_storage()
        .store_state(&handle, &shard_state_stuff)
        .await?;

    storage.node_state().store_last_mc_block_id(&master_id);

    Ok(storage)
}
