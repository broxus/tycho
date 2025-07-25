use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::StreamExt;
use tycho_collator::validator::{
    AddSession, BriefValidatorDescr, ValidationStatus, Validator, ValidatorStdImpl,
    ValidatorStdImplConfig,
};
use tycho_crypto::ed25519;
use tycho_network::{DhtClient, PeerInfo};
use tycho_slasher_traits::NoopValidatorEventsListener;
use tycho_types::cell::HashBytes;
use tycho_types::models::{BlockId, ShardIdent, ValidatorDescription};
use tycho_util::futures::JoinTask;

mod common;

struct ValidatorNode {
    dht_client: DhtClient,
    peer_info: Arc<PeerInfo>,
    validator: ValidatorStdImpl,
    descr: BriefValidatorDescr,
}

impl ValidatorNode {
    fn generate(zerostate_id: &BlockId, rng: &mut impl rand::Rng) -> Self {
        let secret_key = rng.random::<ed25519::SecretKey>();
        let keypair = Arc::new(ed25519::KeyPair::from(&secret_key));

        let validator_network = common::make_validator_network(&secret_key, zerostate_id);
        let validator_descr = BriefValidatorDescr {
            peer_id: *validator_network.network.peer_id(),
            public_key: keypair.public_key,
            weight: 1,
        };

        let network = &validator_network.network;
        let dht_client = validator_network
            .peer_resolver
            .dht_service()
            .make_client(network);
        let peer_info = Arc::new(network.sign_peer_info(0, u32::MAX));

        let validator = ValidatorStdImpl::new(
            validator_network,
            keypair.clone(),
            ValidatorStdImplConfig::default(),
            Arc::new(NoopValidatorEventsListener),
        );

        Self {
            dht_client,
            peer_info,
            validator,
            descr: validator_descr,
        }
    }
}

fn generate_network(
    zerostate_id: &BlockId,
    node_count: usize,
    rng: &mut impl rand::Rng,
) -> Vec<ValidatorNode> {
    let nodes = (0..node_count)
        .map(|_| ValidatorNode::generate(zerostate_id, rng))
        .collect::<Vec<_>>();

    for i in 0..nodes.len() {
        for j in 0..nodes.len() {
            if i == j {
                continue;
            }
            let left = &nodes[i];
            let right = &nodes[j];

            left.dht_client.add_peer(right.peer_info.clone()).unwrap();
            right.dht_client.add_peer(left.peer_info.clone()).unwrap();
        }
    }

    nodes
}

fn make_description(seqno: u32, nodes: &[ValidatorNode]) -> Vec<ValidatorDescription> {
    let mut validators = Vec::with_capacity(nodes.len());
    let mut prev_total_weight = 0;
    for node in nodes {
        validators.push(ValidatorDescription {
            public_key: HashBytes(*node.descr.public_key.as_bytes()),
            weight: 1,
            adnl_addr: Some(HashBytes(*node.descr.peer_id.as_bytes())),
            mc_seqno_since: seqno,
            prev_total_weight,
        });
        prev_total_weight += node.descr.weight;
    }

    validators
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn validator_signatures_match() -> Result<()> {
    tycho_util::test::init_logger(
        "validator_signatures_match",
        "info,tycho_collator::validator=trace",
    );

    const NODE_COUNT: usize = 13;
    const SESSION_COUNT: usize = 5;
    const REQUIRED_SIGS: usize = (NODE_COUNT * 2) / 3 + 1;

    let zerostate_id = BlockId {
        shard: ShardIdent::MASTERCHAIN,
        seqno: 0,
        root_hash: HashBytes::ZERO,
        file_hash: HashBytes::ZERO,
    };
    let nodes = generate_network(&zerostate_id, NODE_COUNT, &mut rand::rng());

    let mut block_id = BlockId {
        seqno: 1,
        ..zerostate_id
    };
    for session_seqno in (0..).step_by(1000).take(SESSION_COUNT) {
        let session_id = (session_seqno, 0);

        tracing::info!(?session_id, %block_id, "adding session");

        let validators = make_description(block_id.seqno, &nodes);
        for node in &nodes {
            node.validator.add_session(AddSession {
                shard_ident: zerostate_id.shard,
                session_id,
                start_block_seqno: block_id.seqno,
                validators: &validators,
            })?;
        }

        for _ in 0..10 {
            tracing::info!(%block_id, ?session_id, "validating block");

            let mut futures = futures_util::stream::FuturesOrdered::new();
            for node in &nodes {
                let peer_id = node.descr.peer_id;
                let validator = node.validator.clone();
                futures.push_back(JoinTask::new(async move {
                    let res = validator.validate(session_id, &block_id).await;
                    (peer_id, res)
                }));
            }

            while let Some((peer_id, res)) = futures.next().await {
                let status = res.with_context(|| {
                    format!("failed to validate block {block_id} for {peer_id}")
                })?;
                let status = BriefStatus::from(&status);
                let BriefStatus::Complete(signature_count) = status else {
                    panic!("must not be skipped");
                };
                assert!(
                    signature_count >= REQUIRED_SIGS,
                    "expected at least {REQUIRED_SIGS} signatures, got {signature_count}"
                );

                tracing::info!(%peer_id, ?status, "validation completed");
            }

            // TODO: Build test around some test-only events collector

            // let short = block_id.as_short_id();
            // let range = short..=short;

            // for node in &nodes {
            //     let events = node.event_collector.stats_for_blocks(range.clone());

            //     // check current node signature
            //     let self_stat = events
            //         .get(&node.descr.peer_id)
            //         .expect("current node should have stats");

            //     assert_eq!(self_stat.invalid, 0);
            //     assert_eq!(self_stat.valid, 1);

            //     // check total valid signatures
            //     let total_valid: usize = events.values().filter(|s| s.valid > 0).count();

            //     assert!(
            //         total_valid >= REQUIRED_SIGS,
            //         "total_valid ({total_valid}) < REQUIRED_SIGS ({REQUIRED_SIGS})"
            //     );

            //     // check that no invalid signatures were given
            //     for (peer, stat) in &events {
            //         assert_eq!(
            //             stat.invalid, 0,
            //             "peer {peer:?} has invalid signatures: {stat:?}"
            //         );
            //     }
            // }

            for node in &nodes {
                node.validator
                    .cancel_validation(&block_id.as_short_id(), Some(session_id))?;
            }

            block_id.seqno += 1;
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malicious_validators_are_ignored() -> Result<()> {
    tycho_util::test::init_logger(
        "malicious_validators_are_ignored",
        "info,tycho_collator::validator=trace",
    );

    const NODE_COUNT: usize = 13;
    const MALICIOUS_NODE_COUNT: usize = 3;

    const SESSION_COUNT: usize = 5;
    const REQUIRED_SIGS: usize = (NODE_COUNT * 2) / 3 + 1; // 9

    let zerostate_id = BlockId {
        shard: ShardIdent::MASTERCHAIN,
        seqno: 0,
        root_hash: HashBytes::ZERO,
        file_hash: HashBytes::ZERO,
    };
    let nodes = generate_network(&zerostate_id, NODE_COUNT, &mut rand::rng());

    let mut block_id = BlockId {
        seqno: 1,
        ..zerostate_id
    };
    for session_seqno in (0..).step_by(1000).take(SESSION_COUNT) {
        let session_id = (session_seqno, 0);

        tracing::info!(?session_id, %block_id, "adding session");

        let validators = make_description(block_id.seqno, &nodes);
        for node in &nodes {
            node.validator.add_session(AddSession {
                shard_ident: zerostate_id.shard,
                session_id,
                start_block_seqno: block_id.seqno,
                validators: &validators,
            })?;
        }

        for _ in 0..10 {
            tracing::info!(%block_id, ?session_id, "validating block");

            let mut good_validators = futures_util::stream::FuturesOrdered::new();
            let mut bad_validators = futures_util::stream::FuturesOrdered::new();

            for (i, node) in nodes.iter().enumerate() {
                let peer_id = node.descr.peer_id;
                let validator = node.validator.clone();

                let is_malicious = i < MALICIOUS_NODE_COUNT;
                if is_malicious {
                    bad_validators.push_back(JoinTask::new(async move {
                        let mut block_id = block_id;
                        block_id.root_hash = rand::random();

                        let res = validator.validate(session_id, &block_id).await;
                        (peer_id, res)
                    }));
                } else {
                    good_validators.push_back(JoinTask::new(async move {
                        let res = validator.validate(session_id, &block_id).await;
                        (peer_id, res)
                    }));
                }
            }

            while let Some((peer_id, res)) = good_validators.next().await {
                let status = res.with_context(|| {
                    format!("failed to validate block {block_id} for {peer_id}")
                })?;

                match &status {
                    ValidationStatus::Complete(res) => {
                        let sigs = res.signatures.len();
                        assert!(sigs >= REQUIRED_SIGS, "need {REQUIRED_SIGS}, got {sigs}");
                        assert!(
                            sigs <= NODE_COUNT - MALICIOUS_NODE_COUNT,
                            "malicious sigs leaked, got {sigs}"
                        );
                    }
                    ValidationStatus::Skipped => panic!("good validator skipped block"),
                }

                tracing::info!(%peer_id, status = ?BriefStatus::from(&status), "validation completed");
            }

            for node in &nodes {
                node.validator
                    .cancel_validation(&block_id.as_short_id(), Some(session_id))?;
            }

            while let Some((peer_id, res)) = bad_validators.next().await {
                let status = res.with_context(|| {
                    format!("failed to validate block {block_id} for {peer_id}")
                })?;

                match &status {
                    ValidationStatus::Complete(_) => panic!("bad validator completed block"),
                    ValidationStatus::Skipped => tracing::info!(%peer_id, "validation skipped"),
                }
            }

            // TODO: Build test around some test-only events collector

            // let short = block_id.as_short_id();
            // let range = short..=short;
            // for (i, node) in nodes.iter().enumerate() {
            //     let stats = node.event_collector.stats_for_blocks(range.clone());
            //     let s = stats.get(&node.descr.peer_id);

            //     if i < MALICIOUS_NODE_COUNT {
            //         // malicious node must not have valid stats
            //         assert!(
            //             s.is_none_or(|st| st.valid == 0),
            //             "malicious node {:?} has valid sigs in stats: {:?}",
            //             node.descr.peer_id,
            //             s
            //         );
            //     } else {
            //         // good node must have valid stats
            //         let st = s.expect("good node must have stats");
            //         assert_eq!(
            //             st.valid, 1,
            //             "good node {:?} valid !=1 {:?}",
            //             node.descr.peer_id, st
            //         );
            //         assert_eq!(
            //             st.invalid, 0,
            //             "good node {:?} invalid !=0 {:?}",
            //             node.descr.peer_id, st
            //         );
            //     }
            // }

            block_id.seqno += 1;
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn network_gets_stuck_without_signatures() -> Result<()> {
    tycho_util::test::init_logger(
        "network_gets_stuck_without_signatures",
        "info,tycho_collator::validator=trace",
    );

    const NODE_COUNT: usize = 13;
    const STUCK_DURATION: Duration = Duration::from_secs(10);

    let zerostate_id = BlockId {
        shard: ShardIdent::MASTERCHAIN,
        seqno: 0,
        root_hash: HashBytes::ZERO,
        file_hash: HashBytes::ZERO,
    };
    let nodes = generate_network(&zerostate_id, NODE_COUNT, &mut rand::rng());

    let block_id = BlockId {
        seqno: 1,
        ..zerostate_id
    };
    let session_id = (0, 0);

    let validators = make_description(block_id.seqno, &nodes);
    for node in &nodes {
        node.validator.add_session(AddSession {
            shard_ident: zerostate_id.shard,
            session_id,
            start_block_seqno: block_id.seqno,
            validators: &validators,
        })?;
    }

    let malicious_node_count = (NODE_COUNT / 3) + 1;

    let mut good_validators = futures_util::stream::FuturesOrdered::new();
    let mut bad_validators = futures_util::stream::FuturesOrdered::new();

    let mut nodes_blocks = Vec::with_capacity(NODE_COUNT);
    for (i, node) in nodes.iter().enumerate() {
        let is_malicious = i < malicious_node_count;

        let mut blk = block_id;
        if is_malicious {
            blk.root_hash = rand::random();
        }

        nodes_blocks.push(blk);

        let peer_id = node.descr.peer_id;
        let validator = node.validator.clone();

        let fut = async move {
            let res = validator.validate(session_id, &blk).await;
            (peer_id, res)
        };

        if is_malicious {
            bad_validators.push_back(JoinTask::new(fut));
        } else {
            good_validators.push_back(JoinTask::new(fut));
        }
    }

    // let range = block_id.as_short_id()..=block_id.as_short_id();
    tokio::select! {
        _ = good_validators.next() => {
            panic!("good validator completed block");
        }
        _ = bad_validators.next() => {
            panic!("malicious validator got block from the stuck network");
        }
        _ = tokio::time::sleep(STUCK_DURATION) => {
            tracing::info!("network got stuck as expected");

            // TODO: Build test around some test-only events collector

            // // 1) check event collector in each node
            // for node in &nodes {
            //     let events = node.event_collector.stats_for_blocks(range.clone());
            //     // each node should have no events
            //     assert_eq!(events.len(), 0);
            // }

            // // 2) notify all nodes about validation completion
            // for (i, node) in nodes.iter().enumerate() {
            //     let block_id = nodes_blocks.get(i)
            //         .expect("should have block id for each node");
            //     node.event_collector.on_validation_complete(
            //         &SessionCtx { session_id },
            //         block_id,
            //     )?;
            // }

            // // 3) calc total valid and invalid signatures
            // for (i, node) in nodes.iter().enumerate() {
            //     let is_malicious = i < malicious_node_count;
            //     let events = node.event_collector.stats_for_blocks(range.clone());
            //     let total_invalid = events.values().map(|s| s.invalid).sum::<u32>() as usize;
            //     let total_valid = events.values().map(|s| s.valid).sum::<u32>()  as usize;
            //     if is_malicious {
            //         // valid only self-own signature because block has a random root hash
            //         assert_eq!(total_valid, 1,
            //             "malicious node {:?} has valid signatures", node.descr.peer_id);
            //         // malicious nodes should have no valid signatures except their own
            //         assert_eq!(total_invalid, NODE_COUNT - 1,
            //             "malicious node {:?} has valid signatures", node.descr.peer_id);
            //     } else {
            //         // good nodes should have valid signatures from all other good nodes
            //         assert_eq!(total_valid, NODE_COUNT - malicious_node_count,
            //             "good node {:?} has no valid signatures", node.descr.peer_id);
            //         // good nodes should have invalid signatures from all malicious nodes
            //         assert_eq!(total_invalid, malicious_node_count,
            //             "good node {:?} has invalid signatures", node.descr.peer_id);

            //     }
            // }
        }
    }

    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
enum BriefStatus {
    Skipped,
    Complete(usize),
}

impl From<&ValidationStatus> for BriefStatus {
    fn from(value: &ValidationStatus) -> Self {
        match value {
            ValidationStatus::Skipped => Self::Skipped,
            ValidationStatus::Complete(res) => Self::Complete(res.signatures.len()),
        }
    }
}
