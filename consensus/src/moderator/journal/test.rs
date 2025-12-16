use anyhow::{Context, Result};
use itertools::Itertools;
use rand::RngCore;
use tl_proto::TlError;
use tycho_crypto::ed25519::KeyPair;
use tycho_network::PeerId;
use tycho_storage::StorageContext;

use crate::dag::InvalidReason;
use crate::intercom::QueryRequestTag;
use crate::models::{
    Cert, DagPoint, Digest, Link, Point, PointData, PointStatusStored, PointStatusValidated, Round,
    UnixTime,
};
use crate::moderator::journal::batch::batch;
use crate::moderator::journal::item::{JournalItem, JournalItemFull};
use crate::moderator::journal::record_key::RecordKeyFactory;
use crate::moderator::{JournalEvent, RecordFull, RecordValue};
use crate::storage::{JournalStore, MempoolDb, MempoolStore};
use crate::test_utils::default_test_config;

const DIFF: UnixTime = UnixTime::from_millis(60 * 1000);

#[tokio::test]
async fn test() -> Result<()> {
    let point_max_bytes = default_test_config().conf.point_max_bytes;

    let (ctx, _tmp_dir) = StorageContext::new_temp().await?;
    let db = MempoolDb::open(ctx)?;
    let store = JournalStore::new(db.clone());

    let points: [_; 5] = std::array::from_fn(|_| gen_point());

    for point in &points {
        let main_store = MempoolStore::new(db.clone());
        let status = PointStatusStored::Validated(PointStatusValidated::default());
        main_store.insert_point(point, status.as_ref());
    }

    let stored = gen_items(10, &points);

    anyhow::ensure!(
        stored.iter().all(|r| r.item.action().store()),
        "test is expected to store all records, change generated events"
    );

    store.store_records(batch(&stored), point_max_bytes)?;

    let stored = stored
        .into_iter()
        .map(|full| RecordFull {
            key: full.key,
            value: item_to_value(full.item),
        })
        .collect::<Vec<_>>();

    anyhow::ensure!(
        stored == store.load_records(u16::MAX, 0, true)?,
        "must load all records"
    );

    load_same_points(&store, &points).context("just stored")?;

    store.delete(UnixTime::from_millis(0)..UnixTime::now() - DIFF)?;

    anyhow::ensure!(
        stored == store.load_records(u16::MAX, 0, true)?,
        "delete past must not affect records"
    );

    load_same_points(&store, &points).context("after delete past")?;

    store.delete(UnixTime::now() + DIFF..UnixTime::from_millis(u64::MAX))?;

    anyhow::ensure!(
        stored == store.load_records(u16::MAX, 0, true)?,
        "delete future must not affect records"
    );

    load_same_points(&store, &points).context("after delete future")?;

    store.delete(UnixTime::now() - DIFF..UnixTime::now() + DIFF)?;

    anyhow::ensure!(
        store.load_records(u16::MAX, 0, true)?.is_empty(),
        "must delete all records"
    );

    for point in points {
        let key = point.info().key();
        let not_found = store.get_point(&key).context("load")?.is_none();
        anyhow::ensure!(not_found, "must remove all points");
    }

    Ok(())
}

fn load_same_points(store: &JournalStore, points: &[Point]) -> Result<()> {
    for point in points {
        let bytes = store
            .get_point(&point.info().key())
            .context("load point")?
            .context("point not found")?;
        let parsed = Point::parse(bytes.into())??.map_err(|(_, err)| err)?;
        anyhow::ensure!(point == &parsed, "must load same point");
    }
    Ok(())
}

fn gen_point() -> Point {
    let keypair = rand::random::<KeyPair>();
    Point::new(
        &keypair,
        PeerId::from(keypair.public_key),
        Round(rand::rng().next_u32()),
        Default::default(),
        PointData {
            includes: Default::default(),
            witness: Default::default(),
            evidence: Default::default(),
            anchor_trigger: Link::ToSelf,
            anchor_proof: Link::ToSelf,
            time: UnixTime::now(),
            anchor_time: UnixTime::now(),
        },
        &default_test_config().conf,
    )
}

fn gen_items(count: u8, points: &[Point]) -> Vec<JournalItemFull> {
    let mut result = Vec::new();
    let mut kf = RecordKeyFactory::default();

    for i in 0..count {
        let peer_id = PeerId([i; _]);
        let event = match i % 4 {
            0 => match points.get(i as usize) {
                Some(point) => JournalEvent::SenderNotAuthor(peer_id, point.info().id()),
                None => JournalEvent::UnknownQuery(peer_id, TlError::InvalidData),
            },
            1 => match points.get(i as usize) {
                Some(point) => JournalEvent::ReplacedPoint {
                    peer_id,
                    requested: point.info().id(), // should be unique but won't
                    received: point.info().id(),
                },
                None => JournalEvent::BadRequest(
                    peer_id,
                    QueryRequestTag::Signature,
                    TlError::UnexpectedEof,
                ),
            },
            2 => match points.get(i as usize) {
                Some(point) => JournalEvent::EvidenceNoInclusion {
                    signer: peer_id,
                    blamed: vec![point.info().id()],
                    proofs: vec![point.info().id()],
                },
                None => JournalEvent::Equivocated(
                    peer_id,
                    Round(rand::rng().next_u32()),
                    std::array::from_fn::<_, 3, _>(|j| *Digest::wrap(&[j as u8; _])).to_vec(),
                ),
            },
            3 => match points.get(i as usize) {
                Some(point) => {
                    let info = point.info().clone();
                    let status = PointStatusValidated::default();
                    let reason = InvalidReason::MustHaveSkippedRound(point.info().id());
                    match DagPoint::new_invalid(info, Cert::default(), &status, reason) {
                        DagPoint::Invalid(invalid) => JournalEvent::Invalid(invalid),
                        _ => unreachable!(),
                    }
                }
                None => JournalEvent::QueryLimitReached(peer_id, QueryRequestTag::Broadcast),
            },
            _ => unreachable!(),
        };
        let key = kf.new_key();
        let item = JournalItem::Event(event);
        result.push(JournalItemFull { key, item });
    }

    result
}

fn item_to_value(item: JournalItem) -> RecordValue {
    let mut points = Vec::new();
    let mut keys = Vec::new();
    item.fill_points(&mut points, &mut keys);

    let point_keys = points
        .iter()
        .map(|point| point.info().key())
        .chain(keys)
        .unique()
        .sorted()
        .collect();

    RecordValue {
        is_ban_related: item.action().is_ban_related(),
        kind: item.kind(),
        peer_id: *item.peer_id(),
        point_keys,
        message: item.to_string(),
    }
}
