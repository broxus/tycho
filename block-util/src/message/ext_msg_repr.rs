use std::cell::RefCell;
use tycho_types::cell::CellTreeStats;
use tycho_types::error::Error;
use tycho_types::models::{ExtInMsgInfo, IntAddr, MsgType, StateInit};
use tycho_types::prelude::*;
use tycho_util::FastHashMap;
pub async fn validate_external_message(
    body: &bytes::Bytes,
) -> Result<(), InvalidExtMsg> {
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(validate_external_message)),
        file!(),
        9u32,
    );
    let body = body;
    if body.len() > ExtMsgRepr::BOUNDARY_BOC_SIZE {
        let body = body.clone();
        {
            __guard.end_section(13u32);
            let __result = tycho_util::sync::rayon_run_fifo(move || {
                    ExtMsgRepr::validate(&body).map(|_| ())
                })
                .await;
            __guard.start_section(13u32);
            __result
        }
    } else {
        ExtMsgRepr::validate(body).map(|_| ())
    }
}
pub async fn parse_external_message(body: &bytes::Bytes) -> Result<Cell, InvalidExtMsg> {
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(parse_external_message)),
        file!(),
        19u32,
    );
    let body = body;
    if body.len() > ExtMsgRepr::BOUNDARY_BOC_SIZE {
        let body = body.clone();
        {
            __guard.end_section(22u32);
            let __result = tycho_util::sync::rayon_run_fifo(move || ExtMsgRepr::validate(
                    &body,
                ))
                .await;
            __guard.start_section(22u32);
            __result
        }
    } else {
        ExtMsgRepr::validate(body)
    }
}
/// Computes a normalized message.
///
/// A normalized message contains only `dst` address and a body as reference.
pub fn normalize_external_message(cell: &'_ DynCell) -> Result<Cell, Error> {
    let mut cs = cell.as_slice()?;
    if MsgType::load_from(&mut cs)? != MsgType::ExtIn {
        return Err(Error::InvalidData);
    }
    let info = ExtInMsgInfo::load_from(&mut cs)?;
    if cs.load_bit()? {
        if cs.load_bit()? {
            cs.load_reference()?;
        } else {
            StateInit::load_from(&mut cs)?;
        }
    }
    let body = if cs.load_bit()? {
        cs.load_reference_cloned()?
    } else if cs.is_empty() {
        Cell::empty_cell()
    } else {
        CellBuilder::build_from(cs)?
    };
    build_normalized_external_message(&info.dst, body)
}
pub fn build_normalized_external_message(
    dst: &IntAddr,
    body: Cell,
) -> Result<Cell, Error> {
    let cx = Cell::empty_context();
    let mut b = CellBuilder::new();
    b.store_small_uint(0b1000, 4)?;
    dst.store_into(&mut b, cx)?;
    b.store_small_uint(0b000001, 6)?;
    b.store_reference(body)?;
    b.build_ext(cx)
}
pub struct ExtMsgRepr;
impl ExtMsgRepr {
    pub const MAX_BOC_SIZE: usize = 65535;
    pub const MAX_REPR_DEPTH: u16 = 512;
    pub const MAX_ALLOWED_MERKLE_DEPTH: u8 = 2;
    pub const MAX_MSG_BITS: u64 = 1 << 21;
    pub const MAX_MSG_CELLS: u64 = 1 << 13;
    pub const BOUNDARY_BOC_SIZE: usize = 1 << 12;
    pub fn validate<T: AsRef<[u8]>>(bytes: T) -> Result<Cell, InvalidExtMsg> {
        if bytes.as_ref().len() > Self::MAX_BOC_SIZE {
            return Err(InvalidExtMsg::BocSizeExceeded);
        }
        let msg_root = Self::boc_decode_with_limit(bytes.as_ref(), Self::MAX_MSG_CELLS)?;
        if msg_root.level() != 0 {
            return Err(InvalidExtMsg::TooBigLevel);
        }
        if msg_root.repr_depth() > Self::MAX_REPR_DEPTH {
            return Err(InvalidExtMsg::DepthExceeded);
        }
        if msg_root.is_exotic() {
            return Err(InvalidExtMsg::InvalidMessage(Error::InvalidData));
        }
        let mut cs = msg_root.as_slice_allow_exotic();
        'info: {
            if MsgType::load_from(&mut cs)? == MsgType::ExtIn {
                let info = ExtInMsgInfo::load_from(&mut cs)?;
                if let IntAddr::Std(std_addr) = &info.dst && std_addr.anycast.is_none() {
                    break 'info;
                }
            }
            return Err(InvalidExtMsg::InvalidMessage(Error::InvalidData));
        }
        if !MsgStorageStat::check_slice(
            &cs,
            Self::MAX_ALLOWED_MERKLE_DEPTH,
            CellTreeStats {
                bit_count: Self::MAX_MSG_BITS,
                cell_count: Self::MAX_MSG_CELLS,
            },
        ) {
            return Err(InvalidExtMsg::MsgSizeExceeded);
        }
        if cs.load_bit()? {
            if cs.load_bit()? {
                cs.load_reference()
                    .and_then(|c| {
                        let mut cs = c.as_slice()?;
                        StateInit::load_from(&mut cs)?;
                        if cs.is_empty() { Ok(()) } else { Err(Error::InvalidData) }
                    })?;
            } else {
                StateInit::load_from(&mut cs)?;
            }
        }
        if cs.load_bit()? {
            if !cs.is_data_empty() || cs.size_refs() != 1 {
                return Err(InvalidExtMsg::InvalidMessage(Error::InvalidData));
            }
        }
        Ok(msg_root)
    }
    fn boc_decode_with_limit(
        data: &[u8],
        max_cells: u64,
    ) -> Result<Cell, InvalidExtMsg> {
        use tycho_types::boc::de::{self, Options};
        let header = tycho_types::boc::de::BocHeader::decode(
            data,
            &Options {
                max_roots: Some(1),
                min_roots: Some(1),
            },
        )?;
        if header.cells().len() as u64 > max_cells {
            return Err(InvalidExtMsg::MsgSizeExceeded);
        }
        if let Some(&root) = header.roots().first() {
            let cells = header.finalize(Cell::empty_context())?;
            if let Some(root) = cells.get(root) {
                return Ok(root);
            }
        }
        Err(InvalidExtMsg::BocError(de::Error::RootCellNotFound))
    }
}
#[derive(Debug, thiserror::Error)]
pub enum InvalidExtMsg {
    #[error("BOC size exceeds maximum allowed size")]
    BocSizeExceeded,
    #[error("invalid message BOC")]
    BocError(#[from] tycho_types::boc::de::Error),
    #[error("too big root cell level")]
    TooBigLevel,
    #[error("max cell repr depth exceeded")]
    DepthExceeded,
    #[error("invalid message")]
    InvalidMessage(#[from] Error),
    #[error("message size limits exceeded")]
    MsgSizeExceeded,
}
pub struct MsgStorageStat<'a> {
    visited: &'a mut FastHashMap<&'static HashBytes, u8>,
    limits: CellTreeStats,
    max_merkle_depth: u8,
    cells: u64,
    bits: u64,
}
impl<'a> MsgStorageStat<'a> {
    thread_local! {
        #[doc = " Storage to reuse for parsing messages."] static VISITED_CELLS : RefCell
        < FastHashMap <&'static HashBytes, u8 >> =
        RefCell::new(FastHashMap::with_capacity_and_hasher(128, Default::default()),);
    }
    pub fn check_slice<'c: 'a>(
        cs: &CellSlice<'c>,
        max_merkle_depth: u8,
        limits: CellTreeStats,
    ) -> bool {
        MsgStorageStat::VISITED_CELLS
            .with_borrow_mut(|visited| {
                let res = unsafe {
                    MsgStorageStat::check_slice_impl(
                        visited,
                        cs,
                        max_merkle_depth,
                        limits,
                    )
                };
                visited.clear();
                res
            })
    }
    /// # Safety
    ///
    /// The following must be true:
    /// - `visited` must be empty;
    /// - `visited` must be cleared right after this call.
    unsafe fn check_slice_impl(
        visited: &'a mut FastHashMap<&'static HashBytes, u8>,
        cs: &CellSlice<'_>,
        max_merkle_depth: u8,
        limits: CellTreeStats,
    ) -> bool {
        debug_assert!(visited.is_empty());
        let mut state = Self {
            visited,
            limits,
            max_merkle_depth,
            cells: 1,
            bits: cs.size_bits() as u64,
        };
        for cell in cs.references() {
            if unsafe { state.add_cell(cell) }.is_none() {
                return false;
            }
        }
        true
    }
    unsafe fn add_cell(&mut self, cell: &DynCell) -> Option<u8> {
        if let Some(merkle_depth) = self.visited.get(cell.repr_hash()) {
            return Some(*merkle_depth);
        }
        self.cells = self.cells.checked_add(1)?;
        self.bits = self.bits.checked_add(cell.bit_len() as u64)?;
        if self.cells > self.limits.cell_count || self.bits > self.limits.bit_count {
            return None;
        }
        let mut max_merkle_depth = 0u8;
        for cell in cell.references() {
            max_merkle_depth = std::cmp::max(
                unsafe { self.add_cell(cell)? },
                max_merkle_depth,
            );
        }
        max_merkle_depth = max_merkle_depth
            .saturating_add(cell.cell_type().is_merkle() as u8);
        self.visited
            .insert(
                unsafe {
                    std::mem::transmute::<
                        &HashBytes,
                        &'static HashBytes,
                    >(cell.repr_hash())
                },
                max_merkle_depth,
            );
        (max_merkle_depth <= self.max_merkle_depth).then_some(max_merkle_depth)
    }
}
#[cfg(test)]
mod test {
    use tycho_types::error::Error;
    use tycho_types::merkle::MerkleProof;
    use tycho_types::models::{
        ExtOutMsgInfo, IntMsgInfo, MessageLayout, MsgInfo, OwnedMessage,
    };
    use super::*;
    use crate::block::AlwaysInclude;
    #[test]
    fn fits_into_limits() -> anyhow::Result<()> {
        #[track_caller]
        fn unwrap_msg(cell: Cell) {
            let boc = Boc::encode(cell);
            ExtMsgRepr::validate(boc).unwrap();
        }
        unwrap_msg(
            CellBuilder::build_from(OwnedMessage {
                info: MsgInfo::ExtIn(Default::default()),
                init: None,
                body: Default::default(),
                layout: None,
            })?,
        );
        unwrap_msg({
            let mut count = 0;
            let body = make_big_tree(
                8,
                &mut count,
                ExtMsgRepr::MAX_MSG_CELLS as u16 - 100,
            );
            println!("{count}");
            CellBuilder::build_from(OwnedMessage {
                info: MsgInfo::ExtIn(Default::default()),
                init: None,
                body: body.into(),
                layout: None,
            })?
        });
        unwrap_msg({
            let leaf_proof = MerkleProof::create(Cell::empty_cell_ref(), AlwaysInclude)
                .build()
                .and_then(CellBuilder::build_from)?;
            let body = MerkleProof::create(leaf_proof.as_ref(), AlwaysInclude)
                .build()
                .and_then(CellBuilder::build_from)?;
            CellBuilder::build_from(OwnedMessage {
                info: MsgInfo::ExtIn(Default::default()),
                init: None,
                body: body.into(),
                layout: Some(MessageLayout {
                    body_to_cell: true,
                    init_to_cell: false,
                }),
            })?
        });
        Ok(())
    }
    #[test]
    fn dont_fit_into_limits() -> anyhow::Result<()> {
        #[track_caller]
        fn expect_err(cell: Cell) -> InvalidExtMsg {
            let boc = Boc::encode(cell);
            ExtMsgRepr::validate(boc).unwrap_err()
        }
        assert!(
            matches!(expect_err(Cell::empty_cell()),
            InvalidExtMsg::InvalidMessage(Error::CellUnderflow))
        );
        assert!(
            matches!(expect_err(CellBuilder::build_from(MerkleProof::default()) ?),
            InvalidExtMsg::InvalidMessage(Error::InvalidData))
        );
        {
            let mut cell = Cell::default();
            for _ in 0..520 {
                cell = CellBuilder::build_from(cell)?;
            }
            assert!(matches!(expect_err(cell), InvalidExtMsg::DepthExceeded));
        }
        {
            let cell = CellBuilder::build_from(OwnedMessage {
                info: MsgInfo::Int(IntMsgInfo::default()),
                init: None,
                body: Default::default(),
                layout: None,
            })?;
            assert!(
                matches!(expect_err(cell),
                InvalidExtMsg::InvalidMessage(Error::InvalidData))
            );
            let cell = CellBuilder::build_from(OwnedMessage {
                info: MsgInfo::ExtOut(ExtOutMsgInfo::default()),
                init: None,
                body: Default::default(),
                layout: None,
            })?;
            assert!(
                matches!(expect_err(cell),
                InvalidExtMsg::InvalidMessage(Error::InvalidData))
            );
        }
        {
            let mut b = CellBuilder::new();
            OwnedMessage {
                info: MsgInfo::ExtOut(ExtOutMsgInfo::default()),
                init: None,
                body: Default::default(),
                layout: Some(MessageLayout {
                    body_to_cell: true,
                    init_to_cell: false,
                }),
            }
                .store_into(&mut b, Cell::empty_context())?;
            assert!(
                matches!(expect_err({ let mut b = b.clone(); b.store_u16(123) ?; b
                .build() ? }), InvalidExtMsg::InvalidMessage(Error::InvalidData))
            );
            assert!(
                matches!(expect_err({ let mut b = b.clone(); b
                .store_reference(Cell::empty_cell()) ?; b.build() ? }),
                InvalidExtMsg::InvalidMessage(Error::InvalidData))
            );
            assert!(
                matches!(expect_err({ let mut b = b.clone(); b.store_u16(123) ?; b
                .store_reference(Cell::empty_cell()) ?; b.build() ? }),
                InvalidExtMsg::InvalidMessage(Error::InvalidData))
            );
        }
        {
            let cell = exceed_big_message()?;
            assert!(matches!(expect_err(cell), InvalidExtMsg::MsgSizeExceeded));
        }
        {
            let cell = create_deep_merkle()?;
            assert!(matches!(expect_err(cell), InvalidExtMsg::MsgSizeExceeded));
        }
        Ok(())
    }
    fn exceed_big_message() -> anyhow::Result<Cell> {
        let mut count = 0;
        let body = make_big_tree(8, &mut count, ExtMsgRepr::MAX_MSG_CELLS as u16 + 100);
        let cell = CellBuilder::build_from(OwnedMessage {
            info: MsgInfo::ExtIn(Default::default()),
            init: None,
            body: body.into(),
            layout: None,
        })?;
        Ok(cell)
    }
    fn create_deep_merkle() -> anyhow::Result<Cell> {
        let leaf_proof = MerkleProof::create(Cell::empty_cell_ref(), AlwaysInclude)
            .build()
            .and_then(CellBuilder::build_from)?;
        let inner_proof = MerkleProof::create(leaf_proof.as_ref(), AlwaysInclude)
            .build()
            .and_then(CellBuilder::build_from)?;
        let body = MerkleProof::create(inner_proof.as_ref(), AlwaysInclude)
            .build()
            .and_then(CellBuilder::build_from)?;
        let cell = CellBuilder::build_from(OwnedMessage {
            info: MsgInfo::ExtIn(Default::default()),
            init: None,
            body: body.into(),
            layout: Some(MessageLayout {
                body_to_cell: true,
                init_to_cell: false,
            }),
        })?;
        Ok(cell)
    }
    fn make_big_tree(depth: u8, count: &mut u16, target: u16) -> Cell {
        *count += 1;
        if depth == 0 {
            CellBuilder::build_from(*count).unwrap()
        } else {
            let mut b = CellBuilder::new();
            for _ in 0..4 {
                if *count < target {
                    b.store_reference(make_big_tree(depth - 1, count, target)).unwrap();
                }
            }
            b.build().unwrap()
        }
    }
}
