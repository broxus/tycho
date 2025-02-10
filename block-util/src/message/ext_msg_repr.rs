use std::cell::RefCell;

use everscale_types::cell::CellTreeStats;
use everscale_types::error::Error;
use everscale_types::models::{IntAddr, MsgInfo, StateInit};
use everscale_types::prelude::*;
use tycho_util::FastHashMap;

pub async fn validate_external_message(body: &bytes::Bytes) -> Result<(), InvalidExtMsg> {
    if body.len() > ExtMsgRepr::BOUNDARY_BOC_SIZE {
        let body = body.clone();
        tycho_util::sync::rayon_run_fifo(move || ExtMsgRepr::validate(&body)).await
    } else {
        ExtMsgRepr::validate(body)
    }
}

pub struct ExtMsgRepr;

impl ExtMsgRepr {
    pub const MAX_BOC_SIZE: usize = 65535;
    pub const MAX_REPR_DEPTH: u16 = 512;
    pub const MAX_ALLOWED_MERKLE_DEPTH: u8 = 2;
    pub const MAX_MSG_BITS: u64 = 1 << 21;
    pub const MAX_MSG_CELLS: u64 = 1 << 13;
    pub const BOUNDARY_BOC_SIZE: usize = 1 << 12;

    pub const ALLOWED_WORKCHAINS: std::ops::RangeInclusive<i8> = -1..=0;

    // === General methods ===

    pub fn validate<T: AsRef<[u8]>>(bytes: T) -> Result<(), InvalidExtMsg> {
        // Apply limits to the encoded BOC.
        if bytes.as_ref().len() > Self::MAX_BOC_SIZE {
            return Err(InvalidExtMsg::BocSizeExceeded);
        }

        // Decode BOC.
        let msg_root = Self::boc_decode_with_limit(bytes.as_ref(), Self::MAX_MSG_CELLS)?;

        // Cell must not contain any suspicious pruned branches not wrapped into merkle stuff.
        if msg_root.level() != 0 {
            return Err(InvalidExtMsg::TooBigLevel);
        }

        // Apply limits to the cell depth.
        if msg_root.repr_depth() > Self::MAX_REPR_DEPTH {
            return Err(InvalidExtMsg::DepthExceeded);
        }

        // External message must be an ordinary cell.
        if msg_root.is_exotic() {
            return Err(InvalidExtMsg::InvalidMessage(Error::InvalidData));
        }

        // Start parsing the message (we are sure now that it is an ordinary cell).
        let mut cs = msg_root.as_slice_allow_exotic();

        // Parse info first.
        let info = MsgInfo::load_from(&mut cs)?;

        'info: {
            // Only external inbound messages are allowed.
            if let MsgInfo::ExtIn(info) = &info {
                if let IntAddr::Std(std_addr) = &info.dst {
                    // Only `addr_std` (without anycast) to existing workchains is allowed.
                    if Self::ALLOWED_WORKCHAINS.contains(&std_addr.workchain)
                        && std_addr.anycast.is_none()
                    {
                        break 'info;
                    }
                }
            }

            // All other cases are considered garbage.
            return Err(InvalidExtMsg::InvalidMessage(Error::InvalidData));
        }

        // Check limits with the remaining slice.
        if !MsgStorageStat::check_slice(&cs, Self::MAX_ALLOWED_MERKLE_DEPTH, CellTreeStats {
            bit_count: Self::MAX_MSG_BITS,
            cell_count: Self::MAX_MSG_CELLS,
        }) {
            return Err(InvalidExtMsg::MsgSizeExceeded);
        }

        // Process message state init.
        if cs.load_bit()? {
            if cs.load_bit()? {
                // State init as reference.
                cs.load_reference().and_then(|c| {
                    let mut cs = c.as_slice()?;
                    StateInit::load_from(&mut cs)?;

                    // State init cell must not contain anything else.
                    if cs.is_empty() {
                        Ok(())
                    } else {
                        Err(Error::InvalidData)
                    }
                })?;
            } else {
                // Inline state init.
                StateInit::load_from(&mut cs)?;
            }
        }

        // Process message body.
        if cs.load_bit()? {
            // Message must not contain anything other than body as cell.
            if !cs.is_data_empty() || cs.size_refs() != 1 {
                return Err(InvalidExtMsg::InvalidMessage(Error::InvalidData));
            }
        }

        Ok(())
    }

    fn boc_decode_with_limit(data: &[u8], max_cells: u64) -> Result<Cell, InvalidExtMsg> {
        use everscale_types::boc::de::{self, Options};

        let header = everscale_types::boc::de::BocHeader::decode(data, &Options {
            max_roots: Some(1),
            min_roots: Some(1),
        })?;

        // Optimistic check based on just cell data ranges.
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
    BocError(#[from] everscale_types::boc::de::Error),
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
        /// Storage to reuse for parsing messages.
        static VISITED_CELLS: RefCell<FastHashMap<&'static HashBytes, u8>> = RefCell::new(
            FastHashMap::with_capacity_and_hasher(128, Default::default()),
        );
    }

    pub fn check_slice<'c: 'a>(
        cs: &CellSlice<'c>,
        max_merkle_depth: u8,
        limits: CellTreeStats,
    ) -> bool {
        MsgStorageStat::VISITED_CELLS.with_borrow_mut(|visited| {
            // SAFETY: We are clearing the `visited` map right after the call.
            let res =
                unsafe { MsgStorageStat::check_slice_impl(visited, cs, max_merkle_depth, limits) };
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
            if state.add_cell(cell).is_none() {
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
            max_merkle_depth = std::cmp::max(self.add_cell(cell)?, max_merkle_depth);
        }
        max_merkle_depth = max_merkle_depth.saturating_add(cell.cell_type().is_merkle() as u8);

        // SAFETY: `visited` must be cleared before dropping the original cell.
        self.visited.insert(
            std::mem::transmute::<&HashBytes, &'static HashBytes>(cell.repr_hash()),
            max_merkle_depth,
        );

        (max_merkle_depth <= self.max_merkle_depth).then_some(max_merkle_depth)
    }
}

#[cfg(test)]
mod test {
    use everscale_types::error::Error;
    use everscale_types::merkle::MerkleProof;
    use everscale_types::models::{ExtOutMsgInfo, IntMsgInfo, MessageLayout, OwnedMessage};

    use super::*;
    use crate::block::AlwaysInclude;

    #[test]
    fn fits_into_limits() -> anyhow::Result<()> {
        #[track_caller]
        fn unwrap_msg(cell: Cell) {
            let boc = Boc::encode(cell);
            ExtMsgRepr::validate(boc).unwrap();
        }

        // Simple message.
        unwrap_msg(CellBuilder::build_from(OwnedMessage {
            info: MsgInfo::ExtIn(Default::default()),
            init: None,
            body: Default::default(),
            layout: None,
        })?);

        // Big message.
        unwrap_msg({
            let mut count = 0;
            let body = make_big_tree(8, &mut count, ExtMsgRepr::MAX_MSG_CELLS as u16 - 100);
            println!("{count}");

            let body_range = CellSliceRange::full(body.as_ref());

            CellBuilder::build_from(OwnedMessage {
                info: MsgInfo::ExtIn(Default::default()),
                init: None,
                body: (body, body_range),
                layout: None,
            })?
        });

        // Close enough merkle depth.
        unwrap_msg({
            let leaf_proof = MerkleProof::create(Cell::empty_cell_ref(), AlwaysInclude)
                .build()
                .and_then(CellBuilder::build_from)?;

            let body = MerkleProof::create(leaf_proof.as_ref(), AlwaysInclude)
                .build()
                .and_then(CellBuilder::build_from)?;
            let body_range = CellSliceRange::full(body.as_ref());

            CellBuilder::build_from(OwnedMessage {
                info: MsgInfo::ExtIn(Default::default()),
                init: None,
                body: (body, body_range),
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

        // Garbage.
        assert!(matches!(
            expect_err(Cell::empty_cell()),
            InvalidExtMsg::InvalidMessage(Error::CellUnderflow)
        ));

        // Exotic cells.
        assert!(matches!(
            expect_err(CellBuilder::build_from(MerkleProof::default())?),
            InvalidExtMsg::InvalidMessage(Error::InvalidData)
        ));

        // Too deep cells tree.
        {
            let mut cell = Cell::default();
            for _ in 0..520 {
                cell = CellBuilder::build_from(cell)?;
            }
            assert!(matches!(expect_err(cell), InvalidExtMsg::DepthExceeded));
        }

        // Non-external message.
        {
            let cell = CellBuilder::build_from(OwnedMessage {
                info: MsgInfo::Int(IntMsgInfo::default()),
                init: None,
                body: Default::default(),
                layout: None,
            })?;
            assert!(matches!(
                expect_err(cell),
                InvalidExtMsg::InvalidMessage(Error::InvalidData)
            ));

            let cell = CellBuilder::build_from(OwnedMessage {
                info: MsgInfo::ExtOut(ExtOutMsgInfo::default()),
                init: None,
                body: Default::default(),
                layout: None,
            })?;
            assert!(matches!(
                expect_err(cell),
                InvalidExtMsg::InvalidMessage(Error::InvalidData)
            ));
        }

        // External message with extra data.
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

            // Bits
            assert!(matches!(
                expect_err({
                    let mut b = b.clone();
                    b.store_u16(123)?;
                    b.build()?
                }),
                InvalidExtMsg::InvalidMessage(Error::InvalidData)
            ));

            // Refs
            assert!(matches!(
                expect_err({
                    let mut b = b.clone();
                    b.store_reference(Cell::empty_cell())?;
                    b.build()?
                }),
                InvalidExtMsg::InvalidMessage(Error::InvalidData)
            ));

            // Both
            assert!(matches!(
                expect_err({
                    let mut b = b.clone();
                    b.store_u16(123)?;
                    b.store_reference(Cell::empty_cell())?;
                    b.build()?
                }),
                InvalidExtMsg::InvalidMessage(Error::InvalidData)
            ));
        }

        // Too big message.
        {
            let cell = exceed_big_message()?;
            assert!(matches!(expect_err(cell), InvalidExtMsg::MsgSizeExceeded));
        }

        // Too big merkle depth.
        {
            let cell = create_deep_merkle()?;
            assert!(matches!(expect_err(cell), InvalidExtMsg::MsgSizeExceeded));
        }

        Ok(())
    }

    fn exceed_big_message() -> anyhow::Result<Cell> {
        let mut count = 0;
        let body = make_big_tree(8, &mut count, ExtMsgRepr::MAX_MSG_CELLS as u16 + 100);

        let body_range = CellSliceRange::full(body.as_ref());

        let cell = CellBuilder::build_from(OwnedMessage {
            info: MsgInfo::ExtIn(Default::default()),
            init: None,
            body: (body, body_range),
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
        let body_range = CellSliceRange::full(body.as_ref());

        let cell = CellBuilder::build_from(OwnedMessage {
            info: MsgInfo::ExtIn(Default::default()),
            init: None,
            body: (body, body_range),
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
                    b.store_reference(make_big_tree(depth - 1, count, target))
                        .unwrap();
                }
            }
            b.build().unwrap()
        }
    }
}
