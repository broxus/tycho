use everscale_types::cell::{Cell, CellBuilder};
use everscale_types::models::{MsgInfo, OwnedMessage};
use tycho_block_util::message::ExtMsgRepr;

pub fn create_big_message() -> anyhow::Result<Cell> {
    let mut count = 0;
    let body = make_big_tree(8, &mut count, ExtMsgRepr::MAX_MSG_CELLS as u16 - 100);

    let cell = CellBuilder::build_from(OwnedMessage {
        info: MsgInfo::ExtIn(Default::default()),
        init: None,
        body: body.into(),
        layout: None,
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
