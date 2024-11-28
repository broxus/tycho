use anyhow::Result;
use everscale_types::boc::de::ProcessedCells;
use everscale_types::cell::{Cell, CellFamily};
use everscale_types::models::OutMsgQueueUpdates;
use tl_proto::TlRead;
use tycho_block_util::queue::{QueueStateHeader, QueueStateRef};

pub struct QueueStateReader<'a> {
    state: QueueStateRef<'a>,
    queue_diff_index: usize,
    message_index: usize,
    boc_index: usize,
    parsed_boc: Option<ParsedBoc>,
}

impl<'a> QueueStateReader<'a> {
    pub fn begin_from_mapped(data: &'a [u8], top_update: &OutMsgQueueUpdates) -> Result<Self> {
        let packet = &mut std::convert::identity(data);
        let state = QueueStateRef::<'a>::read_from(packet)?;
        // TODO: Does this check really needed?
        anyhow::ensure!(packet.is_empty(), "data not fully read");

        let Some(top_queue_diff) = state.header.queue_diffs.first() else {
            // NOTE: It is also checked by the TL
            anyhow::bail!("no queue diffs in state");
        };

        anyhow::ensure!(
            top_queue_diff.hash == top_update.diff_hash,
            "top queue diff hash mismatch"
        );

        // TODO: Check `queue_diffs` length (https://github.com/broxus/tycho/issues/358)

        Ok(Self {
            state,
            queue_diff_index: 0,
            message_index: 0,
            boc_index: 0,
            parsed_boc: None,
        })
    }

    pub fn state(&self) -> &QueueStateRef<'_> {
        &self.state
    }

    pub fn header(&self) -> &QueueStateHeader {
        &self.state.header
    }

    pub fn read_next_message(&mut self) -> Result<Option<Cell>> {
        use everscale_types::boc::de;

        const MAX_ALLOWED_ROOTS_PER_CHUNK: usize = 10000;

        loop {
            let Some(queue_diff) = self.state.header.queue_diffs.get(self.queue_diff_index) else {
                anyhow::ensure!(self.parsed_boc.is_none(), "too many messages");
                return Ok(None);
            };

            let Some(expected_hash) = queue_diff.messages.get(self.message_index) else {
                // Move to the queue diff
                self.queue_diff_index += 1;
                self.message_index = 0;
                continue;
            };

            loop {
                if let Some(boc) = &mut self.parsed_boc {
                    if let Some(cell) = boc.next() {
                        if boc.roots.is_empty() {
                            self.parsed_boc = None;
                            self.boc_index += 1;
                        }

                        self.message_index += 1;
                        anyhow::ensure!(cell.repr_hash() == expected_hash, "message hash mismatch");
                        return Ok(Some(cell));
                    }
                }

                let Some(data) = self.state.messages.get(self.boc_index) else {
                    anyhow::bail!("not enough messages");
                };
                let boc = de::BocHeader::decode(data, &de::Options {
                    min_roots: None,
                    // NOTE: We must specify the max number of roots to avoid the default
                    //       limit (which is quite low since it is rarely used in practice).
                    max_roots: Some(MAX_ALLOWED_ROOTS_PER_CHUNK),
                })?;

                let mut roots = boc.roots().to_vec();
                let cells = boc.finalize(&mut Cell::empty_context())?;

                // NOTE: Reverse root indices here to allow the `ParsedBoc` iterator to just pop them.
                roots.reverse();
                self.parsed_boc = Some(ParsedBoc { roots, cells });
            }
        }
    }

    // TODO: Is this needed? We might want to stop a bit earlier,
    //       but at the same time we are reusing the downloaded state
    //       and it should be minimal.
    pub fn finish(self) -> Result<()> {
        let queue_diffs = &self.state.header.queue_diffs;
        anyhow::ensure!(
            self.queue_diff_index == queue_diffs.len() || self.message_index != 0,
            "not all queue diffs read"
        );
        anyhow::ensure!(self.parsed_boc.is_none(), "too many messages");
        Ok(())
    }
}

struct ParsedBoc {
    roots: Vec<u32>,
    cells: ProcessedCells,
}

impl Iterator for ParsedBoc {
    type Item = Cell;

    fn next(&mut self) -> Option<Self::Item> {
        let index = self.roots.pop()?;
        self.cells.get(index)
    }
}
