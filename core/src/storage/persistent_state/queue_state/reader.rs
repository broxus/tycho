use anyhow::{Context, Result};
use tl_proto::TlRead;
use tycho_block_util::queue::{QueueDiff, QueueStateHeader, QueueStateRef};
use tycho_types::boc::de::ProcessedCells;
use tycho_types::cell::{Cell, CellFamily};
use tycho_types::models::OutMsgQueueUpdates;

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
        let state =
            QueueStateRef::<'a>::read_from(packet).context("failed to init QueueStateRef")?;
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

        anyhow::ensure!(
            state.header.queue_diffs.len() == top_update.tail_len as usize,
            "queue diffs count mismatch"
        );

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

    pub fn read_next_queue_diff(&mut self) -> Result<Option<QueueDiffReader<'_>>> {
        let Some(queue_diff) = self.state.header.queue_diffs.get(self.queue_diff_index) else {
            anyhow::ensure!(self.parsed_boc.is_none(), "too many messages");
            return Ok(None);
        };

        Ok(Some(QueueDiffReader {
            state: &self.state,
            queue_diff,
            next_queue_diff_index: self.queue_diff_index + 1,
            queue_diff_index: &mut self.queue_diff_index,
            message_index: &mut self.message_index,
            boc_index: &mut self.boc_index,
            parsed_boc: &mut self.parsed_boc,
        }))
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

pub struct QueueDiffReader<'a> {
    state: &'a QueueStateRef<'a>,
    queue_diff: &'a QueueDiff,
    next_queue_diff_index: usize,
    queue_diff_index: &'a mut usize,
    message_index: &'a mut usize,
    boc_index: &'a mut usize,
    parsed_boc: &'a mut Option<ParsedBoc>,
}

impl<'a> QueueDiffReader<'a> {
    pub fn queue_diff(&self) -> &'a QueueDiff {
        self.queue_diff
    }

    pub fn read_next_message(&mut self) -> Result<Option<Cell>> {
        use tycho_types::boc::de;

        const MAX_ALLOWED_ROOTS_PER_CHUNK: usize = 10000;

        let Some(expected_hash) = self.queue_diff.messages.get(*self.message_index) else {
            // Move to the next queue diff
            *self.queue_diff_index = self.next_queue_diff_index;
            *self.message_index = 0;
            return Ok(None);
        };

        loop {
            if let Some(boc) = &mut self.parsed_boc {
                if let Some(cell) = boc.next() {
                    if boc.roots.is_empty() {
                        *self.parsed_boc = None;
                        *self.boc_index += 1;
                    }

                    *self.message_index += 1;
                    anyhow::ensure!(cell.repr_hash() == expected_hash, "message hash mismatch");

                    return Ok(Some(cell));
                }
            }

            let Some(data) = self.state.messages.get(*self.boc_index) else {
                anyhow::bail!("not enough messages");
            };
            let boc = de::BocHeader::decode(data, &de::Options {
                min_roots: None,
                // NOTE: We must specify the max number of roots to avoid the default
                //       limit (which is quite low since it is rarely used in practice).
                max_roots: Some(MAX_ALLOWED_ROOTS_PER_CHUNK),
            })?;

            let mut roots = boc.roots().to_vec();
            let cells = boc.finalize(Cell::empty_context())?;

            // NOTE: Reverse root indices here to allow the `ParsedBoc` iterator to just pop them.
            roots.reverse();
            *self.parsed_boc = Some(ParsedBoc { roots, cells });
        }
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
