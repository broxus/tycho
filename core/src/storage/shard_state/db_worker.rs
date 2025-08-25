use tycho_storage::kv::refcount;
use tycho_types::cell::HashBytes;
use weedb::rocksdb::WriteBatch;

use crate::storage::CoreDb;
use crate::storage::shard_state::cell_storage::CellStorageError;

#[derive(Clone)]
pub struct DbWorker {
    pub db: CoreDb,
    pub command_rx: crossbeam_channel::Receiver<DbCommand>,
}

impl DbWorker {
    pub fn run(self) {
        while let Ok(command) = self.command_rx.recv() {
            match command {
                DbCommand::WriteBatch { data, response_tx } => {
                    let before_sn = self.db.rocksdb().latest_sequence_number();

                    let batch = WriteBatch::from_data(&data);

                    let result = self
                        .db
                        .rocksdb()
                        .write(batch)
                        .map_err(CellStorageError::Internal);

                    let after_sn = self.db.rocksdb().latest_sequence_number();

                    response_tx
                        .send(DbResponse::WriteBatch {
                            before_sn,
                            after_sn,
                            result,
                        })
                        .ok();
                }
                DbCommand::GetRcForInsert { key, response_tx } => {
                    let sn = self.db.rocksdb().latest_sequence_number();

                    let result = match self.db.cells.get(key.as_slice()) {
                        Ok(Some(value)) => {
                            let (rc, value) = refcount::decode_value_with_rc(value.as_ref());

                            // TODO: lower to `debug_assert` when sure
                            let has_value = value.is_some();
                            assert!(has_value && rc > 0 || !has_value && rc == 0);

                            Ok(rc)
                        }
                        Ok(None) => Ok(0),
                        Err(e) => Err(CellStorageError::Internal(e)),
                    };

                    response_tx
                        .send(DbResponse::GetRcForInsert { sn, result })
                        .ok();
                }
                DbCommand::GetRcForDelete { key, response_tx } => {
                    // let sn = self.db.rocksdb().latest_sequence_number();

                    let result = match self.db.cells.get(key.as_slice()) {
                        Ok(value) => {
                            if let Some(value) = value
                                && let (rc, Some(value)) = refcount::decode_value_with_rc(&value)
                            {
                                Ok((rc, value.to_vec()))
                            } else {
                                Err(CellStorageError::CellNotFound)
                            }
                        }
                        Err(e) => Err(CellStorageError::Internal(e)),
                    };

                    response_tx.send(DbResponse::GetRcForDelete { result }).ok();
                }
                DbCommand::GetRc { key, response_tx } => {
                    let sn = self.db.rocksdb().latest_sequence_number();

                    let result = match self.db.cells.get(key) {
                        Ok(Some(value)) => {
                            let (rc, _value) = refcount::decode_value_with_rc(value.as_ref());
                            Ok(rc)
                        }
                        Ok(None) => Ok(0),
                        Err(e) => Err(CellStorageError::Internal(e)),
                    };

                    response_tx.send(DbResponse::GetRc { sn, result }).ok();
                }
                DbCommand::GetRaw { key, response_tx } => {
                    // let sn = self.db.rocksdb().latest_sequence_number();

                    let result = self
                        .db
                        .cells
                        .get(key.as_slice())
                        .map(|value| {
                            if let Some(value) = value {
                                let (_, data) = refcount::decode_value_with_rc(value.as_ref());
                                data.map(|value| value.to_vec())
                            } else {
                                None
                            }
                        })
                        .map_err(CellStorageError::Internal);

                    response_tx.send(DbResponse::GetRaw { result }).ok();
                }
            }
        }
    }
}

pub enum DbCommand {
    WriteBatch {
        data: Vec<u8>,
        response_tx: crossbeam_channel::Sender<DbResponse>,
    },
    GetRcForInsert {
        key: HashBytes,
        response_tx: crossbeam_channel::Sender<DbResponse>,
    },
    GetRcForDelete {
        key: HashBytes,
        response_tx: crossbeam_channel::Sender<DbResponse>,
    },
    GetRc {
        key: HashBytes,
        response_tx: crossbeam_channel::Sender<DbResponse>,
    },
    GetRaw {
        key: HashBytes,
        response_tx: crossbeam_channel::Sender<DbResponse>,
    },
}

pub enum DbResponse {
    WriteBatch {
        before_sn: u64,
        after_sn: u64,
        result: Result<(), CellStorageError>,
    },
    GetRcForInsert {
        sn: u64,
        result: Result<i64, CellStorageError>,
    },
    GetRcForDelete {
        result: Result<(i64, Vec<u8>), CellStorageError>,
    },
    GetRc {
        sn: u64,
        result: Result<i64, CellStorageError>,
    },
    GetRaw {
        result: Result<Option<Vec<u8>>, CellStorageError>,
    },
}

#[derive(Clone)]
pub struct DbHandle {
    pub command_tx: crossbeam_channel::Sender<DbCommand>,
}

impl DbHandle {
    pub fn write_batch(&self, batch: WriteBatch) -> Result<(u64, u64), anyhow::Error> {
        // oneshot
        let (response_tx, response_rx) = crossbeam_channel::bounded(1);

        self.command_tx.send(DbCommand::WriteBatch {
            data: batch.data().to_vec(),
            response_tx,
        })?;

        match response_rx.recv()? {
            DbResponse::WriteBatch {
                before_sn,
                after_sn,
                result,
            } => {
                result?;
                Ok((before_sn, after_sn))
            }
            _ => unreachable!(),
        }
    }

    pub fn get_rc_for_insert(&self, key: HashBytes) -> Result<(i64, u64), anyhow::Error> {
        // oneshot
        let (response_tx, response_rx) = crossbeam_channel::bounded(1);

        self.command_tx
            .send(DbCommand::GetRcForInsert { key, response_tx })?;

        match response_rx.recv()? {
            DbResponse::GetRcForInsert { sn, result } => {
                let rc = result?;
                Ok((rc, sn))
            }
            _ => unreachable!(),
        }
    }

    pub fn get_rc_for_delete(&self, key: HashBytes) -> Result<(i64, Vec<u8>), anyhow::Error> {
        // oneshot
        let (response_tx, response_rx) = crossbeam_channel::bounded(1);

        self.command_tx
            .send(DbCommand::GetRcForDelete { key, response_tx })?;

        match response_rx.recv()? {
            DbResponse::GetRcForDelete { result } => Ok(result?),
            _ => unreachable!(),
        }
    }

    pub fn get_rc(&self, key: HashBytes) -> Result<(i64, u64), anyhow::Error> {
        // oneshot
        let (response_tx, response_rx) = crossbeam_channel::bounded(1);

        self.command_tx
            .send(DbCommand::GetRc { key, response_tx })?;

        match response_rx.recv()? {
            DbResponse::GetRc { sn, result } => {
                let rc = result?;
                Ok((rc, sn))
            }
            _ => unreachable!(),
        }
    }

    pub fn get_raw(&self, key: HashBytes) -> Result<Option<Vec<u8>>, anyhow::Error> {
        // oneshot
        let (response_tx, response_rx) = crossbeam_channel::bounded(1);

        self.command_tx
            .send(DbCommand::GetRaw { key, response_tx })?;

        match response_rx.recv()? {
            DbResponse::GetRaw { result } => Ok(result?),
            _ => unreachable!(),
        }
    }
}
