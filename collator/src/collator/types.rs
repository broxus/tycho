use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use anyhow::{anyhow, bail, Result};
use everscale_types::cell::{Cell, HashBytes, UsageTree, UsageTreeMode};
use everscale_types::dict::{AugDict, Dict};
use everscale_types::models::{
    Account, AccountBlock, AccountState, BlockId, BlockIdShort, BlockInfo, BlockRef,
    BlockchainConfig, CurrencyCollection, HashUpdate, ImportFees, InMsg, Lazy, LibDescr,
    McStateExtra, MsgInfo, OptionalAccount, OutMsg, PrevBlockRef, ProcessedUptoInfo, ShardAccount,
    ShardAccounts, ShardDescription, ShardFeeCreated, ShardFees, ShardIdent, ShardIdentFull,
    SimpleLib, SpecialFlags, StateInit, Transaction, ValueFlow,
};
use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};
use tycho_util::FastHashMap;

use crate::mempool::MempoolAnchor;
use crate::types::ProofFunds;

// В текущем коллаторе перед коллацией блока импортируется:
// - предыдущий мастер стейт
// - предыдущие стейты шарды (их может быть 2, если мерж)
// ImportedData {
// mc_state: Arc<ShardStateStuff>,
// prev_states: Vec<Arc<ShardStateStuff>>,
// prev_ext_blocks_refs: Vec<ExtBlkRef>,
// top_shard_blocks_descr: Vec<Arc<TopBlockDescrStuff>>,
// }
// top_shard_blocks_descr - список верхних новых шардблоков с последнего мастера, если будем коллировать мастер
// берутся из prev_states
// prev_ext_blocks_refs - ссылки на предыдущие шард блоки, на момент которых загружаются стейты шарды,
// они берутся на основании prev_blocks_ids коллатора, загружаются вместе с prev_states
// для мастерчейна выполняется проверка на номер блока (надо в ней разобраться)
//
// Что входит в стейт шарды
// ShardStateStuff {
// block_id: BlockId,
// shard_state: Option<ShardStateUnsplit>,
// out_msg_queue: Option<ShardStateUnsplit>,
// out_msg_queue_for: i32,
// shard_state_extra: Option<McStateExtra>,
// root: Cell
// }
//
// Затем из этих данных методом prepare_data() готовится: McData, PrevData и CollatorData
// pub struct McData {
// mc_state_extra: McStateExtra,
// prev_key_block_seqno: u32,
// prev_key_block: Option<BlockId>,
// state: Arc<ShardStateStuff>
// }
// pub struct PrevData {
// states: Vec<Arc<ShardStateStuff>>, // предыдущие стейты с отслеживанием изменений через UsageTree
// pure_states: Vec<Arc<ShardStateStuff>>, // исходные предыдущие стейты шарды без отслеживания посещений
// state_root: Cell,   // рутовая ячейка предыдущего стейта шарды (при мерже там будет объединенная ячейка из двух шард)
// без отслеживания изменений
// accounts: ShardAccounts,    // предыдущие аккаунты шарды с отслеживанием (получены с учетом сплита/мержа)
// gen_utime: u32,
// gen_lt: u64,
// total_validator_fees: CurrencyCollection,
// overload_history: u64,
// underload_history: u64,
// state_copyleft_rewards: CopyleftRewards,
// }
// pub struct CollatorData {
// usage_tree: UsageTree, // дерево посещенный ячеек стейта для вычисления меркл пруфа
// }
//
// Далее при коллации
// При инициализации ExecutionManager стейты, ячейки и аккаунты не используются
// При создании tick-tock транзакций McData используется для получения ИД контракта с конфигом и чтения конфига консенсуса
// В коллацию интерналов McData не передается, передается PrevData и CollatorData
//
// PrevData и CollatorData передаются в execute. Там берется аккаунт из PrevData и передается в таску выполнения сообщения.
// Там из аккаунта берется рутовая ячейка и передается в метод выполнения сообщения, где она изменяется.
// Затем подменяется рут в аккаунте, а предыдущее состояние аккаунта сохраняется в prev_account_stuff,
// то есть изменяемый аккаунт накапливает историю изменений
// При завершении таски она возвращает актуальный обновленный аккаунт - это происходит при финализации блока
//
// В методе финализации блока
// - запоминаем аккаунты из предыдущего стейта в new_accounts
// - берем все измененные аккаунты shard_acc и перименяем их изменения в стейт аккаунтов new_accounts
// - из измененного аккаунта делаем AccountBlock и сохраняем в accounts, если в нем есть транзакции
// - так же кладем измененный аккаунт shard_acc в список changed_accounts
// - создаем новый стейт шарды new_state с использованием обновленных аккаунтов new_accounts
// - из нового стейта делаем новую рут ячейку new_ss_root
// - вычисляем меркл апдейты
// - завершаем создание блока с использованием accounts с транзакциями
//
// Метод коллации блока возвращает новый стейт шарды типа ShardStateUnsplit
// из него можно собрать новый ShardStateStuff, который может использоваться для дальнейшей коллации

pub(super) struct WorkingState {
    pub mc_data: McData,
    pub prev_shard_data: PrevData,
    pub usage_tree: UsageTree,
    pub has_pending_internals: Option<bool>,
}

pub(super) struct McData {
    global_id: i32,
    mc_state_extra: McStateExtra,
    prev_key_block_seqno: u32,
    // TODO: remove if we do not need this
    _prev_key_block: Option<BlockId>,
    mc_state_stuff: ShardStateStuff,
}

impl McData {
    pub fn build(mc_state_stuff: ShardStateStuff) -> Result<Self> {
        let mc_state_extra = mc_state_stuff.state_extra()?;

        // prev key block
        let (prev_key_block_seqno, prev_key_block) = if mc_state_extra.after_key_block {
            (
                mc_state_stuff.block_id().seqno,
                Some(*mc_state_stuff.block_id()),
            )
        } else if let Some(block_ref) = mc_state_extra.last_key_block.as_ref() {
            (
                block_ref.seqno,
                Some(BlockId {
                    shard: ShardIdent::MASTERCHAIN,
                    seqno: block_ref.seqno,
                    root_hash: block_ref.root_hash,
                    file_hash: block_ref.file_hash,
                }),
            )
        } else {
            (0, None)
        };

        Ok(Self {
            global_id: mc_state_stuff.state().global_id,
            mc_state_extra: mc_state_extra.clone(),
            _prev_key_block: prev_key_block,
            prev_key_block_seqno,
            mc_state_stuff,
        })
    }

    pub fn global_id(&self) -> i32 {
        self.global_id
    }

    pub fn prev_key_block_seqno(&self) -> u32 {
        self.prev_key_block_seqno
    }

    pub fn mc_state_stuff(&self) -> ShardStateStuff {
        self.mc_state_stuff.clone()
    }

    pub fn mc_state_extra(&self) -> &McStateExtra {
        &self.mc_state_extra
    }

    pub fn get_master_ref(&self) -> BlockRef {
        let end_lt = self.mc_state_stuff.state().gen_lt;
        let block_id = self.mc_state_stuff.block_id();
        BlockRef {
            end_lt,
            seqno: block_id.seqno,
            root_hash: block_id.root_hash,
            file_hash: block_id.file_hash,
        }
    }

    pub fn config(&self) -> &BlockchainConfig {
        &self.mc_state_extra.config
    }

    pub fn libraries(&self) -> &Dict<HashBytes, LibDescr> {
        &self.mc_state_stuff.state().libraries
    }

    pub fn get_lt_align(&self) -> u64 {
        1000000
    }
}

pub(super) struct PrevData {
    observable_states: Vec<ShardStateStuff>,
    observable_accounts: ShardAccounts,

    blocks_ids: Vec<BlockId>,

    pure_states: Vec<ShardStateStuff>,
    pure_state_root: Cell,

    gen_chain_time: u32,
    gen_lt: u64,
    total_validator_fees: CurrencyCollection,
    // TODO: remove if we do not need this
    _overload_history: u64,
    _underload_history: u64,

    processed_upto: ProcessedUptoInfo,
}

impl PrevData {
    pub fn build(
        prev_states: Vec<ShardStateStuff>,
        state_tracker: &MinRefMcStateTracker,
    ) -> Result<(Self, UsageTree)> {
        // TODO: make real implementation
        // consider split/merge logic
        //  Collator::prepare_data()
        //  Collator::unpack_last_state()

        let prev_blocks_ids: Vec<_> = prev_states.iter().map(|s| *s.block_id()).collect();
        let pure_prev_state_root = prev_states[0].root_cell();
        let pure_prev_states = prev_states.clone();

        let usage_tree = UsageTree::new(UsageTreeMode::OnDataAccess);
        let observable_root = usage_tree.track(pure_prev_state_root);
        let observable_states = vec![ShardStateStuff::from_root(
            pure_prev_states[0].block_id(),
            observable_root,
            state_tracker,
        )?];

        let gen_utime = observable_states[0].state().gen_utime;
        let gen_lt = observable_states[0].state().gen_lt;
        let observable_accounts = observable_states[0].state().load_accounts()?;
        let total_validator_fees = observable_states[0].state().total_validator_fees.clone();
        let overload_history = observable_states[0].state().overload_history;
        let underload_history = observable_states[0].state().underload_history;
        let processed_upto = pure_prev_states[0].state().processed_upto.load()?;

        let prev_data = Self {
            observable_states,
            observable_accounts,

            blocks_ids: prev_blocks_ids,

            pure_states: pure_prev_states,
            pure_state_root: pure_prev_state_root.clone(),

            gen_chain_time: gen_utime,
            gen_lt,
            total_validator_fees,
            _overload_history: overload_history,
            _underload_history: underload_history,

            processed_upto,
        };

        Ok((prev_data, usage_tree))
    }

    pub fn observable_states(&self) -> &Vec<ShardStateStuff> {
        &self.observable_states
    }

    pub fn observable_accounts(&self) -> &ShardAccounts {
        &self.observable_accounts
    }

    pub fn blocks_ids(&self) -> &Vec<BlockId> {
        &self.blocks_ids
    }

    pub fn get_blocks_ref(&self) -> Result<PrevBlockRef> {
        if self.pure_states.is_empty() || self.pure_states.len() > 2 {
            bail!(
                "There should be 1 or 2 prev states. Actual count is {}",
                self.pure_states.len()
            )
        }

        let mut block_refs = vec![];
        for state in self.pure_states.iter() {
            block_refs.push(BlockRef {
                end_lt: state.state().gen_lt,
                seqno: state.block_id().seqno,
                root_hash: state.block_id().root_hash,
                file_hash: state.block_id().file_hash,
            });
        }

        let prev_ref = if block_refs.len() == 2 {
            PrevBlockRef::AfterMerge {
                left: block_refs.remove(0),
                right: block_refs.remove(0),
            }
        } else {
            PrevBlockRef::Single(block_refs.remove(0))
        };

        Ok(prev_ref)
    }

    pub fn pure_state_root(&self) -> &Cell {
        &self.pure_state_root
    }

    pub fn gen_chain_time(&self) -> u32 {
        self.gen_chain_time
    }

    pub fn gen_lt(&self) -> u64 {
        self.gen_lt
    }

    pub fn total_validator_fees(&self) -> &CurrencyCollection {
        &self.total_validator_fees
    }

    pub fn processed_upto(&self) -> &ProcessedUptoInfo {
        &self.processed_upto
    }
}

#[derive(Debug, Default)]
pub(super) struct BlockCollationData {
    // block_descr: Arc<String>,
    pub block_id_short: BlockIdShort,
    pub gen_utime: u32,
    pub gen_utime_ms: u16,

    pub tx_count: u32,

    pub total_execute_msgs_time_mc: u128,

    pub execute_count_all: u32,
    pub execute_count_ext: u32,
    pub execute_count_int: u32,
    pub execute_count_new_int: u32,
    pub enqueue_count: u32,
    pub dequeue_count: u32,

    pub new_msgs_created: u32,
    pub inserted_new_msgs_to_iterator: u32,
    pub read_new_msgs_from_iterator: u32,

    pub start_lt: u64,
    // Should be updated on each tx finalization from ExecutionManager.max_lt
    // which is updating during tx execution
    pub next_lt: u64,

    pub in_msgs: BTreeMap<HashBytes, PreparedInMsg>,
    pub out_msgs: BTreeMap<HashBytes, PreparedOutMsg>,

    pub processed_upto: ProcessedUptoInfo,
    pub externals_reading_started: bool,
    // TODO: remove if we do not need this
    pub _internals_reading_started: bool,

    /// Ids of top blocks from shards that be included in the master block
    pub top_shard_blocks_ids: Vec<BlockId>,

    shards: Option<FastHashMap<ShardIdent, Box<ShardDescription>>>,
    shards_max_end_lt: u64,

    // TODO: setup update logic when ShardFees would be implemented
    pub shard_fees: ShardFees,

    pub mint_msg: Option<InMsg>,
    pub recover_create_msg: Option<InMsg>,

    pub value_flow: ValueFlow,

    min_ref_mc_seqno: Option<u32>,

    pub rand_seed: HashBytes,

    pub block_create_count: FastHashMap<HashBytes, u64>,

    // TODO: set from anchor
    pub created_by: HashBytes,
}

#[derive(Debug)]
pub struct PreparedInMsg {
    pub in_msg: Lazy<InMsg>,
    pub import_fees: ImportFees,
}

#[derive(Debug)]
pub struct PreparedOutMsg {
    pub out_msg: Lazy<OutMsg>,
    pub exported_value: CurrencyCollection,
    pub new_tx: Option<Lazy<Transaction>>,
}

impl BlockCollationData {
    pub fn shards(&self) -> Result<&FastHashMap<ShardIdent, Box<ShardDescription>>> {
        self.shards
            .as_ref()
            .ok_or_else(|| anyhow!("`shards` is not initialized yet"))
    }
    pub fn set_shards(&mut self, shards: FastHashMap<ShardIdent, Box<ShardDescription>>) {
        self.shards = Some(shards);
    }
    pub fn shards_mut(&mut self) -> Result<&mut FastHashMap<ShardIdent, Box<ShardDescription>>> {
        self.shards
            .as_mut()
            .ok_or_else(|| anyhow!("`shards` is not initialized yet"))
    }

    pub fn shards_max_end_lt(&self) -> u64 {
        self.shards_max_end_lt
    }
    pub fn update_shards_max_end_lt(&mut self, val: u64) {
        if val > self.shards_max_end_lt {
            self.shards_max_end_lt = val;
        }
    }

    pub fn update_ref_min_mc_seqno(&mut self, mc_seqno: u32) -> u32 {
        let min_ref_mc_seqno =
            std::cmp::min(self.min_ref_mc_seqno.unwrap_or(std::u32::MAX), mc_seqno);
        self.min_ref_mc_seqno = Some(min_ref_mc_seqno);
        min_ref_mc_seqno
    }

    pub fn min_ref_mc_seqno(&self) -> Result<u32> {
        self.min_ref_mc_seqno
            .ok_or_else(|| anyhow!("`min_ref_mc_seqno` is not initialized yet"))
    }

    pub fn store_shard_fees(
        &mut self,
        shard_id: ShardIdent,
        proof_funds: ProofFunds,
    ) -> Result<()> {
        let shard_fee_created = ShardFeeCreated {
            fees: proof_funds.fees_collected.clone(),
            create: proof_funds.funds_created.clone(),
        };
        self.shard_fees.set(
            ShardIdentFull::from(shard_id),
            shard_fee_created.clone(),
            shard_fee_created,
        )?;
        Ok(())
    }

    pub fn register_shard_block_creators(&mut self, creators: Vec<HashBytes>) -> Result<()> {
        for creator in creators {
            self.block_create_count
                .entry(creator)
                .and_modify(|count| *count += 1)
                .or_insert(1);
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub(super) struct CollatorStats {
    pub total_execute_msgs_time_mc: u128,
    pub avg_exec_msgs_per_1000_ms: u128,

    pub total_execute_count_all: u32,
    pub total_execute_count_ext: u32,
    pub total_execute_count_int: u32,
    pub total_execute_count_new_int: u32,
    pub int_queue_length: u32,
}

pub(super) struct CachedMempoolAnchor {
    pub anchor: Arc<MempoolAnchor>,
    /// Has externals for current shard of collator
    pub has_externals: bool,
}

pub(super) type AccountId = HashBytes;

pub(super) type Transactions = AugDict<u64, CurrencyCollection, Lazy<Transaction>>;

pub(super) type AccountBlocksDict = AugDict<HashBytes, CurrencyCollection, AccountBlock>;

#[derive(Clone)]
pub(super) struct ShardAccountStuff {
    pub account_addr: AccountId,
    pub shard_account: ShardAccount,
    pub special: SpecialFlags,
    pub initial_state_hash: HashBytes,
    pub libraries: Dict<HashBytes, SimpleLib>,
    pub transactions: Transactions,
}

impl ShardAccountStuff {
    pub fn new(account_addr: &AccountId, shard_account: ShardAccount) -> Result<Self> {
        let initial_state_hash = *shard_account.account.inner().repr_hash();

        // TODO: Add intrinsic to everscale_types for a more optimal way to get libraries
        let (libraries, special) = shard_account
            .load_account()?
            .and_then(|account| {
                if let AccountState::Active(StateInit {
                    libraries, special, ..
                }) = account.state
                {
                    Some((libraries, special.unwrap_or_default()))
                } else {
                    None
                }
            })
            .unwrap_or_default();

        Ok(Self {
            account_addr: *account_addr,
            shard_account,
            special,
            initial_state_hash,
            libraries,
            transactions: Default::default(),
        })
    }

    pub fn new_empty(account_addr: &AccountId) -> Self {
        static EMPTY_SHARD_ACCOUNT: OnceLock<ShardAccount> = OnceLock::new();

        let shard_account = EMPTY_SHARD_ACCOUNT
            .get_or_init(|| ShardAccount {
                account: Lazy::new(&OptionalAccount::EMPTY).unwrap(),
                last_trans_hash: Default::default(),
                last_trans_lt: 0,
            })
            .clone();

        let initial_state_hash = *shard_account.account.inner().repr_hash();

        Self {
            account_addr: *account_addr,
            shard_account,
            special: Default::default(),
            initial_state_hash,
            libraries: Dict::new(),
            transactions: Default::default(),
        }
    }

    pub fn build_hash_update(&self) -> Lazy<HashUpdate> {
        Lazy::new(&HashUpdate {
            old: self.initial_state_hash,
            new: *self.shard_account.account.inner().repr_hash(),
        })
        .unwrap()
    }

    pub fn add_transaction(
        &mut self,
        lt: u64,
        total_fees: &CurrencyCollection,
        transaction: &Lazy<Transaction>,
    ) -> Result<()> {
        self.transactions.set(lt, total_fees, transaction)?;
        Ok(())
    }

    pub fn update_public_libraries(
        &self,
        loaded_account: &Option<Account>,
        global_libraries: &mut Dict<HashBytes, LibDescr>,
    ) -> Result<()> {
        static EMPTY_LIBS: Dict<HashBytes, SimpleLib> = Dict::new();

        let new_libraries = match loaded_account {
            Some(Account {
                state: AccountState::Active(s),
                ..
            }) => &s.libraries,
            _ => &EMPTY_LIBS,
        };

        if new_libraries.root() == self.libraries.root() {
            return Ok(());
        }

        for entry in new_libraries.iter_union(&self.libraries) {
            let (ref key, new_value, old_value) = entry?;
            match (new_value, old_value) {
                (Some(new), Some(old)) => {
                    if new.public && !old.public {
                        self.add_public_library(key, &new.root, global_libraries)?;
                    } else if !new.public && old.public {
                        self.remove_public_library(key, global_libraries)?;
                    }
                }
                (Some(new), None) if new.public => {
                    self.add_public_library(key, &new.root, global_libraries)?;
                }
                (None, Some(old)) if old.public => {
                    self.remove_public_library(key, global_libraries)?;
                }
                _ => continue,
            }
        }
        Ok(())
    }

    pub fn remove_public_library(
        &self,
        key: &HashBytes,
        global_libraries: &mut Dict<HashBytes, LibDescr>,
    ) -> Result<()> {
        tracing::trace!(
            account_addr = %self.account_addr,
            library = %key,
            "removing public library",
        );

        let Some(mut lib_descr) = global_libraries.get(key)? else {
            anyhow::bail!(
                "cannot remove public library {key} of account {} because this public \
                library did not exist",
                self.account_addr
            )
        };

        anyhow::ensure!(
            lib_descr.lib.repr_hash() == key,
            "cannot remove public library {key} of account {} because this public library \
            LibDescr record does not contain a library root cell with required hash",
            self.account_addr
        );

        anyhow::ensure!(
            lib_descr.publishers.remove(self.account_addr)?.is_some(),
            "cannot remove public library {key} of account {} because this public library \
            LibDescr record does not list this account as one of publishers",
            self.account_addr
        );

        if lib_descr.publishers.is_empty() {
            tracing::debug!(
                account_addr = %self.account_addr,
                library = %key,
                "library has no publishers left, removing altogether",
            );
            global_libraries.remove(key)?;
        } else {
            global_libraries.set(key, &lib_descr)?;
        }

        Ok(())
    }

    pub fn add_public_library(
        &self,
        key: &HashBytes,
        library: &Cell,
        global_libraries: &mut Dict<HashBytes, LibDescr>,
    ) -> Result<()> {
        tracing::trace!(
            account_addr = %self.account_addr,
            library = %key,
            "adding public library",
        );

        anyhow::ensure!(
            library.repr_hash() == key,
            "cannot add library {key} because its root has a different hash",
        );

        let lib_descr = if let Some(mut old_lib_descr) = global_libraries.get(key)? {
            anyhow::ensure!(
                old_lib_descr.lib.repr_hash() == library.repr_hash(),
                "cannot add public library {key} of account {} because existing LibDescr \
                data has a different root cell hash",
                self.account_addr,
            );

            anyhow::ensure!(
                old_lib_descr.publishers.get(&self.account_addr)?.is_none(),
                "cannot add public library {key} of account {} because this public library's \
                LibDescr record already lists this account as a publisher",
                self.account_addr,
            );

            old_lib_descr.publishers.set(self.account_addr, ())?;
            old_lib_descr
        } else {
            let mut dict = Dict::new();
            dict.set(self.account_addr, ())?;
            LibDescr {
                lib: library.clone(),
                publishers: dict,
            }
        };

        global_libraries.set(key, &lib_descr)?;

        Ok(())
    }
}

pub trait ShardDescriptionExt {
    fn from_block_info(
        block_id: BlockId,
        block_info: &BlockInfo,
        value_flow: &ValueFlow,
    ) -> ShardDescription;
}

impl ShardDescriptionExt for ShardDescription {
    fn from_block_info(
        block_id: BlockId,
        block_info: &BlockInfo,
        value_flow: &ValueFlow,
    ) -> ShardDescription {
        ShardDescription {
            seqno: block_id.seqno,
            reg_mc_seqno: 0,
            start_lt: block_info.start_lt,
            end_lt: block_info.end_lt,
            root_hash: block_id.root_hash,
            file_hash: block_id.file_hash,
            before_split: block_info.before_split,
            before_merge: false, // TODO: by t-node, needs to review
            want_split: block_info.want_split,
            want_merge: block_info.want_merge,
            nx_cc_updated: false, // TODO: by t-node, needs to review
            next_catchain_seqno: block_info.gen_catchain_seqno,
            next_validator_shard: block_info.shard.prefix(), /* eq to `shard_prefix_with_tag` in old node */
            min_ref_mc_seqno: block_info.min_ref_mc_seqno,
            gen_utime: block_info.gen_utime,
            split_merge_at: None, // TODO: check if we really should not use it here
            fees_collected: value_flow.fees_collected.clone(),
            funds_created: value_flow.created.clone(),
            copyleft_rewards: Default::default(),
            proof_chain: None,
            #[cfg(feature = "venom")]
            collators: None,
        }
    }
}

// /// Async message
// pub(super) enum AsyncMessage {
//     Ordinary(Box<OrdinaryMessage>),
//     Special(TickTock),
// }

// impl AsyncMessage {
//     pub fn display_kind(&self) -> &str {
//         match self {
//             AsyncMessage::Ordinary(msg) => msg.display_kind(),
//             AsyncMessage::Special(TickTock::Tick) => "Tick",
//             AsyncMessage::Special(TickTock::Tock) => "Tock",
//         }
//     }
// }

pub struct AsyncMessage {
    pub info: MsgInfo,
    pub cell: Cell,
    pub special_origin: Option<SpecialOrigin>,
    pub dequeued: Option<Dequeued>,
}

impl AsyncMessage {
    pub fn kind(&self) -> AsyncMessageKind {
        match (&self.info, self.special_origin) {
            (_, Some(SpecialOrigin::Recover)) => AsyncMessageKind::Recover,
            (_, Some(SpecialOrigin::Mint)) => AsyncMessageKind::Mint,
            (MsgInfo::ExtIn(_), _) => AsyncMessageKind::ExtIn,
            (MsgInfo::Int(_), _) => AsyncMessageKind::Int,
            (MsgInfo::ExtOut(_), _) => AsyncMessageKind::ExtOut,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsyncMessageKind {
    Recover,
    Mint,
    ExtIn,
    Int,
    ExtOut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpecialOrigin {
    Recover,
    Mint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Dequeued {
    pub same_shard: bool,
}
