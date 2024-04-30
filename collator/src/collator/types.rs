use std::sync::atomic::{AtomicU64, Ordering};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use anyhow::{anyhow, bail, Result};

use everscale_types::cell::{CellFamily, Store};
use everscale_types::merkle::MerkleUpdate;
use everscale_types::models::{Lazy, StateInit, TickTock, Transaction};
use everscale_types::{
    cell::{Cell, HashBytes, UsageTree, UsageTreeMode},
    dict::{AugDict, Dict},
    models::{
        in_message::{ImportFees, InMsg},
        out_message::OutMsg,
        AccountBlock, AccountState, BlockId, BlockIdShort, BlockInfo, BlockRef, BlockchainConfig,
        CurrencyCollection, LibDescr, McStateExtra, OutMsgQueueInfo, OwnedMessage, PrevBlockRef,
        ProcessedUpto, ShardAccount, ShardAccounts, ShardDescription, ShardFees, ShardIdent,
        SimpleLib, ValueFlow,
    },
};

use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};
use tycho_core::internal_queue::types::ext_types_stubs::EnqueuedMessage;
use tycho_util::FastDashMap;

use crate::mempool::MempoolAnchorId;

/*
В текущем коллаторе перед коллацией блока импортируется:
    - предыдущий мастер стейт
    - предыдущие стейты шарды (их может быть 2, если мерж)
ImportedData {
    mc_state: Arc<ShardStateStuff>,
    prev_states: Vec<Arc<ShardStateStuff>>,
    prev_ext_blocks_refs: Vec<ExtBlkRef>,
    top_shard_blocks_descr: Vec<Arc<TopBlockDescrStuff>>,
}
top_shard_blocks_descr - список верхних новых шардблоков с последнего мастера, если будем коллировать мастер
    берутся из prev_states
prev_ext_blocks_refs - ссылки на предыдущие шард блоки, на момент которых загружаются стейты шарды,
    они берутся на основании prev_blocks_ids коллатора, загружаются вместе с prev_states
для мастерчейна выполняется проверка на номер блока (надо в ней разобраться)

Что входит в стейт шарды
ShardStateStuff {
    block_id: BlockId,
    shard_state: Option<ShardStateUnsplit>,
    out_msg_queue: Option<ShardStateUnsplit>,
    out_msg_queue_for: i32,
    shard_state_extra: Option<McStateExtra>,
    root: Cell
}

Затем из этих данных методом prepare_data() готовится: McData, PrevData и CollatorData
pub struct McData {
    mc_state_extra: McStateExtra,
    prev_key_block_seqno: u32,
    prev_key_block: Option<BlockId>,
    state: Arc<ShardStateStuff>
}
pub struct PrevData {
    states: Vec<Arc<ShardStateStuff>>, // предыдущие стейты с отслеживанием изменений через UsageTree
    pure_states: Vec<Arc<ShardStateStuff>>, // исходные предыдущие стейты шарды без отслеживания посещений
    state_root: Cell,   // рутовая ячейка предыдущего стейта шарды (при мерже там будет объединенная ячейка из двух шард)
                        // без отслеживания изменений
    accounts: ShardAccounts,    // предыдущие аккаунты шарды с отслеживанием (получены с учетом сплита/мержа)
    gen_utime: u32,
    gen_lt: u64,
    total_validator_fees: CurrencyCollection,
    overload_history: u64,
    underload_history: u64,
    state_copyleft_rewards: CopyleftRewards,
}
pub struct CollatorData {
    usage_tree: UsageTree, // дерево посещенный ячеек стейта для вычисления меркл пруфа
}

Далее при коллации
При инициализации ExecutionManager стейты, ячейки и аккаунты не используются
При создании tick-tock транзакций McData используется для получения ИД контракта с конфигом и чтения конфига консенсуса
В коллацию интерналов McData не передается, передается PrevData и CollatorData

PrevData и CollatorData передаются в execute. Там берется аккаунт из PrevData и передается в таску выполнения сообщения.
Там из аккаунта берется рутовая ячейка и передается в метод выполнения сообщения, где она изменяется.
Затем подменяется рут в аккаунте, а предыдущее состояние аккаунта сохраняется в prev_account_stuff,
то есть изменяемый аккаунт накапливает историю изменений
При завершении таски она возвращает актуальный обновленный аккаунт - это происходит при финализации блока

В методе финализации блока
- запоминаем аккаунты из предыдущего стейта в new_accounts
- берем все измененные аккаунты shard_acc и перименяем их изменения в стейт аккаунтов new_accounts
- из измененного аккаунта делаем AccountBlock и сохраняем в accounts, если в нем есть транзакции
- так же кладем измененный аккаунт shard_acc в список changed_accounts
- создаем новый стейт шарды new_state с использованием обновленных аккаунтов new_accounts
- из нового стейта делаем новую рут ячейку new_ss_root
- вычисляем меркл апдейты
- завершаем создание блока с использованием accounts с транзакциями

Метод коллации блока возвращает новый стейт шарды типа ShardStateUnsplit
из него можно собрать новый ShardStateStuff, который может использоваться для дальнейшей коллации
*/

pub(super) struct WorkingState {
    pub mc_data: McData,
    pub prev_shard_data: PrevData,
    pub usage_tree: UsageTree,
}

pub(super) struct McData {
    mc_state_extra: McStateExtra,
    prev_key_block_seqno: u32,
    prev_key_block: Option<BlockId>,
    mc_state_stuff: Arc<ShardStateStuff>,
}
impl McData {
    pub fn build(mc_state_stuff: Arc<ShardStateStuff>) -> Result<Self> {
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
            mc_state_extra: mc_state_extra.clone(),
            prev_key_block,
            prev_key_block_seqno,
            mc_state_stuff,
        })
    }

    pub fn prev_key_block_seqno(&self) -> u32 {
        self.prev_key_block_seqno
    }

    pub fn mc_state_stuff(&self) -> Arc<ShardStateStuff> {
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
            root_hash: block_id.root_hash.clone(),
            file_hash: block_id.file_hash.clone(),
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
    observable_states: Vec<Arc<ShardStateStuff>>,
    observable_accounts: ShardAccounts,

    blocks_ids: Vec<BlockId>,

    pure_states: Vec<Arc<ShardStateStuff>>,
    pure_state_root: Cell,

    gen_utime: u32,
    gen_lt: u64,
    total_validator_fees: CurrencyCollection,
    overload_history: u64,
    underload_history: u64,

    externals_processed_upto: BTreeMap<MempoolAnchorId, u64>,
}
impl PrevData {
    pub fn build(
        _mc_data: &McData,
        prev_states: &Vec<Arc<ShardStateStuff>>,
    ) -> Result<(Self, UsageTree)> {
        //TODO: make real implementation
        // refer to the old node impl:
        //  Collator::prepare_data()
        //  Collator::unpack_last_state()

        let prev_blocks_ids: Vec<_> = prev_states.iter().map(|s| *s.block_id()).collect();
        let pure_prev_state_root = prev_states[0].root_cell();
        let pure_prev_states = prev_states.clone();

        let usage_tree = UsageTree::new(UsageTreeMode::OnDataAccess);
        let observable_root = usage_tree.track(pure_prev_state_root);
        let tracker = MinRefMcStateTracker::new();
        let observable_states = vec![Arc::new(ShardStateStuff::new(
            *pure_prev_states[0].block_id(),
            observable_root,
            &tracker,
        )?)];

        let gen_utime = observable_states[0].state().gen_utime;
        let gen_lt = observable_states[0].state().gen_lt;
        let observable_accounts = observable_states[0].state().load_accounts()?;
        let total_validator_fees = observable_states[0].state().total_validator_fees.clone();
        let overload_history = observable_states[0].state().overload_history;
        let underload_history = observable_states[0].state().underload_history;
        let iter = pure_prev_states[0]
            .state()
            .externals_processed_upto
            .iter()
            .filter_map(|kv| kv.ok());
        let externals_processed_upto = BTreeMap::from_iter(iter);

        let prev_data = Self {
            observable_states,
            observable_accounts,

            blocks_ids: prev_blocks_ids,

            pure_states: pure_prev_states,
            pure_state_root: pure_prev_state_root.clone(),

            gen_utime,
            gen_lt,
            total_validator_fees,
            overload_history,
            underload_history,

            externals_processed_upto,
        };

        Ok((prev_data, usage_tree))
    }

    pub fn update_state(&mut self, new_blocks_ids: Vec<BlockId>) -> Result<()> {
        //TODO: make real implementation
        //STUB: currently have stub signature and implementation
        self.blocks_ids = new_blocks_ids;

        Ok(())
    }

    pub fn observable_states(&self) -> &Vec<Arc<ShardStateStuff>> {
        &self.observable_states
    }

    pub fn observable_accounts(&self) -> &ShardAccounts {
        &self.observable_accounts
    }

    pub fn blocks_ids(&self) -> &Vec<BlockId> {
        &self.blocks_ids
    }

    pub fn get_blocks_ref(&self) -> Result<PrevBlockRef> {
        if self.pure_states.len() < 1 || self.pure_states.len() > 2 {
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
                root_hash: state.block_id().root_hash.clone(),
                file_hash: state.block_id().file_hash.clone(),
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

    pub fn pure_states(&self) -> &Vec<Arc<ShardStateStuff>> {
        &self.pure_states
    }

    pub fn pure_state_root(&self) -> &Cell {
        &self.pure_state_root
    }

    pub fn gen_lt(&self) -> u64 {
        self.gen_lt
    }

    pub fn total_validator_fees(&self) -> &CurrencyCollection {
        &self.total_validator_fees
    }

    pub fn externals_processed_upto(&self) -> &BTreeMap<MempoolAnchorId, u64> {
        &self.externals_processed_upto
    }
}

#[derive(Debug, Default)]
pub(super) struct BlockCollationData {
    //block_descr: Arc<String>,
    pub block_id_short: BlockIdShort,
    pub chain_time: u32,

    pub start_lt: u64,
    // Should be updated on each tx finalization from ExecutionManager.max_lt
    // which is updating during tx execution
    pub max_lt: u64,

    pub in_msgs: InMsgDescr,
    pub out_msgs: OutMsgDescr,

    // should read from prev_shard_state
    pub out_msg_queue_stuff: OutMsgQueueInfoStuff,
    /// Index of the highest external processed from the anchor: (anchor, index)
    pub externals_processed_upto: Dict<u32, u64>,

    /// Ids of top blocks from shards that be included in the master block
    pub top_shard_blocks_ids: Vec<BlockId>,

    shards: Option<HashMap<ShardIdent, Box<ShardDescription>>>,
    shards_max_end_lt: u64,

    //TODO: setup update logic when ShardFees would be implemented
    pub shard_fees: ShardFees,

    pub mint_msg: Option<InMsg>,
    pub recover_create_msg: Option<InMsg>,

    pub value_flow: ValueFlow,

    min_ref_mc_seqno: Option<u32>,

    pub rand_seed: HashBytes,
}

impl BlockCollationData {
    pub fn shards(&self) -> Result<&HashMap<ShardIdent, Box<ShardDescription>>> {
        self.shards
            .as_ref()
            .ok_or_else(|| anyhow!("`shards` is not initialized yet"))
    }
    pub fn set_shards(&mut self, shards: HashMap<ShardIdent, Box<ShardDescription>>) {
        self.shards = Some(shards);
    }
    pub fn shards_mut(&mut self) -> Result<&mut HashMap<ShardIdent, Box<ShardDescription>>> {
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
}

pub(super) type AccountId = HashBytes;

pub(super) type InMsgDescr = AugDict<HashBytes, ImportFees, InMsg>;
pub(super) type OutMsgDescr = AugDict<HashBytes, CurrencyCollection, OutMsg>;

pub(super) type AccountBlocksDict = AugDict<HashBytes, CurrencyCollection, AccountBlock>;

#[derive(Clone)]
pub(super) struct ShardAccountStuff {
    pub account_addr: AccountId,
    pub shard_account: ShardAccount,
    pub orig_libs: Dict<HashBytes, SimpleLib>,
    pub account_root: Cell,
    pub last_trans_hash: HashBytes,
    pub state_update: HashBytes,
    pub last_trans_lt: u64,
    pub lt: Arc<AtomicU64>,
    pub transactions: Vec<Transaction>,
}

impl ShardAccountStuff {
    // pub fn update_shard_state(&mut self, shard_accounts: &mut ShardAccounts) -> Result<AccountBlock> {
    //     let account = self.shard_account.load_account()?;
    //     if account.is_none() {
    //         new_accounts.remove(self.account_addr().clone())?;
    //     } else {
    //         let shard_acc = ShardAccount::with_account_root(self.account_root(), self.last_trans_hash.clone(), self.last_trans_lt);
    //         let value = shard_acc.write_to_new_cell()?;
    //         new_accounts.set_builder_serialized(self.account_addr().clone(), &value, &account.aug()?)?;
    //     }
    //     AccountBlock::with_params(&self.account_addr, &self.transactions, &self.state_update)
    // }

    pub fn update_public_libraries(&self, libraries: &mut Dict<HashBytes, LibDescr>) -> Result<()> {
        let opt_account = self.shard_account.account.load()?;
        let state_init = match opt_account.state() {
            Some(AccountState::Active(ref state_init)) => Some(state_init),
            _ => None,
        };
        let new_libs = state_init.map(|v| v.libraries.clone()).unwrap_or_default();
        if new_libs.root() != self.orig_libs.root() {
            //TODO: implement when scan_diff be added
            //STUB: just do nothing, no accounts, no libraries updates in prototype
            // new_libs.scan_diff(&self.orig_libs, |key: UInt256, old, new| {
            //     let old = old.unwrap_or_default();
            //     let new = new.unwrap_or_default();
            //     if old.is_public_library() && !new.is_public_library() {
            //         self.remove_public_library(key, libraries)?;
            //     } else if !old.is_public_library() && new.is_public_library() {
            //         self.add_public_library(key, new.root, libraries)?;
            //     }
            //     Ok(true)
            // })?;
        }
        Ok(())
    }

    pub fn new(
        account_addr: AccountId,
        shard_account: ShardAccount,
        max_lt: u64,
        min_lt: u64,
    ) -> Result<Self> {
        let account_root = shard_account.account.clone().inner();
        let state_update = account_root.repr_hash();
        let last_trans_hash = shard_account.last_trans_hash.clone();
        let last_trans_lt = shard_account.last_trans_lt;
        let orig_libs = shard_account
            .load_account()?
            .map(|account| {
                if let AccountState::Active(StateInit { ref libraries, .. }) = account.state {
                    libraries.clone()
                } else {
                    Default::default()
                }
            })
            .unwrap_or_default();

        let mut lt = Arc::new(max_lt.into());
        lt.fetch_max(min_lt, Ordering::Release);
        lt.fetch_max(last_trans_lt + 1, Ordering::Release);
        Ok(Self {
            account_addr,
            shard_account,
            orig_libs,
            account_root: account_root.clone(),
            last_trans_hash,
            last_trans_lt,
            lt,
            transactions: Default::default(),
            state_update: state_update.clone(),
        })
    }
    pub fn add_transaction(
        &mut self,
        transaction: &mut Transaction,
        account_root: Cell,
    ) -> Result<()> {
        transaction.prev_trans_hash = self.last_trans_hash.clone();
        transaction.prev_trans_lt = self.last_trans_lt;

        self.account_root = account_root;
        self.state_update = self.account_root.repr_hash().clone();

        let mut builder = everscale_types::cell::CellBuilder::new();
        transaction.store_into(&mut builder, &mut Cell::empty_context())?;
        let tr_root = builder.build()?;

        self.last_trans_hash = tr_root.repr_hash().clone();
        self.last_trans_lt = transaction.lt;

        // TODO! setref
        // self.transactions.setref(
        //     &transaction.logical_time(),
        //     &tr_root,
        //     transaction.total_fees()
        // )?;

        Ok(())
    }
}

#[derive(Debug, Default)]
pub(super) struct OutMsgQueueInfoStuff {
    ///  Dict (shard, seq_no): processed up to info
    pub proc_info: Dict<(u64, u32), ProcessedUpto>,
}

impl OutMsgQueueInfoStuff {
    ///TODO: make real implementation
    pub fn get_out_msg_queue_info(&self) -> (OutMsgQueueInfo, u32) {
        let mut min_ref_mc_seqno = u32::MAX;
        //STUB: just clone existing
        let msg_queue_info = OutMsgQueueInfo {
            proc_info: self.proc_info.clone(),
        };
        (msg_queue_info, min_ref_mc_seqno)
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
            before_merge: false, //TODO: by t-node, needs to review
            want_split: block_info.want_split,
            want_merge: block_info.want_merge,
            nx_cc_updated: false, //TODO: by t-node, needs to review
            next_catchain_seqno: block_info.gen_catchain_seqno,
            next_validator_shard: block_info.shard.prefix(), // eq to `shard_prefix_with_tag` in old node
            min_ref_mc_seqno: block_info.min_ref_mc_seqno,
            gen_utime: block_info.gen_utime,
            split_merge_at: None, //TODO: check if we really should not use it here
            fees_collected: value_flow.fees_collected.clone(),
            funds_created: value_flow.created.clone(),
            copyleft_rewards: Default::default(),
            proof_chain: None,
            #[cfg(feature = "venom")]
            collators: None,
        }
    }
}

/// Async message
pub(super) enum AsyncMessage {
    /// 0 - message; 1 - message.id_hash()
    Ext(OwnedMessage, HashBytes),
    /// 0 - message in execution queue; 1 - TRUE when from the same shard
    Int(EnqueuedMessage, bool),
    /// TickTock message
    TickTock(TickTock),
}

pub mod ext_types_stubs {}

pub trait ShardStateProvider {
    fn get_account_state(&self, account_id: &AccountId) -> Option<ShardAccount>;
    fn get_updated_accounts(&self) -> Vec<(AccountId, ShardAccount)>;
    fn update_account_state(&self, account_id: &AccountId, account: ShardAccount);
}

#[derive(Debug)]
pub(super) struct ShardStateProviderImpl<'a> {
    pub changed_accounts: FastDashMap<AccountId, ShardAccount>,
    pub shard_accounts: &'a ShardAccounts,
}

impl ShardStateProviderImpl<'_> {
    pub fn new(shard_accounts: &ShardAccounts) -> Self {
        Self {
            changed_accounts: Default::default(),
            shard_accounts,
        }
    }
}

impl ShardStateProvider for ShardStateProviderImpl<'_> {
    fn get_account_state(&self, account_id: &AccountId) -> Option<ShardAccount> {
        if let Some(a) = self.changed_accounts.get(account_id) {
            return Some(a.clone());
        } else if let Some(a) = self.shard_accounts.get(account_id) {
            self.changed_accounts.insert(account_id.clone(), a.clone());
            return Some(a.clone());
        } else {
            None
        }
    }
    fn get_updated_accounts(&self) -> Vec<(AccountId, ShardAccount)> {
        self.changed_accounts
            .iter()
            .map(|kv| (kv.key().clone(), kv.value().clone()))
            .collect()
    }
    fn update_account_state(&self, account_id: &AccountId, account: ShardAccount) {
        self.changed_accounts.insert(account_id.clone(), account);
    }
}
