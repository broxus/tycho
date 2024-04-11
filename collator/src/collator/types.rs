use std::{collections::BTreeMap, sync::Arc};

use anyhow::Result;

use everscale_types::{
    cell::{Cell, UsageTree, UsageTreeMode},
    models::{BlockId, CurrencyCollection, McStateExtra, ShardAccounts, ShardIdent},
};

use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};

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
    state: Arc<ShardStateStuff>,
}
impl McData {
    pub fn new(mc_state: Arc<ShardStateStuff>) -> Result<Self> {
        let mc_state_extra = mc_state.state_extra()?;

        // prev key block
        let (prev_key_block_seqno, prev_key_block) = if mc_state_extra.after_key_block {
            (mc_state.block_id().seqno, Some(*mc_state.block_id()))
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
            state: mc_state,
        })
    }

    pub fn state(&self) -> Arc<ShardStateStuff> {
        self.state.clone()
    }

    pub fn mc_state_extra(&self) -> &McStateExtra {
        &self.mc_state_extra
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

    externals_processed_upto: BTreeMap<MempoolAnchorId, usize>,
}
impl PrevData {
    pub fn build(
        _mc_data: &McData,
        prev_states: &Vec<Arc<ShardStateStuff>>,
        prev_blocks_ids: Vec<BlockId>,
    ) -> Result<(Self, UsageTree)> {
        //TODO: make real implementation
        // refer to the old node impl:
        //  Collator::prepare_data()
        //  Collator::unpack_last_state()

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

            externals_processed_upto: BTreeMap::new(),
        };

        Ok((prev_data, usage_tree))
    }

    pub fn update_state(&mut self, new_blocks_ids: Vec<BlockId>) -> Result<()> {
        //TODO: make real implementation
        //STUB: currently have stub signature and implementation
        self.blocks_ids = new_blocks_ids;

        Ok(())
    }

    pub fn blocks_ids(&self) -> &Vec<BlockId> {
        &self.blocks_ids
    }

    pub fn pure_states(&self) -> &Vec<Arc<ShardStateStuff>> {
        &self.pure_states
    }

    pub fn externals_processed_upto(&self) -> &BTreeMap<MempoolAnchorId, usize> {
        &self.externals_processed_upto
    }
}

pub(super) struct BlockCollationData {
    block_descr: Arc<String>,
}
