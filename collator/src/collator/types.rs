use std::sync::Arc;

use crate::types::{
    ext_types::{
        BlockIdExt, Cell, CurrencyCollection, McStateExtra, ShardAccounts, ShardStateUnsplit,
    },
    ShardStateStuff,
};

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
    block_id: BlockIdExt,
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
    prev_key_block: Option<BlockIdExt>,
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

pub struct WorkingState {
    mc_data: McData,
    prev_shard_data: PrevData,
}

pub struct McData {
    mc_state_extra: McStateExtra,
    prev_key_block_seqno: u32,
    prev_key_block: Option<BlockIdExt>,
    state: Arc<ShardStateStuff>,
}

pub struct PrevData {
    observable_states: Vec<Arc<ShardStateStuff>>,
    observable_accounts: ShardAccounts,
    pure_state_root: Cell,

    gen_utime: u32,
    gen_lt: u64,
    total_validator_fees: CurrencyCollection,
    overload_history: u64,
    underload_history: u64,
}

pub struct BlockCollationData {
    block_descr: Arc<String>,
}
