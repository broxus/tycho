// @generated automatically by Diesel CLI.

pub mod sql_types {
    pub use crate::types::*;
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::StateMapping;

    accounts (address, workchain) {
        workchain -> Tinyint,
        #[max_length = 32]
        address -> Varbinary,
        #[max_length = 8]
        state -> StateMapping,
        balance -> Unsigned<Bigint>,
        #[max_length = 32]
        init_code_hash -> Nullable<Varbinary>,
        #[max_length = 32]
        code_hash -> Nullable<Varbinary>,
        #[max_length = 32]
        data_hash -> Nullable<Varbinary>,
        #[max_length = 32]
        creator_address -> Nullable<Varbinary>,
        creator_wc -> Tinyint,
        created_at -> Unsigned<Integer>,
        updated_at -> Unsigned<Integer>,
        updated_lt -> Unsigned<Bigint>,
    }
}

diesel::table! {
    address_info (workchain_id, address) {
        #[max_length = 32]
        user_public_key -> Varbinary,
        workchain_id -> Tinyint,
        #[max_length = 32]
        address -> Varbinary,
        #[max_length = 32]
        code_hash -> Nullable<Varbinary>,
        #[max_length = 100]
        name -> Varchar,
        #[max_length = 255]
        descr -> Nullable<Varchar>,
        socials -> Nullable<Json>,
        #[max_length = 255]
        url -> Nullable<Varchar>,
        system_info -> Bool,
    }
}

diesel::table! {
    blocks (workchain, shard, seqno) {
        workchain -> Tinyint,
        shard -> Unsigned<Bigint>,
        seqno -> Unsigned<Integer>,
        #[max_length = 32]
        root_hash -> Varbinary,
        #[max_length = 32]
        file_hash -> Varbinary,
        is_key_block -> Bool,
        transaction_count -> Unsigned<Smallint>,
        gen_utime -> Unsigned<Integer>,
        gen_software_version -> Unsigned<Integer>,
        #[max_length = 32]
        prev1 -> Varbinary,
        prev1_seqno -> Unsigned<Integer>,
        #[max_length = 32]
        prev2 -> Nullable<Varbinary>,
        prev2_seqno -> Nullable<Unsigned<Integer>>,
        prev_key_block -> Unsigned<Integer>,
        block_info -> Json,
        value_flow -> Json,
        account_blocks -> Json,
        shards_info -> Nullable<Json>,
        additional_info -> Nullable<Json>,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::MessageTypeMapping;

    messages (message_hash) {
        #[max_length = 32]
        message_hash -> Varbinary,
        src_workchain -> Tinyint,
        #[max_length = 32]
        src_address -> Nullable<Varbinary>,
        dst_workchain -> Tinyint,
        #[max_length = 32]
        dst_address -> Nullable<Varbinary>,
        #[max_length = 11]
        message_type -> MessageTypeMapping,
        message_value -> Unsigned<Bigint>,
        ihr_fee -> Unsigned<Bigint>,
        fwd_fee -> Unsigned<Bigint>,
        import_fee -> Unsigned<Bigint>,
        created_lt -> Unsigned<Bigint>,
        created_at -> Unsigned<Integer>,
        bounced -> Bool,
        bounce -> Bool,
    }
}

diesel::table! {
    network_config (end_lt) {
        end_lt -> Unsigned<Bigint>,
        seq_no -> Unsigned<Integer>,
        config_params_boc -> Mediumblob,
    }
}

diesel::table! {
    network_info (root_hash) {
        #[max_length = 64]
        root_hash -> Varchar,
    }
}

diesel::table! {
    contracts_info (contract_id) {
        #[max_length = 1024]
        contract_name -> Varchar,
        #[max_length = 1024]
        project_link -> Nullable<Varchar>,
        #[max_length = 32]
        code_hash -> Varbinary,
        #[max_length = 16]
        contract_id -> Varbinary,
        abi -> Json,
        tvc -> Mediumblob,
        sources -> Json,
        history_processed -> Unsigned<Integer>,
        created_at -> Datetime,
        #[max_length = 128]
        compiler_version -> Varchar,
        #[max_length = 128]
        linker_version -> Varchar,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::ParsedTypeMapping;

    parsed_messages_new (message_hash) {
        #[max_length = 32]
        message_hash -> Varbinary,
        #[max_length = 128]
        method_name -> Varchar,
        #[max_length = 16]
        contract_id -> Varbinary,
        parsed_id -> Unsigned<Integer>,
        #[max_length = 15]
        parsed_type -> ParsedTypeMapping,
    }
}

diesel::table! {
    raw_transactions (wc, account_id, lt) {
        wc -> Tinyint,
        #[max_length = 32]
        account_id -> Varbinary,
        lt -> Unsigned<Bigint>,
        data -> Mediumblob,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::MessageTypeMapping;

    transaction_messages (transaction_time, transaction_lt, transaction_hash, is_out, index_in_transaction) {
        #[max_length = 32]
        transaction_hash -> Varbinary,
        index_in_transaction -> Unsigned<Smallint>,
        is_out -> Bool,
        transaction_time -> Unsigned<Integer>,
        transaction_lt -> Unsigned<Bigint>,
        transaction_account_workchain -> Tinyint,
        #[max_length = 32]
        transaction_account_address -> Varbinary,
        #[max_length = 32]
        block_hash -> Varbinary,
        dst_workchain -> Tinyint,
        #[max_length = 32]
        dst_address -> Nullable<Varbinary>,
        src_workchain -> Tinyint,
        #[max_length = 32]
        src_address -> Nullable<Varbinary>,
        #[max_length = 32]
        message_hash -> Varbinary,
        #[max_length = 11]
        message_type -> MessageTypeMapping,
        message_value -> Unsigned<Bigint>,
        bounced -> Bool,
        bounce -> Bool,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::TransactionTypeMapping;

    transactions (time, lt, hash) {
        workchain -> Tinyint,
        #[max_length = 32]
        account_id -> Varbinary,
        lt -> Unsigned<Bigint>,
        time -> Unsigned<Integer>,
        #[max_length = 32]
        hash -> Varbinary,
        block_shard -> Unsigned<Bigint>,
        block_seqno -> Unsigned<Integer>,
        #[max_length = 32]
        block_hash -> Varbinary,
        #[max_length = 12]
        tx_type -> TransactionTypeMapping,
        aborted -> Bool,
        balance_change -> Bigint,
        exit_code -> Nullable<Integer>,
        result_code -> Nullable<Integer>,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    accounts,
    address_info,
    blocks,
    contracts_info,
    messages,
    network_config,
    network_info,
    parsed_messages_new,
    raw_transactions,
    transaction_messages,
    transactions,
);
