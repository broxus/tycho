use chrono::NaiveDateTime;
use diesel::mysql::Mysql;
use diesel::{Queryable, Selectable};

use crate::schema::contracts_info;
use crate::utils::HashInsert;

#[derive(Queryable, Debug, Selectable)]
#[diesel(check_for_backend(Mysql))]
#[diesel(table_name = contracts_info)]
pub struct ContractData {
    pub code_hash: HashInsert<32>,
    pub abi: serde_json::Value,
    pub contract_id: HashInsert<16>,
    pub created_at: NaiveDateTime,
}
