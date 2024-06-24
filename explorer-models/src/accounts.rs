use diesel::mysql::Mysql;
use diesel::{Insertable, Queryable, Selectable};
use serde::{Deserialize, Serialize};

use crate::schema::sql_types::State;
#[cfg(feature = "csv")]
use crate::utils::*;
use crate::{Hash, NumBinds};

#[derive(Debug, Serialize, Clone, Insertable, Queryable, Eq, PartialEq)]
#[diesel(table_name = crate::schema::accounts)]
pub struct Account {
    pub workchain: i8,
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bytes"))]
    pub address: Hash,
    pub state: State,
    pub balance: u64,
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bytes_optional"))]
    pub init_code_hash: Option<Hash>,
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bytes_optional"))]
    pub code_hash: Option<Hash>,
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bytes_optional"))]
    pub data_hash: Option<Hash>,
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bytes_optional"))]
    pub creator_address: Option<Hash>,
    pub creator_wc: i8,
    pub created_at: u32,
    pub updated_at: u32,
    pub updated_lt: u64,
}

impl NumBinds for Account {
    const NUM_BINDS: usize = 12;
}

#[derive(Debug, Selectable, Queryable)]
#[diesel(check_for_backend(Mysql))]
#[diesel(table_name = crate::schema::accounts)]
pub struct BriefAccountData {
    pub address: Hash,
    pub state: State,
    pub creator_wc: i8,
    pub creator_address: Option<Hash>,
    pub created_at: u32,
    pub balance: u64,
    pub init_code_hash: Option<Hash>,
    pub code_hash: Option<Hash>,
    pub data_hash: Option<Hash>,
}

impl Account {
    pub fn delete(&mut self) {
        // Leave account state as `Deleted`
        // or set `NonExist` otherwise
        if self.state != State::Deleted {
            self.state = State::NonExist;
        }

        // Reset other state variables
        self.balance = 0;
        self.init_code_hash = None;
        self.code_hash = None;
        self.data_hash = None;
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AccountUpdate {
    pub address: Hash,
    pub wc: i8,
    pub last_transaction_time: u32,
    pub last_transaction_lt: u64,
    pub creator: Option<CreatorInfo>,
    pub state: State,
    pub deleted: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct CreatorInfo {
    pub created_at: u32,
    pub creator_address: Hash,
    pub creator_wc: i8,
}

#[derive(Queryable, Selectable)]
#[diesel(check_for_backend(Mysql))]
#[diesel(table_name = crate::schema::accounts)]
pub struct AccountWithCreatorInfo {
    pub address: Hash,
    pub creator_address: Option<Hash>,
    pub creator_wc: i8,
    pub created_at: u32,
    pub state: State,
}

#[cfg(test)]
mod test {
    use diesel::{BoolExpressionMethods, ExpressionMethods, QueryDsl, SelectableHelper};
    use diesel_async::pooled_connection::mobc::Pool;
    use diesel_async::pooled_connection::AsyncDieselConnectionManager;
    use diesel_async::{AsyncMysqlConnection, RunQueryDsl};

    #[tokio::test]
    async fn aaa() {
        use super::BriefAccountData;
        use crate::schema::accounts::dsl::*;

        let db_client: AsyncDieselConnectionManager<AsyncMysqlConnection> =
            AsyncDieselConnectionManager::new(std::env::var("DATABASE_URL").unwrap());
        let db_client = Pool::builder().build(db_client);

        let workchains = std::iter::repeat(0).take(100_000).collect::<Vec<_>>();
        let addrs = (0..100_000)
            .map(|x| {
                let mut addr = [0u8; 32];
                addr[0] = x as u8;
                addr.to_vec()
            })
            .collect::<Vec<_>>();

        let rand_addrs: Vec<Vec<u8>> = accounts
            .limit(100_000)
            .filter(workchain.eq(0))
            .select(address)
            .load(&mut db_client.get().await.unwrap())
            .await
            .unwrap();

        for zipped in workchains.chunks(31000).zip(rand_addrs.chunks(31000)) {
            let (workchains, addrs) = zipped;
            let query = accounts
                .filter(workchain.eq_any(workchains).and(address.eq_any(addrs)))
                .select(BriefAccountData::as_select())
                .load(&mut db_client.get().await.unwrap())
                .await
                .unwrap();
            println!("Returnd {} rows", query.len());
        }

        // let query = accounts
        //     .filter(workchain.eq_any(workchains).and(address.eq_any(addrs)))
        //     .select(BriefAccountData::as_select())
        //     .load(&mut db_client.get().await.unwrap())
        //     .await
        //     .unwrap();

        // println!("{}", diesel::debug_query::<diesel::mysql::Mysql, _>(&query));
    }
}
