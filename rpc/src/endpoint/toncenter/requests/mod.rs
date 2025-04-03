use everscale_types::models::StdAddr;
use serde::Deserialize;

#[derive(Deserialize)]
pub(crate) struct GetTransactionsQuery {
    pub(crate) address: StdAddr,
    pub(crate) limit: Option<u8>,
    pub(crate) lt: Option<u64>,
    #[allow(unused)]
    pub(crate) to_lt: Option<u64>,
    #[allow(unused)]
    pub(crate) hash: Option<String>,
    #[allow(unused)]
    pub(crate) archival: Option<bool>,
}

#[derive(Deserialize)]
pub(crate) struct AddressInformationQuery {
    pub address: StdAddr,
}

#[derive(Debug, Copy, Clone, Deserialize, Eq, PartialEq, Hash)]
pub(crate) enum Direction {
    #[serde(rename = "asc")]
    Ascending,
    #[serde(rename = "desc")]
    Descending,
}

#[derive(Default, Debug, Clone, PartialEq, Deserialize)]
pub struct SendMessageRequest {
    pub boc: String,
}

#[derive(Deserialize)]
pub(crate) struct ExecMethodArgs {
    pub address: StdAddr,
    pub method: String,
    pub stack: Vec<Vec<String>>,
    #[allow(dead_code)]
    pub seqno: i64,
}

#[derive(Deserialize)]
#[allow(unused)]
pub(crate) struct JsonRpcMethodArgs {
    pub id: Option<String>,
    pub method: String,
    pub jsonrpc: Option<String>,
    pub params: serde_json::Value,
}
