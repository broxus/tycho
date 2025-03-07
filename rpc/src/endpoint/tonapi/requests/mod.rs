use serde::Deserialize;

#[derive(Deserialize)]
pub(crate) struct Pagination {
    pub(crate) after_lt: Option<u64>,
    #[allow(unused)]
    pub(crate) before_lt: Option<u64>,
    pub(crate) limit: Option<u8>,
    #[allow(unused)]
    pub(crate) sort_order: Option<Direction>,
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
    pub batch: Option<Vec<String>>,
    pub meta: Option<serde_json::Value>,
}

#[derive(Deserialize)]
pub(crate) struct ExecMethodArgs {
    #[serde(default)]
    #[allow(unused)]
    pub(crate) args: Vec<String>,
    #[serde(default)]
    #[allow(unused)]
    pub(crate) fix_order: bool,
}
