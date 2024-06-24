use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize, diesel_derive_enum::DbEnum)]
#[DbValueStyle = "PascalCase"]
pub enum State {
    NonExist,
    Uninit,
    Active,
    Frozen,
    Deleted,
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, diesel_derive_enum::DbEnum)]
#[DbValueStyle = "PascalCase"]
pub enum MessageType {
    Internal,
    ExternalIn,
    ExternalOut,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, diesel_derive_enum::DbEnum)]
#[DbValueStyle = "PascalCase"]
pub enum TransactionType {
    Ordinary,
    Storage,
    TickTock,
    SplitPrepare,
    SplitInstall,
    MergePrepare,
    MergeInstall,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, diesel_derive_enum::DbEnum)]
#[DbValueStyle = "PascalCase"]
pub enum ParsedType {
    FunctionInput,
    FunctionOutput,
    BouncedFunction,
    Event,
}
