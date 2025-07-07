pub use self::ext_msg_repr::{
    ExtMsgRepr, InvalidExtMsg, MsgStorageStat, build_normalized_external_message,
    normalize_external_message, parse_external_message, validate_external_message,
};

mod ext_msg_repr;
