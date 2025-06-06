pub use self::ext_msg_repr::{
    build_normalized_external_message, normalize_external_message, parse_external_message,
    validate_external_message, ExtMsgRepr, InvalidExtMsg, MsgStorageStat,
};

mod ext_msg_repr;
