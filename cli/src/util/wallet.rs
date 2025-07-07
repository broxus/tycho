use std::sync::OnceLock;

use tycho_crypto::ed25519;
use tycho_types::abi::{AbiHeaderType, AbiVersion, Function, IntoAbi, WithAbiType};
use tycho_types::models::{StateInit, StdAddr};
use tycho_types::prelude::*;

pub const MSG_FLAGS_SIMPLE_SEND: u8 = 3;
pub const MSG_FLAGS_SEPARATE_SEND: u8 = 2;
pub const MSG_FLAGS_SEND_ALL: u8 = 3 + 128;

pub fn compute_address(workchain: i8, public_key: &ed25519::PublicKey) -> StdAddr {
    let address = *CellBuilder::build_from(make_state_init(public_key))
        .unwrap()
        .repr_hash();
    StdAddr::new(workchain, address)
}

pub fn make_state_init(public_key: &ed25519::PublicKey) -> StateInit {
    let data = CellBuilder::build_from((HashBytes(public_key.to_bytes()), 0u64)).unwrap();
    StateInit {
        split_depth: None,
        special: None,
        code: Some(code().clone()),
        data: Some(data),
        libraries: Dict::new(),
    }
}

pub fn code() -> &'static Cell {
    static CODE: OnceLock<Cell> = OnceLock::new();
    CODE.get_or_init(|| Boc::decode(include_bytes!("../../res/ever_wallet_code.boc")).unwrap())
}

pub mod methods {
    use super::*;

    pub fn send_transaction() -> &'static Function {
        static FUNCTION: OnceLock<Function> = OnceLock::new();
        FUNCTION.get_or_init(move || {
            Function::builder(AbiVersion::V2_3, "sendTransaction")
                .with_headers([
                    AbiHeaderType::PublicKey,
                    AbiHeaderType::Time,
                    AbiHeaderType::Expire,
                ])
                .with_inputs(SendTransactionInputs::abi_type().named("").flatten())
                .build()
        })
    }

    #[derive(Debug, Clone, WithAbiType, IntoAbi)]
    pub struct SendTransactionInputs {
        pub dest: StdAddr,
        pub value: u128,
        pub bounce: bool,
        pub flags: u8,
        pub payload: Cell,
    }
}
