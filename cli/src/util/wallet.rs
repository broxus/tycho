use std::sync::OnceLock;

use everscale_crypto::ed25519;
use everscale_types::abi::{AbiType, AbiValue, Function, IntoAbi, WithAbiType};
use everscale_types::models::{StateInit, StdAddr};
use everscale_types::prelude::*;

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
        FUNCTION.get_or_init(move || todo!())
    }

    #[derive(Debug, Clone)]
    pub struct SendTransactionInputs {
        pub dest: StdAddr,
        pub value: u128,
        pub bounce: bool,
        pub flags: u8,
        pub payload: Cell,
    }

    // TODO: Replace with macros
    impl WithAbiType for SendTransactionInputs {
        fn abi_type() -> AbiType {
            AbiType::tuple([
                StdAddr::abi_type().named("dest"),
                u128::abi_type().named("value"),
                bool::abi_type().named("bounce"),
                u8::abi_type().named("flags"),
                Cell::abi_type().named("payload"),
            ])
        }
    }

    // TODO: Replace with macros
    impl IntoAbi for SendTransactionInputs {
        fn as_abi(&self) -> AbiValue {
            AbiValue::tuple([
                self.dest.as_abi().named("dest"),
                self.value.into_abi().named("value"),
                self.bounce.into_abi().named("bounce"),
                self.flags.into_abi().named("flags"),
                self.payload.as_abi().named("payload"),
            ])
        }

        fn into_abi(self) -> AbiValue
        where
            Self: Sized,
        {
            self.as_abi()
        }
    }
}
