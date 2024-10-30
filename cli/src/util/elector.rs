use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use anyhow::Result;
use bytes::Bytes;
use everscale_types::abi::{
    AbiType, AbiValue, AbiVersion, FromAbi, Function, IntoAbi, WithAbiType,
};
use everscale_types::num::Tokens;
use everscale_types::prelude::*;
use serde::{Deserialize, Serialize};

pub mod methods {
    use super::*;

    pub fn recover_stake() -> &'static Function {
        static FUNCTION: OnceLock<Function> = OnceLock::new();
        FUNCTION.get_or_init(|| {
            Function::builder(AbiVersion::V2_0, "recover_stake")
                .with_id(0x47657424)
                .with_inputs([u64::abi_type().named("query_id")])
                .build()
        })
    }

    pub fn participate_in_elections() -> &'static Function {
        static FUNCTION: OnceLock<Function> = OnceLock::new();
        FUNCTION.get_or_init(|| {
            Function::builder(AbiVersion::V2_0, "participate_in_elections")
                .with_id(0x4e73744b)
                .with_inputs([ParticiateInElectionsInput::abi_type().named("input")])
                .build()
        })
    }

    #[derive(Debug, Clone)]
    pub struct ParticiateInElectionsInput {
        pub query_id: u64,
        pub validator_key: HashBytes,
        pub stake_at: u32,
        pub max_factor: u32,
        pub adnl_addr: HashBytes,
        pub signature: Vec<u8>,
    }

    // TODO: Replace with macros
    impl WithAbiType for ParticiateInElectionsInput {
        fn abi_type() -> AbiType {
            AbiType::tuple([
                u64::abi_type().named("query_id"),
                HashBytes::abi_type().named("validator_key"),
                u32::abi_type().named("stake_at"),
                u32::abi_type().named("max_factor"),
                HashBytes::abi_type().named("adnl_addr"),
                Bytes::abi_type().named("signature"),
            ])
        }
    }

    // TODO: Replace with macros
    impl IntoAbi for ParticiateInElectionsInput {
        fn as_abi(&self) -> AbiValue {
            AbiValue::tuple([
                self.query_id.into_abi().named("query_id"),
                self.validator_key.into_abi().named("validator_key"),
                self.stake_at.into_abi().named("stake_at"),
                self.max_factor.into_abi().named("max_factor"),
                self.adnl_addr.into_abi().named("adnl_addr"),
                // TODO: Just use `self.signature.as_abi()` when the fix is merged.
                AbiValue::bytes(self.signature.clone()).named("signature"),
            ])
        }

        fn into_abi(self) -> everscale_types::abi::AbiValue
        where
            Self: Sized,
        {
            self.as_abi()
        }
    }
}

pub mod data {
    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct PartialElectorData {
        pub current_election: Option<Ref<CurrentElectionData>>,
        pub credits: BTreeMap<HashBytes, Tokens>,
        pub past_elections: BTreeMap<u32, PastElectionData>,
    }

    impl PartialElectorData {
        pub fn nearest_unfreeze_at(&self, time: u32) -> Option<u32> {
            self.past_elections
                .values()
                .map(|election| election.unfreeze_at)
                .find(|&unfreeze_at| unfreeze_at < time)
        }
    }

    // TODO: Replace with macros
    impl WithAbiType for PartialElectorData {
        fn abi_type() -> AbiType {
            AbiType::tuple([
                Option::<Ref<CurrentElectionData>>::abi_type().named("current_election"),
                BTreeMap::<HashBytes, Tokens>::abi_type().named("credits"),
                BTreeMap::<u32, PastElectionData>::abi_type().named("past_elections"),
            ])
        }
    }

    // TODO: Replace with macros
    impl FromAbi for PartialElectorData {
        fn from_abi(value: AbiValue) -> Result<Self> {
            let (current_election, credits, past_elections) = <_>::from_abi(value)?;
            Ok(Self {
                current_election,
                credits,
                past_elections,
            })
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct CurrentElectionData {
        pub elect_at: u32,
        pub elect_close: u32,
        pub min_stake: Tokens,
        pub total_stake: Tokens,
        pub members: BTreeMap<HashBytes, ElectionMember>,
        pub failed: bool,
        pub finished: bool,
    }

    // TODO: Replace with macros
    impl WithAbiType for CurrentElectionData {
        fn abi_type() -> AbiType {
            AbiType::tuple([
                u32::abi_type().named("elect_at"),
                u32::abi_type().named("elect_close"),
                Tokens::abi_type().named("min_stake"),
                Tokens::abi_type().named("total_stake"),
                BTreeMap::<HashBytes, ElectionMember>::abi_type().named("members"),
                bool::abi_type().named("failed"),
                bool::abi_type().named("finished"),
            ])
        }
    }

    // TODO: Replace with macros
    impl FromAbi for CurrentElectionData {
        fn from_abi(value: AbiValue) -> Result<Self> {
            let (elect_at, elect_close, min_stake, total_stake, members, failed, finished) =
                <_>::from_abi(value)?;

            Ok(Self {
                elect_at,
                elect_close,
                min_stake,
                total_stake,
                members,
                failed,
                finished,
            })
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ElectionMember {
        pub msg_value: Tokens,
        pub created_at: u32,
        pub max_factor: u32,
        pub src_addr: HashBytes,
        pub adnl_addr: HashBytes,
    }

    // TODO: Replace with macros
    impl WithAbiType for ElectionMember {
        fn abi_type() -> AbiType {
            AbiType::tuple([
                Tokens::abi_type().named("msg_value"),
                u32::abi_type().named("created_at"),
                u32::abi_type().named("max_factor"),
                HashBytes::abi_type().named("src_addr"),
                HashBytes::abi_type().named("adnl_addr"),
            ])
        }
    }

    // TODO: Replace with macros
    impl FromAbi for ElectionMember {
        fn from_abi(value: AbiValue) -> Result<Self> {
            let (msg_value, created_at, max_factor, src_addr, adnl_addr) = <_>::from_abi(value)?;

            Ok(Self {
                msg_value,
                created_at,
                max_factor,
                src_addr,
                adnl_addr,
            })
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct PastElectionData {
        pub unfreeze_at: u32,
    }

    // TODO: Replace with macros
    impl WithAbiType for PastElectionData {
        fn abi_type() -> AbiType {
            AbiType::tuple([u32::abi_type().named("unfreeze_at")])
        }
    }

    // TODO: Replace with macros
    impl FromAbi for PastElectionData {
        fn from_abi(value: AbiValue) -> Result<Self> {
            let (unfreeze_at,) = <(u32,)>::from_abi(value)?;
            Ok(Self { unfreeze_at })
        }
    }

    // TODO: Move into `everscale-types`?
    #[repr(transparent)]
    pub struct Ref<T>(pub T);

    impl<T: Clone> Clone for Ref<T> {
        #[inline]
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }

    impl<T: std::fmt::Debug> std::fmt::Debug for Ref<T> {
        #[inline]
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_tuple("Ref").field(&self.0).finish()
        }
    }

    impl<T: WithAbiType> WithAbiType for Ref<T> {
        fn abi_type() -> AbiType {
            AbiType::Ref(Arc::new(T::abi_type()))
        }
    }

    impl<T: FromAbi> FromAbi for Ref<T> {
        fn from_abi(value: AbiValue) -> Result<Self> {
            match value {
                AbiValue::Ref(value) => T::from_abi(*value).map(Self),
                value => {
                    anyhow::bail!(everscale_types::abi::error::AbiError::TypeMismatch {
                        expected: Box::from("ref"),
                        ty: value.display_type().to_string().into(),
                    })
                }
            }
        }
    }

    impl<T: Serialize> Serialize for Ref<T> {
        #[inline]
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            self.0.serialize(serializer)
        }
    }

    impl<'de, T: Deserialize<'de>> Deserialize<'de> for Ref<T> {
        #[inline]
        fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            T::deserialize(deserializer).map(Self)
        }
    }
}
