use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use anyhow::Result;
use everscale_types::abi::{
    AbiType, AbiValue, AbiVersion, FromAbi, Function, IntoAbi, WithAbiType,
};
use everscale_types::prelude::*;
use serde::{Deserialize, Serialize};

use crate::util::FpTokens;

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

    #[derive(Debug, Clone, IntoAbi, WithAbiType)]
    pub struct ParticiateInElectionsInput {
        pub query_id: u64,
        pub validator_key: HashBytes,
        pub stake_at: u32,
        pub stake_factor: u32,
        pub adnl_addr: HashBytes,
        pub signature: Vec<u8>,
    }
}

pub mod data {
    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize, WithAbiType, FromAbi)]
    pub struct PartialElectorData {
        pub current_election: Option<Ref<CurrentElectionData>>,
        pub credits: BTreeMap<HashBytes, FpTokens>,
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

    #[derive(Debug, Clone, Serialize, Deserialize, WithAbiType, FromAbi)]
    pub struct CurrentElectionData {
        pub elect_at: u32,
        pub elect_close: u32,
        pub min_stake: FpTokens,
        pub total_stake: FpTokens,
        pub members: BTreeMap<HashBytes, ElectionMember>,
        pub failed: bool,
        pub finished: bool,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, WithAbiType, FromAbi)]
    pub struct ElectionMember {
        pub msg_value: FpTokens,
        pub created_at: u32,
        pub stake_factor: u32,
        pub src_addr: HashBytes,
        pub adnl_addr: HashBytes,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, WithAbiType, FromAbi)]
    pub struct PastElectionData {
        pub unfreeze_at: u32,
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

#[cfg(test)]
pub mod tests {
    use std::collections::BTreeMap;
    use std::str::FromStr;
    use std::sync::Arc;

    use bytes::Bytes;
    use everscale_types::abi::{
        AbiType, AbiValue, FromAbi, IntoAbi, PlainAbiType, PlainAbiValue, WithAbiType,
    };
    use everscale_types::cell::HashBytes;
    use everscale_types::num::Tokens;
    use num_bigint::BigUint;
    use serde::{Deserialize, Serialize};

    use crate::util::elector::data::{CurrentElectionData, ElectionMember, Ref};
    use crate::util::elector::methods::ParticiateInElectionsInput;
    use crate::util::FpTokens;

    #[test]
    fn test_participate_in_elections_input() {
        let input = ParticiateInElectionsInput {
            query_id: 322,
            validator_key: HashBytes::default(),
            stake_at: 1,
            stake_factor: 2,
            adnl_addr: HashBytes::from_str(
                "2b4c1a29d2fc2320637f37904e148475efbc1716036274d994efaccce9868234",
            )
            .unwrap(),
            signature: vec![1, 2, 3, 4, 5],
        };

        let abi_value_manual = AbiValue::tuple([
            input.query_id.into_abi().named("query_id"),
            input.validator_key.into_abi().named("validator_key"),
            input.stake_at.into_abi().named("stake_at"),
            input.stake_factor.into_abi().named("stake_factor"),
            input.adnl_addr.into_abi().named("adnl_addr"),
            input.signature.as_abi().named("signature"),
        ]);

        let abi_type_manual = AbiType::tuple([
            u64::abi_type().named("query_id"),
            HashBytes::abi_type().named("validator_key"),
            u32::abi_type().named("stake_at"),
            u32::abi_type().named("stake_factor"),
            HashBytes::abi_type().named("adnl_addr"),
            Bytes::abi_type().named("signature"),
        ]);

        let abi_type = ParticiateInElectionsInput::abi_type();

        assert_eq!(abi_type_manual, abi_type);

        let as_abi_value = input.as_abi();
        let into_abi_value = input.into_abi();

        assert_eq!(abi_value_manual, as_abi_value);
        assert_eq!(into_abi_value, as_abi_value);
    }

    #[test]
    fn test_partial_elector_data() {
        #[derive(Debug, Clone, Serialize, Deserialize, WithAbiType, FromAbi)]
        pub struct PartialElectorDataShort {
            pub current_election: Option<Ref<CurrentElectionData>>,
        }

        let mut members = BTreeMap::new();
        members.insert(
            HashBytes::from_str("2b4c1a29d2fc2320637f37904e148475efbc1716036274d994efaccce9868234")
                .unwrap(),
            ElectionMember {
                msg_value: FpTokens(100_000),
                created_at: 1,
                stake_factor: 2,
                src_addr: Default::default(),
                adnl_addr: Default::default(),
            },
        );

        let initial_current = Ref(CurrentElectionData {
            elect_at: 1,
            elect_close: 2,
            min_stake: FpTokens(123),
            total_stake: FpTokens(456),
            members,
            failed: false,
            finished: true,
        });

        let abi_type = PartialElectorDataShort::abi_type();
        let manual_abi_type = AbiType::tuple([
            Option::<Ref<CurrentElectionData>>::abi_type().named("current_election")
        ]);

        let map_inner = [
            AbiType::Token.named("msg_value"),
            AbiType::Uint(32).named("created_at"),
            AbiType::Uint(32).named("stake_factor"),
            AbiType::Bytes.named("src_addr"),
            AbiType::Bytes.named("adnl_addr"),
        ];

        let first_value_inner_type = [
            AbiType::Uint(32).named("elect_at"),
            AbiType::Uint(32).named("elect_close"),
            AbiType::Token.named("min_stake"),
            AbiType::Token.named("total_stake"),
            AbiType::Map(
                PlainAbiType::Uint(256),
                Arc::new(AbiType::Tuple(Arc::from(map_inner))),
            )
            .named("members"),
        ];

        let first_type = AbiType::Ref(Arc::new(AbiType::Tuple(Arc::from(first_value_inner_type))));

        let map_inner_type = [
            AbiType::Token.named("msg_value"),
            AbiType::Uint(32).named("created_at"),
            AbiType::Uint(32).named("stake_factor"),
            AbiType::Uint(256).named("src_addr"),
            AbiType::Uint(256).named("adnl_addr"),
        ];

        let mut map_inner_value = BTreeMap::new();
        map_inner_value.insert(
            PlainAbiValue::Uint(
                256,
                BigUint::from_bytes_be(
                    b"1b4c1a29d2fc2320637f37904e148475efbc1716036274d994efaccce9868234",
                ),
            ),
            AbiValue::tuple([
                AbiValue::Token(Tokens::new(100_000)).named("msg_value"),
                AbiValue::uint(32, 1u32).named("created_at"),
                AbiValue::uint(32, 2u32).named("stake_factor"),
                AbiValue::Uint(256, BigUint::from(0u32)).named("src_addr"),
                AbiValue::Uint(256, BigUint::from(0u32)).named("adnl_addr"),
            ]),
        );

        let first_value_inner = vec![
            AbiValue::uint(32u16, 1u32).named("elect_at"),
            AbiValue::uint(32u16, 2u32).named("elect_at"),
            AbiValue::Token(Tokens::new(123)).named("min_stake"),
            AbiValue::Token(Tokens::new(456)).named("total_stake"),
            AbiValue::Map(
                PlainAbiType::Uint(256),
                Arc::new(AbiType::Tuple(Arc::from(map_inner_type))),
                map_inner_value,
            )
            .named("members"),
            AbiValue::Bool(false).named("failed"),
            AbiValue::Bool(true).named("finished"),
        ];

        let first_value = AbiValue::Ref(Box::new(AbiValue::Tuple(first_value_inner)));

        let abi_value_manual = AbiValue::Tuple(vec![AbiValue::Optional(
            Arc::new(first_type),
            Some(Box::new(first_value)),
        )
        .named("current_election")]);

        let new = PartialElectorDataShort::from_abi(abi_value_manual).unwrap();
        assert!(new.current_election.is_some());

        let current_election = new.current_election.unwrap();

        assert_eq!(current_election.0.elect_at, initial_current.0.elect_at);
        assert_eq!(
            current_election.0.elect_close,
            initial_current.0.elect_close
        );
        assert_eq!(current_election.0.failed, initial_current.0.failed);
        assert_eq!(current_election.0.finished, initial_current.0.finished);
        assert_eq!(
            current_election.0.total_stake,
            initial_current.0.total_stake
        );
        assert_eq!(current_election.0.min_stake, initial_current.0.min_stake);

        assert_eq!(manual_abi_type, abi_type);
    }
}
