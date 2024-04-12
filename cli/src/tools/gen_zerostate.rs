use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::Result;
use everscale_crypto::ed25519;
use everscale_types::models::{
    Account, AccountState, CurrencyCollection, OptionalAccount, SpecialFlags, StateInit, StdAddr,
};
use everscale_types::num::Tokens;
use everscale_types::prelude::*;
use serde::{Deserialize, Serialize};
use tycho_util::serde_helpers;

use crate::util::compute_storage_used;

/// Generate a zero state for a network.
#[derive(clap::Parser)]
pub struct Cmd {
    /// dump the template of the zero state config
    #[clap(short = 'i', long, exclusive = true)]
    init_config: Option<String>,

    /// path to the zero state config
    #[clap(required_unless_present = "init_config")]
    config: Option<PathBuf>,

    /// path to the output file
    #[clap(short, long)]
    output: PathBuf,

    /// explicit unix timestamp of the zero state
    #[clap(long)]
    now: Option<u32>,
}

impl Cmd {
    pub fn run(self) -> Result<()> {
        // todo
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct ZerostateConfig {
    global_id: i32,

    #[serde(with = "serde_helpers::public_key")]
    config_public_key: ed25519::PublicKey,
    #[serde(with = "serde_helpers::public_key")]
    minter_public_key: ed25519::PublicKey,

    #[serde(with = "serde_account_states")]
    accounts: HashMap<HashBytes, OptionalAccount>,
}

#[derive(Serialize, Deserialize)]
struct BlockchainConfig {
    #[serde(with = "serde_helpers::hex_byte_array")]
    config_address: HashBytes,
    #[serde(with = "serde_helpers::hex_byte_array")]
    elector_address: HashBytes,
    #[serde(with = "serde_helpers::hex_byte_array")]
    minter_address: HashBytes,
    // TODO: additional currencies
    global_version: u32,
    global_capabilities: u64,
}

fn build_minter_account(pubkey: &ed25519::PublicKey) -> Result<Account> {
    const MINTER_STATE: &[u8] = include_bytes!("../../res/minter_state.boc");

    let mut account = BocRepr::decode::<OptionalAccount, _>(MINTER_STATE)?
        .0
        .expect("invalid minter state");

    match &mut account.state {
        AccountState::Active(state_init) => {
            let mut data = CellBuilder::new();

            // Append pubkey first
            data.store_u256(HashBytes::wrap(pubkey.as_bytes()))?;

            // Append everything except the pubkey
            let prev_data = state_init
                .data
                .take()
                .expect("minter state must contain data");
            let mut prev_data = prev_data.as_slice()?;
            prev_data.advance(256, 0)?;

            data.store_slice(prev_data)?;

            // Update data
            state_init.data = Some(data.build()?);
        }
        _ => unreachable!("saved state is for the active account"),
    };

    account.balance = CurrencyCollection::ZERO;
    account.storage_stat.used = compute_storage_used(&account)?;

    Ok(account)
}

fn build_config_account(
    pubkey: &ed25519::PublicKey,
    address: &HashBytes,
    balance: Tokens,
) -> Result<Account> {
    const CONFIG_CODE: &[u8] = include_bytes!("../../res/config_code.boc");

    let code = Boc::decode(CONFIG_CODE)?;

    let mut data = CellBuilder::new();
    data.store_reference(Cell::empty_cell())?;
    data.store_u32(0)?;
    data.store_u256(HashBytes::wrap(pubkey.as_bytes()))?;
    data.store_bit_zero()?;
    let data = data.build()?;

    let mut account = Account {
        address: StdAddr::new(-1, *address).into(),
        storage_stat: Default::default(),
        last_trans_lt: 0,
        balance: balance.into(),
        state: AccountState::Active(StateInit {
            split_depth: None,
            special: Some(SpecialFlags {
                tick: false,
                tock: true,
            }),
            code: Some(code),
            data: Some(data),
            libraries: Dict::new(),
        }),
        init_code_hash: None,
    };

    account.storage_stat.used = compute_storage_used(&account)?;

    Ok(account)
}

fn build_elector_code(address: &HashBytes, balance: Tokens) -> Result<Account> {
    const ELECTOR_CODE: &[u8] = include_bytes!("../../res/elector_code.boc");

    let code = Boc::decode(ELECTOR_CODE)?;

    let mut data = CellBuilder::new();
    data.store_small_uint(0, 3)?; //empty dict, empty dict, empty dict
    data.store_small_uint(0, 4)?; // tokens
    data.store_u32(0)?; // elections id
    data.store_zeros(256)?; // elections hash
    let data = data.build()?;

    let mut account = Account {
        address: StdAddr::new(-1, *address).into(),
        storage_stat: Default::default(),
        last_trans_lt: 0,
        balance: balance.into(),
        state: AccountState::Active(StateInit {
            split_depth: None,
            special: Some(SpecialFlags {
                tick: true,
                tock: false,
            }),
            code: Some(code),
            data: Some(data),
            libraries: Dict::new(),
        }),
        init_code_hash: None,
    };

    account.storage_stat.used = compute_storage_used(&account)?;

    Ok(account)
}

mod serde_account_states {
    use std::collections::HashMap;

    use everscale_types::boc::BocRepr;
    use everscale_types::cell::HashBytes;
    use everscale_types::models::OptionalAccount;
    use serde::de::Deserializer;
    use serde::ser::{SerializeMap, Serializer};
    use serde::{Deserialize, Serialize};
    use tycho_util::serde_helpers;

    pub fn serialize<S>(
        value: &HashMap<HashBytes, OptionalAccount>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        #[repr(transparent)]
        struct WrapperKey<'a>(#[serde(with = "serde_helpers::hex_byte_array")] &'a [u8; 32]);

        #[derive(Serialize)]
        #[repr(transparent)]
        struct WrapperValue<'a>(
            #[serde(serialize_with = "BocRepr::serialize")] &'a OptionalAccount,
        );

        let mut ser = serializer.serialize_map(Some(value.len()))?;
        for (key, value) in value {
            ser.serialize_entry(&WrapperKey(key.as_array()), &WrapperValue(value))?;
        }
        ser.end()
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<HashMap<HashBytes, OptionalAccount>, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize, Hash, PartialEq, Eq)]
        #[repr(transparent)]
        struct WrapperKey(#[serde(with = "serde_helpers::hex_byte_array")] [u8; 32]);

        #[derive(Deserialize)]
        #[repr(transparent)]
        struct WrappedValue(#[serde(with = "BocRepr")] OptionalAccount);

        <HashMap<WrapperKey, WrappedValue>>::deserialize(deserializer).map(|map| {
            map.into_iter()
                .map(|(k, v)| (HashBytes(k.0), v.0))
                .collect()
        })
    }
}
