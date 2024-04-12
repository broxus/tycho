use std::io::IsTerminal;

use anyhow::{Context, Result};
use everscale_crypto::ed25519;
use everscale_types::models::{
    Account, AccountState, OptionalAccount, StateInit, StdAddr, StorageUsed,
};
use everscale_types::num::{Tokens, VarUint56};
use everscale_types::prelude::*;

use crate::util::parse_public_key;

/// Generate an account state
#[derive(clap::Parser)]
pub struct Cmd {
    #[clap(subcommand)]
    cmd: SubCmd,
}

impl Cmd {
    pub fn run(self) -> Result<()> {
        self.cmd.run()
    }
}

#[derive(clap::Subcommand)]
enum SubCmd {
    Wallet(WalletCmd),
    Multisig(MultisigCmd),
    Giver(GiverCmd),
}

impl SubCmd {
    fn run(self) -> Result<()> {
        match self {
            Self::Wallet(cmd) => cmd.run(),
            Self::Multisig(cmd) => cmd.run(),
            Self::Giver(cmd) => cmd.run(),
        }
    }
}

/// Generate a simple wallet state
#[derive(clap::Parser)]
struct WalletCmd {
    /// account public key
    #[clap(short, long, required = true)]
    pubkey: String,

    /// initial balance of the wallet (in nano)
    #[clap(short, long, required = true)]
    balance: Tokens,
}

impl WalletCmd {
    fn run(self) -> Result<()> {
        let pubkey =
            parse_public_key(self.pubkey.as_bytes(), false).context("invalid deployer pubkey")?;

        let (account, state) = WalletBuilder {
            pubkey,
            balance: self.balance,
        }
        .build()?;

        write_state(&account, &state)
    }
}

/// Generate a multisig wallet state
#[derive(clap::Parser)]
struct MultisigCmd {
    /// account public key
    #[clap(short, long, required = true)]
    pubkey: String,

    /// initial balance of the wallet (in nano)
    #[clap(short, long, required = true)]
    balance: Tokens,

    /// list of custodian public keys
    #[clap(short, long)]
    custodians: Vec<String>,

    /// Number of required confirmations
    #[clap(short, long)]
    req_confirms: Option<u8>,

    /// Custom lifetime of the wallet
    #[clap(short, long)]
    lifetime: Option<u32>,

    /// Use SetcodeMultisig instead of SafeMultisig
    #[clap(short, long)]
    updatable: bool,
}

impl MultisigCmd {
    fn run(self) -> Result<()> {
        let pubkey =
            parse_public_key(self.pubkey.as_bytes(), false).context("invalid deployer pubkey")?;

        let custodians = self
            .custodians
            .iter()
            .map(|key| parse_public_key(key.as_bytes(), false))
            .collect::<Result<Vec<_>>>()
            .context("invalid custodian pubkey")?;

        let (account, state) = MultisigBuilder {
            pubkey,
            custodians,
            updatable: self.updatable,
            required_confirms: self.req_confirms,
            lifetime: self.lifetime,
            balance: self.balance,
        }
        .build()?;

        write_state(&account, &state)
    }
}

/// Generate a giver state
#[derive(clap::Parser)]
struct GiverCmd {
    /// account public key
    #[clap(short, long, required = true)]
    pubkey: String,

    /// initial balance of the giver (in nano)
    #[clap(short, long, required = true)]
    balance: Tokens,
}

impl GiverCmd {
    fn run(self) -> Result<()> {
        let pubkey =
            parse_public_key(self.pubkey.as_bytes(), false).context("invalid deployer pubkey")?;

        let (account, state) = GiverBuilder {
            pubkey,
            balance: self.balance,
        }
        .build()?;

        write_state(&account, &state)
    }
}

fn write_state(account: &HashBytes, state: &Account) -> Result<()> {
    let res = serde_json::json!({
        "account": account.to_string(),
        "boc": BocRepr::encode_base64(OptionalAccount(Some(state.clone())))?,
    });

    let output = if std::io::stdin().is_terminal() {
        serde_json::to_string_pretty(&res)
    } else {
        serde_json::to_string(&res)
    }?;
    println!("{}", output);
    Ok(())
}

struct WalletBuilder {
    pubkey: ed25519::PublicKey,
    balance: Tokens,
}

impl WalletBuilder {
    fn build(self) -> Result<(HashBytes, Account)> {
        const EVER_WALLET_CODE: &[u8] = include_bytes!("../../res/ever_wallet_code.boc");

        let data = CellBuilder::build_from((HashBytes::wrap(self.pubkey.as_bytes()), 0u64))?;
        let code = Boc::decode(EVER_WALLET_CODE)?;

        let state_init = StateInit {
            split_depth: None,
            special: None,
            code: Some(code),
            data: Some(data),
            libraries: Dict::new(),
        };
        let address = *CellBuilder::build_from(&state_init)?.repr_hash();

        let mut account = Account {
            address: StdAddr::new(-1, address).into(),
            storage_stat: Default::default(),
            last_trans_lt: 0,
            balance: self.balance.into(),
            state: AccountState::Active(state_init),
            init_code_hash: None,
        };

        account.storage_stat.used = compute_storage_used(&account)?;

        Ok((address, account))
    }
}

struct MultisigBuilder {
    pubkey: ed25519::PublicKey,
    custodians: Vec<ed25519::PublicKey>,
    updatable: bool,
    required_confirms: Option<u8>,
    lifetime: Option<u32>,
    balance: Tokens,
}

impl MultisigBuilder {
    fn build(mut self) -> Result<(HashBytes, Account)> {
        const DEFAULT_LIFETIME: u32 = 3600;
        const MIN_LIFETIME: u32 = 600;

        // Multisig2
        const SAFE_MULTISIG_CODE: &[u8] = include_bytes!("../../res/safe_multisig_code.boc");
        // SetcodeMultisig (old)
        const SETCODE_MULTISIG_CODE: &[u8] = include_bytes!("../../res/setcode_multisig_code.boc");

        if let Some(lifetime) = self.lifetime {
            anyhow::ensure!(
                !self.updatable,
                "custom lifetime is not supported by SetcodeMultisig",
            );
            anyhow::ensure!(
                lifetime >= MIN_LIFETIME,
                "transaction lifetime is too short",
            );
        }

        let code = Boc::decode(match self.updatable {
            false => SAFE_MULTISIG_CODE,
            true => SETCODE_MULTISIG_CODE,
        })
        .expect("invalid contract code");

        let custodian_count = match self.custodians.len() {
            0 => {
                self.custodians.push(self.pubkey);
                1 // set deployer as the single custodian
            }
            len @ 1..=32 => len as u8,
            _ => anyhow::bail!("too many custodians"),
        };

        // All confirmations are required if it wasn't explicitly specified
        let required_confirms = self.required_confirms.unwrap_or(custodian_count);

        // Compute address
        let data = {
            let mut init_params = Dict::<u64, CellSlice<'_>>::new();

            let pubkey_cell = CellBuilder::build_from(HashBytes::wrap(self.pubkey.as_bytes()))?;
            init_params.set(0, pubkey_cell.as_slice()?)?;

            let garbage_cell;
            if self.updatable {
                // Set some garbage for the SetcodeMultisig to match
                // the commonly used `tvc` file.
                garbage_cell = {
                    let mut garbage_dict = Dict::<u64, ()>::new();
                    garbage_dict.set(0, ())?;
                    CellBuilder::build_from(garbage_dict)?
                };
                init_params.set(8, garbage_cell.as_slice()?)?;
            }

            CellBuilder::build_from(init_params)?
        };

        let mut state_init = StateInit {
            split_depth: None,
            special: None,
            code: Some(code),
            data: Some(data),
            libraries: Dict::new(),
        };
        let address = *CellBuilder::build_from(&state_init)?.repr_hash();

        // Compute runtime data
        let owner_key = HashBytes::wrap(self.custodians.first().unwrap_or(&self.pubkey).as_bytes());

        let mut custodians = Dict::<HashBytes, u8>::new();
        for (i, custodian) in self.custodians.iter().enumerate() {
            custodians.set(HashBytes::wrap(custodian.as_bytes()), i as u8)?;
        }

        let default_required_confirmations = std::cmp::min(required_confirms, custodian_count);

        let required_votes = if custodian_count <= 2 {
            custodian_count
        } else {
            (custodian_count * 2 + 1) / 3
        };

        let mut data = CellBuilder::new();

        // Write headers
        data.store_u256(HashBytes::wrap(self.pubkey.as_bytes()))?;
        data.store_u64(0)?; // time
        data.store_bit_one()?; // constructor flag

        // Write state variables
        match self.updatable {
            false => {
                data.store_u256(owner_key)?; // m_ownerKey
                data.store_u256(&HashBytes::ZERO)?; // m_requestsMask
                data.store_bit_zero()?; // empty m_transactions
                custodians.store_into(&mut data, &mut Cell::empty_context())?; // m_custodians
                data.store_u8(custodian_count)?; // m_custodianCount
                data.store_bit_zero()?; // empty m_updateRequests
                data.store_u32(0)?; // m_updateRequestsMask
                data.store_u8(required_votes)?; // m_requiredVotes
                data.store_u8(default_required_confirmations)?; // m_defaultRequiredConfirmations
                data.store_u32(self.lifetime.unwrap_or(DEFAULT_LIFETIME))?;
            }
            true => {
                data.store_u256(owner_key)?; // m_ownerKey
                data.store_u256(&HashBytes::ZERO)?; // m_requestsMask
                data.store_u8(custodian_count)?; // m_custodianCount
                data.store_u32(0)?; // m_updateRequestsMask
                data.store_u8(required_votes)?; // m_requiredVotes

                let mut updates = CellBuilder::new();
                updates.store_bit_zero()?; // empty m_updateRequests
                data.store_reference(updates.build()?)?; // sub reference

                data.store_u8(default_required_confirmations)?; // m_defaultRequiredConfirmations
                data.store_bit_zero()?; // empty m_transactions
                custodians.store_into(&mut data, &mut Cell::empty_context())?; // m_custodians
            }
        };

        // "Deploy" wallet
        state_init.data = Some(data.build()?);

        // Done
        let mut account = Account {
            address: StdAddr::new(-1, address).into(),
            storage_stat: Default::default(),
            last_trans_lt: 0,
            balance: self.balance.into(),
            state: AccountState::Active(state_init),
            init_code_hash: None,
        };

        account.storage_stat.used = compute_storage_used(&account)?;

        Ok((address, account))
    }
}

struct GiverBuilder {
    pubkey: ed25519::PublicKey,
    balance: Tokens,
}

impl GiverBuilder {
    fn build(self) -> Result<(HashBytes, Account)> {
        const GIVER_STATE: &[u8] = include_bytes!("../../res/giver_state.boc");

        let mut account = BocRepr::decode::<OptionalAccount, _>(GIVER_STATE)?
            .0
            .expect("invalid giver state");

        let address;
        match &mut account.state {
            AccountState::Active(state_init) => {
                let mut data = CellBuilder::new();

                // Append pubkey first
                data.store_u256(HashBytes::wrap(self.pubkey.as_bytes()))?;

                // Append everything except the pubkey
                let prev_data = state_init
                    .data
                    .take()
                    .expect("giver state must contain data");
                let mut prev_data = prev_data.as_slice()?;
                prev_data.advance(256, 0)?;

                data.store_slice(prev_data)?;

                // Update data
                state_init.data = Some(data.build()?);

                // Compute address
                address = *CellBuilder::build_from(&*state_init)?.repr_hash();
            }
            _ => unreachable!("saved state is for the active account"),
        };

        account.balance.tokens = self.balance;
        account.storage_stat.used = compute_storage_used(&account)?;

        Ok((address, account))
    }
}

// TODO: move into types
fn compute_storage_used(account: &Account) -> Result<StorageUsed> {
    let cell = {
        let cx = &mut Cell::empty_context();
        let mut storage = CellBuilder::new();
        storage.store_u64(account.last_trans_lt)?;
        account.balance.store_into(&mut storage, cx)?;
        account.state.store_into(&mut storage, cx)?;
        if account.init_code_hash.is_some() {
            account.init_code_hash.store_into(&mut storage, cx)?;
        }
        storage.build_ext(cx)?
    };

    let res = cell
        .compute_unique_stats(usize::MAX)
        .context("max size exceeded")?;

    let res = StorageUsed {
        cells: VarUint56::new(res.cell_count),
        bits: VarUint56::new(res.bit_count),
        public_cells: Default::default(),
    };

    anyhow::ensure!(res.bits.is_valid(), "bit count overflow");
    anyhow::ensure!(res.cells.is_valid(), "cell count overflow");

    Ok(res)
}
