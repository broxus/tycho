use anyhow::{Context, Result, anyhow};
use num_bigint::BigInt;
use num_traits::ToPrimitive;
use tycho_types::cell::CellBuilder;
use tycho_types::crc::crc_16;
use tycho_types::models::{Account, AccountState, BlockchainConfig, StdAddr};
use tycho_types::prelude::{Cell, HashBytes};
use tycho_vm::{GasParams, OwnedCellSlice, RcStackValue, SafeRc, SmcInfoBase, VmStateBuilder};

pub trait AsGetterMethodId {
    fn as_getter_method_id(&self) -> u32;
}

impl<T: AsGetterMethodId + ?Sized> AsGetterMethodId for &T {
    fn as_getter_method_id(&self) -> u32 {
        T::as_getter_method_id(*self)
    }
}

impl<T: AsGetterMethodId + ?Sized> AsGetterMethodId for &mut T {
    fn as_getter_method_id(&self) -> u32 {
        T::as_getter_method_id(*self)
    }
}

impl AsGetterMethodId for u32 {
    fn as_getter_method_id(&self) -> u32 {
        *self
    }
}

impl AsGetterMethodId for str {
    fn as_getter_method_id(&self) -> u32 {
        let crc = crc_16(self.as_bytes());
        crc as u32 | 0x10000
    }
}

pub trait AccountExt {
    fn bind<'a>(&'a self, config: &'a BlockchainConfig) -> ExecutionContext<'a>;
}

impl AccountExt for Account {
    #[inline]
    fn bind<'a>(&'a self, config: &'a BlockchainConfig) -> ExecutionContext<'a> {
        ExecutionContext {
            account: self,
            config,
        }
    }
}

pub struct ExecutionContext<'a> {
    pub account: &'a Account,
    pub config: &'a BlockchainConfig,
}

impl ExecutionContext<'_> {
    pub fn call_getter(
        &self,
        method_id: impl AsGetterMethodId,
        args: Vec<RcStackValue>,
    ) -> Result<VmGetterOutput> {
        self.call_getter_impl(method_id.as_getter_method_id(), args)
    }

    fn call_getter_impl(
        &self,
        method_id: u32,
        mut args: Vec<RcStackValue>,
    ) -> Result<VmGetterOutput> {
        let state = match &self.account.state {
            AccountState::Active(state_init) => state_init,
            _ => anyhow::bail!("account is not active"),
        };
        let code = state.clone().code.ok_or(anyhow!("account has no code"))?;

        let block_lt = 0;
        let block_unixtime = tycho_util::time::now_sec();

        let smc = SmcInfoBase::new()
            .with_account_addr(self.account.address.clone())
            .with_account_balance(self.account.balance.clone())
            .with_config(self.config.params.clone())
            .with_block_lt(block_lt)
            .with_tx_lt(block_lt)
            .with_now(block_unixtime)
            .require_ton_v4()
            .require_ton_v6()
            .fill_unpacked_config()?
            .require_ton_v11();

        let data = state.clone().data.unwrap_or_default();

        args.push(SafeRc::new_dyn_value(BigInt::from(method_id)));

        let mut vm_state = VmStateBuilder::new()
            .with_code(code)
            .with_data(data)
            .with_stack(args)
            .with_smc_info(smc)
            .with_gas(GasParams::getter())
            .build();

        let exit_code = !vm_state.run();

        Ok(VmGetterOutput {
            exit_code,
            stack: vm_state.stack.items.clone(),
            success: exit_code == 0 || exit_code == 1,
        })
    }
}

pub struct VmGetterOutput {
    pub exit_code: i32,
    pub stack: Vec<RcStackValue>,
    pub success: bool,
}

impl VmGetterOutput {
    pub fn parse<T: FromStackValue>(self) -> Result<T> {
        anyhow::ensure!(
            self.success,
            "cannot parse failed vm getter output, exit_code={}",
            self.exit_code
        );
        LazyTuple(self.stack.into_iter()).read_next()
    }
}

// TODO: Move into sdk

pub trait FromStackValue: Sized {
    fn from_stack_value(value: RcStackValue) -> Result<Self>;
}

impl FromStackValue for RcStackValue {
    #[inline]
    fn from_stack_value(value: RcStackValue) -> Result<Self> {
        Ok(value)
    }
}

impl<T: FromStackValue> FromStackValue for Option<T> {
    fn from_stack_value(value: RcStackValue) -> Result<Self> {
        if value.is_null() {
            Ok(None)
        } else {
            T::from_stack_value(value).map(Some)
        }
    }
}

#[repr(transparent)]
pub struct LazyTuple(pub std::vec::IntoIter<RcStackValue>);

impl LazyTuple {
    pub fn read_next<T: FromStackValue>(&mut self) -> Result<T> {
        let value = self.0.next().context("unexpected tuple eof")?;
        T::from_stack_value(value)
    }

    #[allow(unused)]
    pub fn skip_next(&mut self) -> Result<()> {
        self.0.next().map(|_| ()).context("unexpected tuple eof")
    }
}

impl FromStackValue for LazyTuple {
    fn from_stack_value(value: RcStackValue) -> Result<Self> {
        let tuple = value.into_tuple().map(SafeRc::unwrap_or_clone)?;
        Ok(Self(tuple.into_iter()))
    }
}

macro_rules! impl_for_tuples {
    ($($n:literal => ($($ty:tt),+$(,)?)),+$(,)?) => {
        $(impl<$($ty: FromStackValue),+> FromStackValues for ($($ty,)+) {
            fn from_stack_values(values: Vec<RcStackValue>) -> Result<Self> {
                anyhow::ensure!(
                    values.len() == $n,
                    "expected tuple of {} items, got {}",
                    $n,
                    values.len(),
                );
                let mut tuple = LazyTuple(values.into_iter());
                Ok(($(tuple.read_next::<$ty>()?,)+))
            }
        }

        impl<$($ty: FromStackValue),+> FromStackValue for ($($ty,)+) {
            fn from_stack_value(value: RcStackValue) -> Result<Self> {
                let tuple = value.into_tuple().map(SafeRc::unwrap_or_clone)?;
                Self::from_stack_values(tuple)
            }
        })+
    };
}

impl_for_tuples! {
    1 => (T1,),
    2 => (T1, T2),
    3 => (T1, T2, T3),
    4 => (T1, T2, T3, T4),
    5 => (T1, T2, T3, T4, T5),
    6 => (T1, T2, T3, T4, T5, T6),
    7 => (T1, T2, T3, T4, T5, T6, T7),
    8 => (T1, T2, T3, T4, T5, T6, T7, T8),
    9 => (T1, T2, T3, T4, T5, T6, T7, T8, T9),
    10 => (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10),
    11 => (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11),
    12 => (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12),
}

#[repr(transparent)]
pub struct LispList<T>(pub Vec<T>);

impl<T> LispList<T> {
    pub fn into_inner(self) -> Vec<T> {
        self.0
    }
}

impl<T: FromStackValue> FromStackValue for LispList<T> {
    fn from_stack_value(mut value: RcStackValue) -> Result<Self> {
        let mut result = Vec::new();
        while !value.is_null() {
            let (head, tail) = <(T, RcStackValue)>::from_stack_value(value)?;
            result.push(head);
            value = tail;
        }
        Ok(LispList(result))
    }
}

impl FromStackValue for num_bigint::BigInt {
    fn from_stack_value(value: RcStackValue) -> Result<Self> {
        value
            .into_int()
            .map_err(Into::into)
            .map(SafeRc::unwrap_or_clone)
    }
}

impl FromStackValue for num_bigint::BigUint {
    fn from_stack_value(value: RcStackValue) -> Result<Self> {
        let (sign, int) = num_bigint::BigInt::from_stack_value(value)?.into_parts();
        anyhow::ensure!(sign != num_bigint::Sign::Minus, "expected unsigned integer");
        Ok(int)
    }
}

impl FromStackValue for bool {
    fn from_stack_value(value: RcStackValue) -> Result<Self> {
        let int = num_bigint::BigInt::from_stack_value(value)?;
        Ok(int.sign() != num_bigint::Sign::NoSign)
    }
}

macro_rules! impl_for_ints {
    ($($ty:ty => $conv:ident),+$(,)?) => {
        $(impl FromStackValue for $ty {
            fn from_stack_value(value: RcStackValue) -> Result<Self> {
                let int = num_bigint::BigInt::from_stack_value(value)?;
                let Some(result) = int.$conv() else {
                    anyhow::bail!("stack integer is out of range: {int}");
                };
                Ok(result)
            }
        })+
    };
}

impl_for_ints! {
    i8 => to_i8,
    u8 => to_u8,
    i16 => to_i16,
    u16 => to_u16,
    i32 => to_i32,
    u32 => to_u32,
    i64 => to_i64,
    u64 => to_u64,
    i128 => to_i128,
    u128 => to_u128,
}

impl FromStackValue for HashBytes {
    fn from_stack_value(value: RcStackValue) -> Result<Self> {
        let uint = num_bigint::BigUint::from_stack_value(value)?;
        let mut bytes = uint.to_bytes_le();
        anyhow::ensure!(bytes.len() <= 32, "stack integer is out of range: {uint}");
        bytes.resize(32, 0);
        bytes.reverse();
        Ok(HashBytes::from_slice(&bytes))
    }
}

impl FromStackValue for Cell {
    fn from_stack_value(value: RcStackValue) -> Result<Self> {
        value
            .into_cell()
            .map_err(Into::into)
            .map(SafeRc::unwrap_or_clone)
    }
}

impl FromStackValue for StdAddr {
    fn from_stack_value(value: RcStackValue) -> Result<Self> {
        Cell::from_stack_value(value)?
            .parse::<StdAddr>()
            .context("failed to parse StdAddr from stack value")
    }
}

impl FromStackValue for CellBuilder {
    fn from_stack_value(value: RcStackValue) -> Result<Self> {
        value
            .into_cell_builder()
            .map_err(Into::into)
            .map(SafeRc::unwrap_or_clone)
    }
}

impl FromStackValue for OwnedCellSlice {
    fn from_stack_value(value: RcStackValue) -> Result<Self> {
        value
            .into_cell_slice()
            .map_err(Into::into)
            .map(SafeRc::unwrap_or_clone)
    }
}

pub trait FromStackValues: Sized {
    fn from_stack_values(values: Vec<RcStackValue>) -> Result<Self>;
}

impl<T: FromStackValue> FromStackValues for Vec<T> {
    #[inline]
    fn from_stack_values(values: Vec<RcStackValue>) -> Result<Self> {
        values.into_iter().map(T::from_stack_value).collect()
    }
}

impl<T: FromStackValue, const N: usize> FromStackValues for [T; N] {
    #[inline]
    fn from_stack_values(values: Vec<RcStackValue>) -> Result<Self> {
        anyhow::ensure!(
            values.len() == N,
            "expected tuple of len {N}, got {}",
            values.len()
        );

        let values = values
            .into_iter()
            .map(T::from_stack_value)
            .collect::<Result<Vec<_>>>()?;

        match values.try_into() {
            Ok(items) => Ok(items),
            Err(_) => unreachable!(),
        }
    }
}
