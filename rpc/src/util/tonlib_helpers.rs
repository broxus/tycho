use anyhow::Result;
use everscale_types::models::StdAddr;
use everscale_types::prelude::*;
use num_bigint::BigInt;
use tycho_vm::{RcStackValue, SafeRc, Stack};

pub fn compute_method_id(bytes: impl AsRef<[u8]>) -> i64 {
    everscale_types::crc::crc_16(bytes.as_ref()) as i64 | 0x10000
}

pub struct StackParser {
    items: Vec<RcStackValue>,
    origin: StackParseOrigin,
}

impl StackParser {
    pub fn begin(items: impl IntoStackItems, origin: StackParseOrigin) -> Self {
        let mut items = items.into_stack_items();
        if origin == StackParseOrigin::Bottom {
            items.reverse();
        }
        Self { items, origin }
    }

    pub fn begin_from_bottom(items: impl IntoStackItems) -> Self {
        Self::begin(items, StackParseOrigin::Bottom)
    }

    #[allow(unused)]
    pub fn begin_from_top(items: impl IntoStackItems) -> Self {
        Self::begin(items, StackParseOrigin::Top)
    }

    #[allow(unused)]
    pub fn set_origin(&mut self, origin: StackParseOrigin) {
        if self.origin != origin {
            self.items.reverse();
            self.origin = origin;
        }
    }

    pub fn pop_int(&mut self) -> Result<BigInt> {
        self.pop_item()?
            .into_int()
            .map(SafeRc::unwrap_or_clone)
            .map_err(Into::into)
    }

    pub fn pop_bool(&mut self) -> Result<bool> {
        let int = self.pop_int()?;
        Ok(int.sign() != num_bigint::Sign::NoSign)
    }

    pub fn pop_cell(&mut self) -> Result<Cell> {
        self.pop_item()?
            .into_cell()
            .map(SafeRc::unwrap_or_clone)
            .map_err(Into::into)
    }

    pub fn pop_address(&mut self) -> Result<StdAddr> {
        self.pop_cell()?.parse::<StdAddr>().map_err(Into::into)
    }

    pub fn pop_address_or_none(&mut self) -> Result<Option<StdAddr>> {
        let cell = self.pop_cell()?;
        let mut cs = cell.as_slice()?;
        if cs.get_small_uint(0, 2)? == 0b00 {
            Ok(None)
        } else {
            StdAddr::load_from(&mut cs).map(Some).map_err(Into::into)
        }
    }

    pub fn pop_item(&mut self) -> Result<RcStackValue> {
        match self.items.pop() {
            Some(item) => Ok(item),
            None => anyhow::bail!("not enough items on stack"),
        }
    }
}

pub trait IntoStackItems {
    fn into_stack_items(self) -> Vec<RcStackValue>;
}

impl IntoStackItems for SafeRc<Stack> {
    fn into_stack_items(self) -> Vec<RcStackValue> {
        SafeRc::unwrap_or_clone(self).items
    }
}

impl IntoStackItems for Stack {
    #[inline]
    fn into_stack_items(self) -> Vec<RcStackValue> {
        self.items
    }
}

impl IntoStackItems for Vec<RcStackValue> {
    #[inline]
    fn into_stack_items(self) -> Vec<RcStackValue> {
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StackParseOrigin {
    Bottom,
    Top,
}

pub fn load_bytes_rope(
    mut cs: CellSlice<'_>,
    strict: bool,
) -> Result<Vec<u8>, everscale_types::error::Error> {
    let mut result = Vec::new();
    let mut buffer = [0u8; 128];
    loop {
        // TODO: Should we trim unaligned bytes?
        let bytes = cs.load_raw(&mut buffer, cs.size_bits())?;
        result.extend_from_slice(bytes);

        match cs.size_refs() {
            0 => break,
            2.. if strict => return Err(everscale_types::error::Error::InvalidData),
            _ => cs = cs.load_reference_as_slice()?,
        }
    }
    Ok(result)
}
