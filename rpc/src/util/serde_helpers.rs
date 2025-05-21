use base64::prelude::{Engine as _, BASE64_STANDARD};
use everscale_types::models::StdAddr;
use everscale_types::prelude::*;
use serde::{Deserialize, Serialize};
pub use tycho_util::serde_helpers::*;

pub fn should_normalize_base64(text: &str) -> bool {
    for byte in text.as_bytes() {
        match *byte {
            b'-' | b'_' => return true,
            _ => {}
        }
    }
    false
}

pub fn normalize_base64(text: &mut str) -> bool {
    // SAFETY: Content of the slice will remain a valid utf8 slice.
    let bytes = unsafe { text.as_bytes_mut() };
    for byte in bytes {
        match *byte {
            b'-' => *byte = b'+',
            b'_' => *byte = b'/',
            byte if !byte.is_ascii() => return false,
            _ => {}
        }
    }
    true
}

pub mod boc_or_empty {
    use super::*;

    pub fn serialize<S>(value: &Option<Cell>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match value {
            Some(value) => Boc::serialize(value, serializer),
            None => serializer.serialize_str(""),
        }
    }
}

pub mod method_id {
    use std::borrow::Cow;

    use super::*;

    const MAX_METHOD_NAME_LEN: usize = 128;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<i64, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum MethodId<'a> {
            Int(i64),
            String(#[serde(borrow)] Cow<'a, str>),
        }

        Ok(match <_>::deserialize(deserializer)? {
            MethodId::Int(int) => int,
            MethodId::String(str) => {
                let bytes = str.as_bytes();
                if bytes.len() > MAX_METHOD_NAME_LEN {
                    return Err(Error::custom("method name is too long"));
                }
                everscale_types::crc::crc_16(bytes) as i64 | 0x10000
            }
        })
    }
}

pub mod string_or_u64 {
    use super::*;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Value {
        Int(u64),
        String(#[serde(with = "string")] u64),
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(match <_>::deserialize(deserializer)? {
            Value::Int(x) | Value::String(x) => x,
        })
    }

    pub fn deserialize_option<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let Some(value) = <_>::deserialize(deserializer)? else {
            return Ok(None);
        };

        Ok(Some(match value {
            Value::Int(x) | Value::String(x) => x,
        }))
    }
}

pub mod option_tonlib_address {
    use super::*;

    pub fn serialize<S>(value: &Option<StdAddr>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        #[serde(transparent)]
        #[repr(transparent)]
        struct Wrapper<'a>(#[serde(with = "tonlib_address")] &'a StdAddr);

        value.as_ref().map(Wrapper).serialize(serializer)
    }
}

pub mod tonlib_address {
    use everscale_types::models::{StdAddr, StdAddrBase64Repr};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<StdAddr, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        StdAddrBase64Repr::<true>::deserialize(deserializer)
    }

    pub fn serialize<S>(value: &StdAddr, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        StdAddrBase64Repr::<true>::serialize(value, serializer)
    }
}

pub mod option_tonlib_hash {
    use super::*;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<HashBytes>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(transparent)]
        #[repr(transparent)]
        struct Wrapper(#[serde(with = "tonlib_hash")] HashBytes);

        Option::<Wrapper>::deserialize(deserializer).map(|x| x.map(|Wrapper(x)| x))
    }

    pub fn serialize<S>(value: &Option<HashBytes>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        #[serde(transparent)]
        #[repr(transparent)]
        struct Wrapper<'a>(#[serde(with = "tonlib_hash")] &'a HashBytes);

        match value {
            None => serializer.serialize_none(),
            Some(hash) => serializer.serialize_some(&Wrapper(hash)),
        }
    }
}

pub mod tonlib_hash {
    use std::str::FromStr;

    use serde::de::Error;

    use super::*;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<HashBytes, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let BorrowedStr(mut s) = <_>::deserialize(deserializer)?;
        if s.len() == 44 && should_normalize_base64(&s) && !normalize_base64(s.to_mut()) {
            return Err(Error::custom("invalid character"));
        }

        HashBytes::from_str(&s).map_err(Error::custom)
    }

    pub fn serialize<S>(value: &HashBytes, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut res = [0u8; 44];
        BASE64_STANDARD
            .encode_slice(value.as_array(), &mut res)
            .unwrap();
        // SAFETY: `res` is guaranteed to contain a valid ASCII base64.
        let res = unsafe { std::str::from_utf8_unchecked(&res) };

        serializer.serialize_str(res)
    }

    pub fn serialize_or_empty<S>(
        value: &Option<HashBytes>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match value {
            Some(value) => serialize(value, serializer),
            None => serializer.serialize_str(""),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_or_u64() {
        #[derive(Debug, Eq, PartialEq, Deserialize)]
        struct Struct {
            #[serde(with = "string_or_u64")]
            value: u64,
        }

        for (serialied, expected) in [
            (r#"{"value":0}"#, Struct { value: 0 }),
            (r#"{"value":123}"#, Struct { value: 123 }),
            (r#"{"value":"123"}"#, Struct { value: 123 }),
            (r#"{"value":18446744073709551615}"#, Struct {
                value: u64::MAX,
            }),
            (r#"{"value":"18446744073709551615"}"#, Struct {
                value: u64::MAX,
            }),
        ] {
            let parsed: Struct = serde_json::from_str(serialied).unwrap();
            assert_eq!(parsed, expected);
        }
    }

    #[test]
    fn string_or_u64_option() {
        #[derive(Debug, Eq, PartialEq, Deserialize)]
        struct Struct {
            #[serde(deserialize_with = "string_or_u64::deserialize_option")]
            value: Option<u64>,
        }

        for (serialied, expected) in [
            (r#"{"value":null}"#, Struct { value: None }),
            (r#"{"value":0}"#, Struct { value: Some(0) }),
            (r#"{"value":"0"}"#, Struct { value: Some(0) }),
            (r#"{"value":123}"#, Struct { value: Some(123) }),
            (r#"{"value":"123","value_opt":null}"#, Struct {
                value: Some(123),
            }),
            (r#"{"value":18446744073709551615}"#, Struct {
                value: Some(u64::MAX),
            }),
            (r#"{"value":"18446744073709551615"}"#, Struct {
                value: Some(u64::MAX),
            }),
        ] {
            let parsed: Struct = serde_json::from_str(serialied).unwrap();
            assert_eq!(parsed, expected);
        }
    }
}
