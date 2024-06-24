use std::fmt;
use std::fmt::Debug;

use anyhow::anyhow;
use diesel::deserialize::FromSql;
use diesel::mysql::Mysql;
use diesel::serialize::{Output, ToSql};
use diesel::sql_types::Binary;
use diesel::{serialize, AsExpression, Queryable};
use serde::de::{Error, Unexpected, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Serialize, Serializer};
use uuid::Uuid;

#[derive(AsExpression, Hash, Eq, PartialEq, Clone, Copy)]
#[diesel(sql_type = diesel::sql_types::Binary)]
pub struct HashInsert<const N: usize> {
    pub hash: [u8; N],
}

impl<const N: usize> Debug for HashInsert<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashInsert")
            .field("hash", &hex::encode(self.hash))
            .finish()
    }
}

impl<const N: usize> Serialize for HashInsert<N> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&hex::encode(self.hash))
        } else {
            let mut seq = serializer.serialize_seq(Some(N))?;
            for byte in self.hash.iter() {
                seq.serialize_element(byte)?;
            }
            seq.end()
        }
    }
}

impl<'de, const N: usize> Deserialize<'de> for HashInsert<N> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct HexVisitor<const N: usize>;

        impl<'de, const N: usize> Visitor<'de> for HexVisitor<N> {
            type Value = HashInsert<N>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("hex-encoded byte array")
            }

            fn visit_str<E: Error>(self, value: &str) -> Result<Self::Value, E> {
                let bytes = hex::decode(value)
                    .map_err(|_| E::invalid_type(Unexpected::Str(value), &self))?;
                let len = bytes.len();
                HashInsert::try_from(bytes).map_err(|_| E::invalid_length(len, &self))
            }

            fn visit_bytes<E: Error>(self, value: &[u8]) -> Result<Self::Value, E> {
                HashInsert::try_from(value.to_vec())
                    .map_err(|_| E::invalid_length(value.len(), &self))
            }
        }

        struct BytesVisitor<const N: usize>;

        impl<'de, const N: usize> Visitor<'de> for BytesVisitor<N> {
            type Value = HashInsert<N>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("byte array")
            }

            fn visit_bytes<E: Error>(self, value: &[u8]) -> Result<Self::Value, E> {
                HashInsert::try_from(value.to_vec())
                    .map_err(|_| E::invalid_length(value.len(), &self))
            }
        }

        if deserializer.is_human_readable() {
            deserializer.deserialize_str(HexVisitor::<N>)
        } else {
            deserializer.deserialize_bytes(BytesVisitor::<N>)
        }
    }
}

impl<const N: usize> FromSql<Binary, Mysql> for HashInsert<N> {
    fn from_sql(bytes: diesel::mysql::MysqlValue<'_>) -> diesel::deserialize::Result<Self> {
        let ty = bytes.value_type();
        let bytes = bytes.as_bytes();
        if bytes.len() != N {
            return Err(format!(
                "Invalid hash length FromSql: expected {}, got {}. Type: {:?}",
                N,
                bytes.len(),
                &ty
            )
            .into());
        }
        let mut hash = [0u8; N];
        hash.copy_from_slice(bytes);
        Ok(Self { hash })
    }
}

impl<const N: usize> ToSql<Binary, Mysql> for HashInsert<N> {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Mysql>) -> serialize::Result {
        let data: &[u8] = self.hash.as_slice();
        <[u8] as ToSql<Binary, Mysql>>::to_sql(data, out)
    }
}

impl<const N: usize> Queryable<Binary, Mysql> for HashInsert<N> {
    type Row = Vec<u8>;

    fn build(row: Self::Row) -> diesel::deserialize::Result<Self> {
        if row.len() != N {
            return Err(format!(
                "Invalid hash length queryable Vec: expected {}, got {}",
                N,
                row.len()
            )
            .into());
        }

        let mut hash = [0u8; N];
        hash.copy_from_slice(&row);
        Ok(Self { hash })
    }
}

impl<const N: usize> From<[u8; N]> for HashInsert<N> {
    fn from(hash: [u8; N]) -> Self {
        Self { hash }
    }
}

impl<'a, const N: usize> From<&'a [u8; N]> for HashInsert<N> {
    fn from(hash: &'a [u8; N]) -> Self {
        Self { hash: *hash }
    }
}

impl<const N: usize> From<HashInsert<N>> for [u8; N] {
    fn from(value: HashInsert<N>) -> Self {
        value.hash
    }
}

impl From<Uuid> for HashInsert<16> {
    fn from(value: Uuid) -> Self {
        Self {
            hash: *value.as_bytes(),
        }
    }
}

impl From<HashInsert<16>> for Uuid {
    fn from(value: HashInsert<16>) -> Self {
        Uuid::from_bytes(value.hash)
    }
}

impl<const N: usize> AsRef<[u8]> for HashInsert<N> {
    fn as_ref(&self) -> &[u8] {
        &self.hash
    }
}

impl<const N: usize> TryFrom<&[u8]> for HashInsert<N> {
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != N {
            return Err(anyhow!(
                "Invalid hash length from slice: expected {}, got {}",
                N,
                value.len()
            ));
        }

        let mut hash = [0u8; N];
        hash.copy_from_slice(value);
        Ok(Self { hash })
    }
}

impl<const N: usize> TryFrom<Vec<u8>> for HashInsert<N> {
    type Error = anyhow::Error;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        if value.len() != N {
            return Err(anyhow!(
                "Invalid hash length from vec<u8>: expected {}, got {}",
                N,
                value.len()
            ));
        }

        let mut hash = [0u8; N];
        hash.copy_from_slice(&value);
        Ok(Self { hash })
    }
}

#[cfg(feature = "csv")]
pub mod serde_csv_bytes {
    pub fn serialize<S, T>(data: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
        T: AsRef<[u8]>,
    {
        // see https://gist.github.com/siddontang/8875771

        let mut result = Vec::with_capacity(data.as_ref().len() * 2);
        for byte in data.as_ref() {
            let escaped = match byte {
                0 => Some(b"\\0"),
                8 => Some(b"\\b"),
                b'\n' => Some(b"\\n"),
                b'\r' => Some(b"\\r"),
                b'\t' => Some(b"\\t"),
                b'\\' => Some(b"\\\\"),
                26 => Some(b"\\Z"),
                _ => None,
            };

            match escaped {
                Some(escaped) => result.extend_from_slice(escaped),
                None => result.push(*byte),
            };
        }
        serializer.serialize_bytes(&result)
    }
}

#[cfg(feature = "csv")]
pub mod serde_csv_bytes_optional {
    use super::*;

    pub fn serialize<S, T>(data: &Option<T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
        T: AsRef<[u8]>,
    {
        match data {
            Some(data) => serde_csv_bytes::serialize(data, serializer),
            None => serializer.serialize_bytes(b"\\N"),
        }
    }
}

#[cfg(feature = "csv")]
pub mod serde_csv_optional {
    use serde::Serialize;

    pub fn serialize<S, T>(data: &Option<T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
        T: Serialize,
    {
        match data {
            Some(data) => serializer.serialize_some(data),
            None => serializer.serialize_bytes(b"\\N"),
        }
    }
}

#[cfg(feature = "csv")]
pub mod serde_csv_bool {
    pub fn serialize<S>(data: &bool, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u8(*data as u8)
    }
}

pub fn is_default<T: Default + PartialEq>(value: &T) -> bool {
    value == &T::default()
}

pub mod serde_string {
    use std::fmt;
    use std::str::FromStr;

    use super::*;

    pub fn serialize<S>(data: &dyn fmt::Display, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        data.to_string().serialize(serializer)
    }

    pub fn deserialize<'de, D, T>(deserializer: D) -> Result<T, D::Error>
    where
        D: serde::Deserializer<'de>,
        T: FromStr,
        T::Err: fmt::Display,
    {
        String::deserialize(deserializer)
            .and_then(|data| T::from_str(&data).map_err(D::Error::custom))
    }
}
