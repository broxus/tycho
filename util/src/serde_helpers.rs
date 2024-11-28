use std::borrow::Cow;
use std::marker::PhantomData;
use std::path::Path;
use std::str::FromStr;

use anyhow::Result;
use serde::de::{Error, Expected, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub fn load_json_from_file<T, P>(path: P) -> Result<T>
where
    for<'de> T: Deserialize<'de>,
    P: AsRef<Path>,
{
    let data = std::fs::read_to_string(path)?;
    let de = &mut serde_json::Deserializer::from_str(&data);
    serde_path_to_error::deserialize(de).map_err(Into::into)
}

pub mod socket_addr {
    use std::net::SocketAddr;

    use super::*;

    pub fn serialize<S: Serializer>(value: &SocketAddr, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.collect_str(value)
        } else {
            value.serialize(serializer)
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<SocketAddr, D::Error> {
        if deserializer.is_human_readable() {
            deserializer.deserialize_str(StrVisitor::new())
        } else {
            SocketAddr::deserialize(deserializer)
        }
    }
}

pub mod humantime {
    use std::time::{Duration, SystemTime};

    use super::*;

    pub fn serialize<T, S: Serializer>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        for<'a> Serde<&'a T>: Serialize,
    {
        Serde::from(value).serialize(serializer)
    }

    pub fn deserialize<'a, T, D: Deserializer<'a>>(deserializer: D) -> Result<T, D::Error>
    where
        Serde<T>: Deserialize<'a>,
    {
        Serde::deserialize(deserializer).map(Serde::into_inner)
    }

    pub struct Serde<T>(T);

    impl<T> Serde<T> {
        #[inline]
        pub fn into_inner(self) -> T {
            self.0
        }
    }

    impl<T> From<T> for Serde<T> {
        fn from(value: T) -> Serde<T> {
            Serde(value)
        }
    }

    impl<'de> Deserialize<'de> for Serde<Duration> {
        fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Serde<Duration>, D::Error> {
            struct V;

            impl Visitor<'_> for V {
                type Value = Duration;

                fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.write_str("a duration")
                }

                fn visit_str<E: Error>(self, v: &str) -> Result<Duration, E> {
                    ::humantime::parse_duration(v)
                        .map_err(|_e| E::invalid_value(serde::de::Unexpected::Str(v), &self))
                }
            }

            d.deserialize_str(V).map(Serde)
        }
    }

    impl<'de> Deserialize<'de> for Serde<SystemTime> {
        fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Serde<SystemTime>, D::Error> {
            struct V;

            impl Visitor<'_> for V {
                type Value = SystemTime;

                fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.write_str("a timestamp")
                }

                fn visit_str<E: Error>(self, v: &str) -> Result<SystemTime, E> {
                    ::humantime::parse_rfc3339_weak(v)
                        .map_err(|_e| E::invalid_value(serde::de::Unexpected::Str(v), &self))
                }
            }

            d.deserialize_str(V).map(Serde)
        }
    }

    impl<'de> Deserialize<'de> for Serde<Option<Duration>> {
        fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Serde<Option<Duration>>, D::Error> {
            match Option::<Serde<Duration>>::deserialize(d)? {
                Some(Serde(v)) => Ok(Serde(Some(v))),
                None => Ok(Serde(None)),
            }
        }
    }

    impl<'de> Deserialize<'de> for Serde<Option<SystemTime>> {
        fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Serde<Option<SystemTime>>, D::Error> {
            match Option::<Serde<SystemTime>>::deserialize(d)? {
                Some(Serde(v)) => Ok(Serde(Some(v))),
                None => Ok(Serde(None)),
            }
        }
    }

    impl Serialize for Serde<&Duration> {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.collect_str(&::humantime::format_duration(*self.0))
        }
    }

    impl Serialize for Serde<Duration> {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.collect_str(&::humantime::format_duration(self.0))
        }
    }

    impl Serialize for Serde<&SystemTime> {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.collect_str(&::humantime::format_rfc3339(*self.0))
        }
    }

    impl Serialize for Serde<SystemTime> {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            ::humantime::format_rfc3339(self.0)
                .to_string()
                .serialize(serializer)
        }
    }

    impl Serialize for Serde<&Option<Duration>> {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            match *self.0 {
                Some(v) => serializer.serialize_some(&Serde(v)),
                None => serializer.serialize_none(),
            }
        }
    }

    impl Serialize for Serde<Option<Duration>> {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            Serde(&self.0).serialize(serializer)
        }
    }

    impl Serialize for Serde<&Option<SystemTime>> {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            match *self.0 {
                Some(v) => serializer.serialize_some(&Serde(v)),
                None => serializer.serialize_none(),
            }
        }
    }

    impl Serialize for Serde<Option<SystemTime>> {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            Serde(&self.0).serialize(serializer)
        }
    }
}

pub mod string {
    use super::*;

    pub fn serialize<S>(value: &dyn std::fmt::Display, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(value)
    }

    pub fn deserialize<'de, D, T>(deserializer: D) -> Result<T, D::Error>
    where
        D: serde::Deserializer<'de>,
        T: FromStr,
        T::Err: std::fmt::Display,
    {
        BorrowedStr::deserialize(deserializer)
            .and_then(|data| T::from_str(&data.0).map_err(D::Error::custom))
    }
}

pub mod option_string {
    use super::*;

    pub fn serialize<S, T>(value: &Option<T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
        T: std::fmt::Display,
    {
        #[derive(Serialize)]
        #[serde(transparent)]
        #[repr(transparent)]
        struct Helper<'a, T: std::fmt::Display>(#[serde(with = "string")] &'a T);

        value.as_ref().map(Helper).serialize(serializer)
    }

    pub fn deserialize<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
    where
        D: serde::Deserializer<'de>,
        T: FromStr,
        T::Err: std::fmt::Display,
    {
        #[derive(Deserialize)]
        #[serde(transparent)]
        #[repr(transparent)]
        struct Helper<T>(#[serde(with = "string")] T)
        where
            T: FromStr,
            T::Err: std::fmt::Display;

        Helper::deserialize(deserializer).map(|Helper(v)| Some(v))
    }
}

pub mod signature {
    use base64::engine::Engine as _;
    use base64::prelude::BASE64_STANDARD;

    use super::*;

    pub fn serialize<S>(data: &[u8; 64], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&BASE64_STANDARD.encode(data))
        } else {
            data.serialize(serializer)
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Box<[u8; 64]>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        if deserializer.is_human_readable() {
            <BorrowedStr<'_> as Deserialize>::deserialize(deserializer).and_then(
                |BorrowedStr(s)| {
                    let mut buffer = [0u8; 66];
                    match BASE64_STANDARD.decode_slice(s.as_ref(), &mut buffer) {
                        Ok(64) => {
                            let [data @ .., _, _] = buffer;
                            Ok(Box::new(data))
                        }
                        _ => Err(Error::custom("Invalid signature")),
                    }
                },
            )
        } else {
            deserializer
                .deserialize_bytes(BytesVisitor::<64>)
                .map(Box::new)
        }
    }
}

#[derive(Deserialize)]
#[repr(transparent)]
pub struct BorrowedStr<'a>(#[serde(borrow)] pub Cow<'a, str>);

pub struct StrVisitor<S>(PhantomData<S>);

impl<S> StrVisitor<S> {
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<S> Default for StrVisitor<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: FromStr> Visitor<'_> for StrVisitor<S>
where
    <S as FromStr>::Err: std::fmt::Display,
{
    type Value = S;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "a string")
    }

    fn visit_str<E: Error>(self, value: &str) -> Result<Self::Value, E> {
        value.parse::<Self::Value>().map_err(Error::custom)
    }
}

pub struct BytesVisitor<const M: usize>;

impl<'de, const M: usize> Visitor<'de> for BytesVisitor<M> {
    type Value = [u8; M];

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("a byte array of size {M}"))
    }

    fn visit_bytes<E: Error>(self, v: &[u8]) -> Result<Self::Value, E> {
        v.try_into()
            .map_err(|_e| Error::invalid_length(v.len(), &self))
    }

    fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        struct SeqIter<'de, A, T> {
            access: A,
            marker: PhantomData<(&'de (), T)>,
        }

        impl<'de, A, T> SeqIter<'de, A, T> {
            pub(crate) fn new(access: A) -> Self
            where
                A: serde::de::SeqAccess<'de>,
            {
                Self {
                    access,
                    marker: PhantomData,
                }
            }
        }

        impl<'de, A, T> Iterator for SeqIter<'de, A, T>
        where
            A: serde::de::SeqAccess<'de>,
            T: Deserialize<'de>,
        {
            type Item = Result<T, A::Error>;

            fn next(&mut self) -> Option<Self::Item> {
                self.access.next_element().transpose()
            }

            fn size_hint(&self) -> (usize, Option<usize>) {
                match self.access.size_hint() {
                    Some(size) => (size, Some(size)),
                    None => (0, None),
                }
            }
        }

        fn array_from_iterator<I, T, E, const N: usize>(
            mut iter: I,
            expected: &dyn Expected,
        ) -> Result<[T; N], E>
        where
            I: Iterator<Item = Result<T, E>>,
            E: Error,
        {
            use core::mem::MaybeUninit;

            /// # Safety
            /// The following must be true:
            /// - The first `num` elements must be initialized.
            unsafe fn drop_array_elems<T, const N: usize>(
                num: usize,
                mut arr: [MaybeUninit<T>; N],
            ) {
                arr[..num]
                    .iter_mut()
                    .for_each(|item| item.assume_init_drop());
            }

            // SAFETY: It is safe to assume that array of uninitialized values is initialized itself.
            let mut arr: [MaybeUninit<T>; N] = unsafe { MaybeUninit::uninit().assume_init() };

            // NOTE: Leaks memory on panic
            for (i, elem) in arr[..].iter_mut().enumerate() {
                *elem = match iter.next() {
                    Some(Ok(value)) => MaybeUninit::new(value),
                    Some(Err(err)) => {
                        // SAFETY: Items until `i` were initialized.
                        unsafe { drop_array_elems(i, arr) };
                        return Err(err);
                    }
                    None => {
                        // SAFETY: Items until `i` were initialized.
                        unsafe { drop_array_elems(i, arr) };
                        return Err(Error::invalid_length(i, expected));
                    }
                };
            }

            // Everything is initialized. Transmute the array to the initialized type.
            // A normal transmute is not possible because of:
            // https://github.com/rust-lang/rust/issues/61956
            Ok(unsafe { std::mem::transmute_copy(&arr) })
        }

        array_from_iterator(SeqIter::new(seq), &self)
    }
}
