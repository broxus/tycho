use bytes::Bytes;
use tl_proto::{TlError, TlPacket, TlRead, TlResult, TlWrite};

macro_rules! define_non_zero {
    ($($mod:ident => ($ty:ident, $write_method:ident, $read_ty:ty)),*$(,)?) => {
        $(pub mod $mod {
            use std::num::$ty;

            use super::*;

            #[inline]
            pub const fn size_hint(_: &$ty) -> usize {
                std::mem::size_of::<$ty>()
            }

            #[inline]
            pub fn write<P: TlPacket>(value: &$ty, packet: &mut P) {
                packet.$write_method(value.get());
            }

            #[inline]
            pub fn read(packet: &'_ [u8], offset: &mut usize) -> TlResult<$ty> {
                match <$ty>::new(<$read_ty>::read_from(packet, offset)?) {
                    Some(value) => Ok(value),
                    None => Err(TlError::InvalidData),
                }
            }
        })*
    };
}

define_non_zero! {
    non_zero_u32 => (NonZeroU32, write_u32, u32),
    non_zero_i32 => (NonZeroI32, write_i32, i32),
    non_zero_u64 => (NonZeroU64, write_u64, u64),
    non_zero_i64 => (NonZeroI64, write_i64, i64),
}

pub mod signature_ref {
    use super::*;

    #[inline]
    pub fn size_hint(signature: &[u8; 64]) -> usize {
        signature.as_slice().max_size_hint()
    }

    #[inline]
    pub fn write<P: TlPacket>(signature: &[u8; 64], packet: &mut P) {
        signature.as_slice().write_to(packet);
    }

    pub fn read<'a>(packet: &'a [u8], offset: &mut usize) -> TlResult<&'a [u8; 64]> {
        <&tl_proto::BoundedBytes<64>>::read_from(packet, offset)
            .and_then(|bytes| bytes.as_ref().try_into().map_err(|_e| TlError::InvalidData))
    }
}

pub mod signature_owned {
    use super::*;

    #[inline]
    pub fn size_hint(signature: &[u8; 64]) -> usize {
        signature.as_slice().max_size_hint()
    }

    #[inline]
    pub fn write<P: TlPacket>(signature: &[u8; 64], packet: &mut P) {
        signature.as_slice().write_to(packet);
    }

    pub fn read(packet: &[u8], offset: &mut usize) -> TlResult<Box<[u8; 64]>> {
        <&tl_proto::BoundedBytes<64>>::read_from(packet, offset).and_then(|bytes| {
            let Ok::<[u8; 64], _>(bytes) = bytes.as_ref().try_into() else {
                return Err(TlError::InvalidData);
            };
            Ok(Box::new(bytes))
        })
    }
}

pub mod signature_arc {
    use std::sync::Arc;

    use super::*;

    #[inline]
    pub fn size_hint(signature: &[u8; 64]) -> usize {
        signature.as_slice().max_size_hint()
    }

    #[inline]
    pub fn write<P: TlPacket>(signature: &[u8; 64], packet: &mut P) {
        signature.as_slice().write_to(packet);
    }

    pub fn read(packet: &[u8], offset: &mut usize) -> TlResult<Arc<[u8; 64]>> {
        <&tl_proto::BoundedBytes<64>>::read_from(packet, offset).and_then(|bytes| {
            let Ok::<[u8; 64], _>(bytes) = bytes.as_ref().try_into() else {
                return Err(TlError::InvalidData);
            };
            Ok(Arc::new(bytes))
        })
    }
}

pub struct VecWithMaxLen<const N: usize>;

impl<const N: usize> VecWithMaxLen<N> {
    #[inline]
    pub fn size_hint<T: tl_proto::TlWrite>(value: &[T]) -> usize {
        value.max_size_hint()
    }

    #[inline]
    pub fn write<P: TlPacket, T: tl_proto::TlWrite>(value: &[T], packet: &mut P) {
        value.write_to(packet);
    }

    pub fn read<'tl, T>(packet: &'tl [u8], offset: &mut usize) -> TlResult<Vec<T>>
    where
        T: tl_proto::TlRead<'tl>,
    {
        let len = u32::read_from(packet, offset)? as usize;
        if len > N {
            return Err(TlError::InvalidData);
        }

        let mut items = Vec::with_capacity(len);
        for _ in 0..len {
            items.push(T::read_from(packet, offset)?);
        }

        Ok(items)
    }
}

pub struct BigBytes<const MAX_SIZE: usize>;

impl<const MAX_SIZE: usize> BigBytes<MAX_SIZE> {
    pub const MAX_SIZE: usize = MAX_SIZE;

    pub const fn size_hint(bytes: &Bytes) -> usize {
        let len = bytes.len();
        4 + len + Self::compute_padding(len)
    }

    pub fn write<P: TlPacket>(bytes: &Bytes, packet: &mut P) {
        const PADDING: [u8; 3] = [0; 3];

        let len = bytes.len();
        packet.write_u32(len as u32);
        packet.write_raw_slice(bytes);
        if len % 4 != 0 {
            packet.write_raw_slice(&PADDING[0..4 - len % 4]);
        }
    }

    pub fn read(packet: &[u8], offset: &mut usize) -> TlResult<Bytes> {
        let len = u32::read_from(packet, offset)? as usize;
        if len > Self::MAX_SIZE {
            return Err(tl_proto::TlError::InvalidData);
        }
        let padding = Self::compute_padding(len);

        if offset.saturating_add(len + padding) > packet.len() {
            return Err(tl_proto::TlError::UnexpectedEof);
        }

        let bytes = Bytes::copy_from_slice(&packet[*offset..*offset + len]);
        *offset += len + padding;

        Ok(bytes)
    }

    const fn compute_padding(len: usize) -> usize {
        (4 - len % 4) % 4
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn big_bytes() {
        type BigEnough = BigBytes<{ 100 << 20 }>;

        // For each padding
        for i in 0..4 {
            let big_bytes = Bytes::from(vec![123; 1000 + i]);

            let mut serialized = Vec::new();
            BigEnough::write(&big_bytes, &mut serialized);

            // Must be aligned by 4
            assert_eq!(serialized.len() % 4, 0);

            let mut offset = 0;
            let deserialized = BigEnough::read(&serialized, &mut offset).unwrap();
            // Must be equal
            assert_eq!(big_bytes, deserialized);

            // Must consume all bytes
            assert_eq!(offset, serialized.len());
        }
    }
}
