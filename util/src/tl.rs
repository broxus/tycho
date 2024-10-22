use bytes::Bytes;
use tl_proto::{TlError, TlPacket, TlRead, TlResult, TlWrite};

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

/// A [`Bytes`]-like object wrapper.
#[repr(transparent)]
pub struct BytesLike<T = Bytes>(pub T);

impl<T> BytesLike<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T: Default> Default for BytesLike<T> {
    #[inline]
    fn default() -> Self {
        Self(T::default())
    }
}

impl<T> std::ops::Deref for BytesLike<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for BytesLike<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: AsRef<[u8]>> std::fmt::Debug for BytesLike<T> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self.0.as_ref(), f)
    }
}

impl<T: AsRef<[u8]>> Eq for BytesLike<T> {}

impl<T: AsRef<[u8]>> PartialEq for BytesLike<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_ref().eq(other.0.as_ref())
    }
}

impl<T: Clone> Clone for BytesLike<T> {
    #[inline]
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: AsRef<[u8]>> TlWrite for BytesLike<T> {
    type Repr = tl_proto::Boxed;

    fn max_size_hint(&self) -> usize {
        tl_proto::bytes_max_size_hint(self.0.as_ref().len())
    }

    fn write_to<P: tl_proto::TlPacket>(&self, packet: &mut P) {
        self.0.as_ref().write_to(packet);
    }
}

impl<'a, T: TlRead<'a>> TlRead<'a> for BytesLike<T> {
    type Repr = tl_proto::Boxed;

    fn read_from(packet: &'a [u8], offset: &mut usize) -> tl_proto::TlResult<Self> {
        T::read_from(packet, offset).map(Self)
    }
}

impl<T> From<T> for BytesLike<T> {
    #[inline]
    fn from(value: T) -> Self {
        Self(value)
    }
}

impl<T: AsRef<[u8]> + 'static> From<BytesLike<T>> for Bytes {
    fn from(value: BytesLike<T>) -> Self {
        castaway::match_type!(value.0, {
            Bytes as v => v,
            Vec<u8> as v => Bytes::from(v),
            Box<[u8]> as v => Bytes::from(v),
            String as v => Bytes::from(v),
            &'static [u8] as v => Bytes::from_static(v),
            &'static str as v => Bytes::from_static(v.as_bytes()),
            s => Bytes::copy_from_slice(s.as_ref()),
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

    #[inline]
    pub fn size_hint<T: AsRef<[u8]>>(bytes: &T) -> usize {
        BigBytesRef::<MAX_SIZE>::size_hint(bytes)
    }

    #[inline]
    pub fn write<P: TlPacket>(bytes: &[u8], packet: &mut P) {
        BigBytesRef::<MAX_SIZE>::write(bytes, packet);
    }

    #[inline]
    pub fn read(packet: &[u8], offset: &mut usize) -> TlResult<Bytes> {
        BigBytesRef::<MAX_SIZE>::read(packet, offset).map(Bytes::copy_from_slice)
    }
}

pub struct BigBytesRef<const MAX_SIZE: usize>;

impl<const MAX_SIZE: usize> BigBytesRef<MAX_SIZE> {
    pub const MAX_SIZE: usize = MAX_SIZE;

    pub fn size_hint<T: AsRef<[u8]>>(bytes: &T) -> usize {
        let len = bytes.as_ref().len();
        4 + len + big_bytes_padding(len)
    }

    pub fn write<P: TlPacket>(bytes: &[u8], packet: &mut P) {
        const PADDING: [u8; 3] = [0; 3];

        let len = bytes.len();
        packet.write_u32(len as u32);
        packet.write_raw_slice(bytes);
        if len % 4 != 0 {
            packet.write_raw_slice(&PADDING[0..4 - len % 4]);
        }
    }

    pub fn read<'tl>(packet: &'tl [u8], offset: &mut usize) -> TlResult<&'tl [u8]> {
        let len = u32::read_from(packet, offset)? as usize;
        if len > Self::MAX_SIZE {
            return Err(tl_proto::TlError::InvalidData);
        }
        let padding = big_bytes_padding(len);

        if offset.saturating_add(len + padding) > packet.len() {
            return Err(tl_proto::TlError::UnexpectedEof);
        }

        let bytes = &packet[*offset..*offset + len];
        *offset += len + padding;

        Ok(bytes)
    }
}

const fn big_bytes_padding(len: usize) -> usize {
    (4 - len % 4) % 4
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
