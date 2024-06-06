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
