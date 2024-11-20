use bytes::Buf;

pub use self::router::{Routable, Router, RouterBuilder};
#[cfg(test)]
pub use self::test::make_peer_info_stub;
pub use self::traits::{NetworkExt, UnknownPeerError};
use crate::types::PeerId;

mod router;
mod traits;

#[cfg(test)]
mod test;

#[macro_export]
macro_rules! match_tl_request {
    ($req_body:expr, $(tag = $tag:expr,)? {
        $($ty:path as $pat:pat => $expr:expr),*$(,)?
    }, $err:pat => $err_exr:expr) => {
        '__match_req: {
            let $err = $crate::match_tl_request!(@inner $req_body, $($tag)?, {
                $(
                    <$ty>::TL_ID => match $crate::__internal::tl_proto::deserialize::<$ty>(&($req_body)) {
                        Ok($pat) => break '__match_req ($expr),
                        Err(e) => e,
                    }
                )*
                _ => $crate::__internal::tl_proto::TlError::UnknownConstructor,
            });
            $err_exr
        }
    };

    (@inner $req_body:expr, $tag:expr, $($rest:tt)*) => {
        match $tag $($rest)*
    };
    (@inner $req_body:expr, , $($rest:tt)*) => {
        if ($req_body).len() >= 4 {
            match ($req_body).as_ref().get_u32_le() $($rest)*
        } else {
            $crate::__internal::tl_proto::TlError::UnexpectedEof
        }
    };
}

pub fn check_peer_signature<T>(peed_id: &PeerId, signature: &[u8; 64], data: &T) -> bool
where
    T: tl_proto::TlWrite,
{
    let Some(public_key) = peed_id.as_public_key() else {
        return false;
    };
    public_key.verify(data, signature)
}

pub fn try_handle_prefix<T>(req: &T) -> Result<(u32, &[u8]), tl_proto::TlError>
where
    T: AsRef<[u8]>,
{
    let body = req.as_ref();
    if body.len() < 4 {
        return Err(tl_proto::TlError::UnexpectedEof);
    }

    let constructor = std::convert::identity(body).get_u32_le();
    Ok((constructor, body))
}

pub fn try_handle_prefix_with_offset<T>(req: &T) -> Result<(u32, &[u8]), tl_proto::TlError>
where
    T: AsRef<[u8]>,
{
    let body = req.as_ref();
    if body.len() < 4 {
        return Err(tl_proto::TlError::UnexpectedEof);
    }

    let constructor = std::convert::identity(body).get_u32_le();
    Ok((constructor, &body[4..]))
}
