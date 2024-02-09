pub use self::futures::BoxFutureOrNoop;
pub use self::router::{Routable, Router, RouterBuilder};
pub use self::traits::NetworkExt;

mod futures;
mod router;
mod traits;

#[macro_export]
macro_rules! match_tl_request {
    ($req_body:expr, {
        $($ty:path as $pat:pat => $expr:expr),*$(,)?
    }, $err:pat => $err_exr:expr) => {
        '__match_req: {
            let $err = if ($req_body).len() >= 4 {
                match ($req_body).as_ref().get_u32_le() {
                    $(
                        <$ty>::TL_ID => match $crate::__internal::tl_proto::deserialize::<$ty>(&($req_body)) {
                            Ok($pat) => break '__match_req ($expr),
                            Err(e) => e,
                        }
                    )*
                    _ => $crate::__internal::tl_proto::TlError::UnknownConstructor,
                }
            } else {
                $crate::__internal::tl_proto::TlError::UnexpectedEof
            };
            $err_exr
        }
    };
}
