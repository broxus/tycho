use std::future::Future;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use tycho_network::InboundRequestMeta;

use super::SelfBroadcastListener;

pub trait BroadcastListener: Send + Sync + 'static {
    type HandleMessageFut<'a>: Future<Output = ()> + Send + 'a;

    fn handle_message(
        &self,
        meta: Arc<InboundRequestMeta>,
        message: Bytes,
    ) -> Self::HandleMessageFut<'_>;
}

macro_rules! impl_listener_tuple {
    ($join_fn:path, {
        $($n:tt: $var:ident = $ty:ident,)*
    }) => {
        impl<$($ty,)*> BroadcastListener for ($($ty,)*)
        where
            $($ty: BroadcastListener,)*
        {
            type HandleMessageFut<'a> = HandleMessageFut<'a>;

            fn handle_message(
                &self,
                meta: Arc<InboundRequestMeta>,
                message: Bytes,
            ) -> Self::HandleMessageFut<'_> {
                impl_listener_tuple!(@call self meta message $($n $var)*);
                Box::pin(async move {
                    $join_fn($($var),*).await;
                })
            }
        }
    };
    // Tail
    (@call $self:ident $meta:ident $message:ident $n:tt $var:ident) => {
        let $var = $self.$n.handle_message($meta, $message);
    };
    // Head
    (@call $self:ident $meta:ident $message:ident $n:tt $var:ident $($rest:tt)+) => {
        let $var = $self.$n.handle_message($meta.clone(), $message.clone());
        impl_listener_tuple!(@call $self $meta $message $($rest)*)
    }
}

impl_listener_tuple! {
    futures_util::future::join,
    {
        0: a = T0,
        1: b = T1,
    }
}

impl_listener_tuple! {
    futures_util::future::join3,
    {
        0: a = T0,
        1: b = T1,
        2: c = T2,
    }
}

impl_listener_tuple! {
    futures_util::future::join4,
    {
        0: a = T0,
        1: b = T1,
        2: c = T2,
        3: d = T3,
    }
}

impl_listener_tuple! {
    futures_util::future::join5,
    {
        0: a = T0,
        1: b = T1,
        2: c = T2,
        3: d = T3,
        4: e = T4,
    }
}

pub trait BroadcastListenerExt: Sized {
    fn boxed(self) -> BoxBroadcastListener;
}

impl<T: BroadcastListener> BroadcastListenerExt for T {
    fn boxed(self) -> BoxBroadcastListener {
        castaway::match_type!(self, {
            BoxBroadcastListener as listener => listener,
            listener => BoxBroadcastListener::new(listener),
        })
    }
}

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
pub struct NoopBroadcastListener;

impl BroadcastListener for NoopBroadcastListener {
    type HandleMessageFut<'a> = futures_util::future::Ready<()>;

    #[inline]
    fn handle_message(&self, _: Arc<InboundRequestMeta>, _: Bytes) -> Self::HandleMessageFut<'_> {
        futures_util::future::ready(())
    }
}

#[async_trait::async_trait]
impl SelfBroadcastListener for NoopBroadcastListener {
    async fn handle_message(&self, _: Bytes) {}
}

pub struct BoxBroadcastListener {
    data: AtomicPtr<()>,
    vtable: &'static Vtable,
}

impl BoxBroadcastListener {
    pub fn new<P>(provider: P) -> Self
    where
        P: BroadcastListener,
    {
        let ptr = Box::into_raw(Box::new(provider));

        Self {
            data: AtomicPtr::new(ptr.cast()),
            vtable: const { Vtable::new::<P>() },
        }
    }
}

impl BroadcastListener for BoxBroadcastListener {
    type HandleMessageFut<'a> = HandleMessageFut<'a>;

    #[inline]
    fn handle_message(
        &self,
        meta: Arc<InboundRequestMeta>,
        message: Bytes,
    ) -> Self::HandleMessageFut<'_> {
        unsafe { (self.vtable.handle_message)(&self.data, meta, message) }
    }
}

impl Drop for BoxBroadcastListener {
    fn drop(&mut self) {
        unsafe { (self.vtable.drop)(&mut self.data) }
    }
}

// Vtable must enforce this behavior
unsafe impl Send for BoxBroadcastListener {}
unsafe impl Sync for BoxBroadcastListener {}

struct Vtable {
    handle_message: HandleMessageFn,
    drop: DropFn,
}

impl Vtable {
    const fn new<T: BroadcastListener>() -> &'static Self {
        &Self {
            handle_message: |ptr, meta, message| {
                let provider = unsafe { &*ptr.load(Ordering::Relaxed).cast::<T>() };
                provider.handle_message(meta, message).boxed()
            },
            drop: |ptr| {
                drop(unsafe { Box::<T>::from_raw(ptr.get_mut().cast::<T>()) });
            },
        }
    }
}

type HandleMessageFn =
    for<'a> unsafe fn(&'a AtomicPtr<()>, Arc<InboundRequestMeta>, Bytes) -> HandleMessageFut<'a>;
type DropFn = unsafe fn(&mut AtomicPtr<()>);

type HandleMessageFut<'a> = BoxFuture<'a, ()>;

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    use anyhow::Result;
    use tycho_network::PeerId;

    use super::*;

    #[tokio::test]
    async fn boxed_listener_works() -> Result<()> {
        struct ListenerState {
            messages: AtomicUsize,
            dropped: AtomicUsize,
        }

        struct TestListener {
            state: Arc<ListenerState>,
        }

        impl Drop for TestListener {
            fn drop(&mut self) {
                self.state.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }

        impl BroadcastListener for TestListener {
            type HandleMessageFut<'a> = futures_util::future::Ready<()>;

            fn handle_message(
                &self,
                _: Arc<InboundRequestMeta>,
                _: Bytes,
            ) -> Self::HandleMessageFut<'_> {
                self.state.messages.fetch_add(1, Ordering::Relaxed);
                futures_util::future::ready(())
            }
        }

        let state = Arc::new(ListenerState {
            messages: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
        });
        let boxed = BoxBroadcastListener::new(TestListener {
            state: state.clone(),
        });

        assert_eq!(state.messages.load(Ordering::Acquire), 0);
        assert_eq!(state.dropped.load(Ordering::Acquire), 0);

        let meta = Arc::new(InboundRequestMeta {
            peer_id: PeerId([0u8; 32]),
            origin: tycho_network::Direction::Inbound,
            remote_address: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 123)),
        });
        boxed.handle_message(meta.clone(), Bytes::new()).await;
        assert_eq!(state.messages.load(Ordering::Acquire), 1);
        assert_eq!(state.dropped.load(Ordering::Acquire), 0);

        boxed.handle_message(meta.clone(), Bytes::new()).await;
        assert_eq!(state.messages.load(Ordering::Acquire), 2);
        assert_eq!(state.dropped.load(Ordering::Acquire), 0);

        assert_eq!(Arc::strong_count(&state), 2);
        drop(boxed);

        assert_eq!(state.messages.load(Ordering::Acquire), 2);
        assert_eq!(state.dropped.load(Ordering::Acquire), 1);

        assert_eq!(Arc::strong_count(&state), 1);

        Ok(())
    }
}
