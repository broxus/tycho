use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_util::future::BoxFuture;
use futures_util::{Future, FutureExt};
use tycho_util::FastHashMap;

use crate::types::{BoxService, Service};

pub struct Router<Request, Q> {
    inner: Arc<Inner<Request, Q>>,
}

impl<Request, Q> Service<Request> for Router<Request, Q>
where
    Request: Send + AsRef<[u8]> + 'static,
    Q: Send + 'static,
{
    type QueryResponse = Q;
    type OnQueryFuture = BoxFutureOrNoop<Option<Self::QueryResponse>>;
    type OnMessageFuture = BoxFutureOrNoop<()>;
    type OnDatagramFuture = BoxFutureOrNoop<()>;

    fn on_query(&self, req: Request) -> Self::OnQueryFuture {
        match find_handler(&req, &self.inner.query_handlers, &self.inner.services) {
            Some(service) => BoxFutureOrNoop::Boxed(service.on_query(req)),
            None => BoxFutureOrNoop::Noop,
        }
    }

    fn on_message(&self, req: Request) -> Self::OnMessageFuture {
        match find_handler(&req, &self.inner.message_handlers, &self.inner.services) {
            Some(service) => BoxFutureOrNoop::Boxed(service.on_message(req)),
            None => BoxFutureOrNoop::Noop,
        }
    }

    fn on_datagram(&self, req: Request) -> Self::OnDatagramFuture {
        match find_handler(&req, &self.inner.datagram_handlers, &self.inner.services) {
            Some(service) => BoxFutureOrNoop::Boxed(service.on_datagram(req)),
            None => BoxFutureOrNoop::Noop,
        }
    }
}

fn find_handler<'a, T: AsRef<[u8]>, S>(
    req: &T,
    indices: &FastHashMap<u32, usize>,
    handlers: &'a [S],
) -> Option<&'a S> {
    if let Some(id) = read_le_u32(req.as_ref()) {
        if let Some(&index) = indices.get(&id) {
            // NOTE: intentionally panics if index is out of bounds as it is
            // an implementation error.
            return Some(handlers.get(index).expect("index must be in bounds"));
        }
    }
    None
}

pub enum BoxFutureOrNoop<T> {
    Boxed(BoxFuture<'static, T>),
    Noop,
}

impl Future for BoxFutureOrNoop<()> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            BoxFutureOrNoop::Boxed(fut) => fut.poll_unpin(cx),
            BoxFutureOrNoop::Noop => std::task::Poll::Ready(()),
        }
    }
}

impl<T> Future for BoxFutureOrNoop<Option<T>> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            BoxFutureOrNoop::Boxed(fut) => fut.poll_unpin(cx),
            BoxFutureOrNoop::Noop => std::task::Poll::Ready(None),
        }
    }
}

struct Inner<Request, Q> {
    services: Vec<BoxService<Request, Q>>,
    query_handlers: FastHashMap<u32, usize>,
    message_handlers: FastHashMap<u32, usize>,
    datagram_handlers: FastHashMap<u32, usize>,
    _response: PhantomData<Q>,
}

fn read_le_u32(buf: &[u8]) -> Option<u32> {
    if buf.len() >= 4 {
        let mut bytes = [0; 4];
        bytes.copy_from_slice(&buf[..4]);
        Some(u32::from_le_bytes(bytes))
    } else {
        None
    }
}
