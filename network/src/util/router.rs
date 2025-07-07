use std::marker::PhantomData;
use std::sync::Arc;

use tycho_util::FastHashMap;
use tycho_util::futures::BoxFutureOrNoop;

use crate::types::{BoxService, Service, ServiceExt};

pub trait Routable {
    #[inline]
    fn query_ids(&self) -> impl IntoIterator<Item = u32> {
        std::iter::empty()
    }

    #[inline]
    fn message_ids(&self) -> impl IntoIterator<Item = u32> {
        std::iter::empty()
    }
}

pub struct RouterBuilder<Request, Q> {
    inner: Inner<Request, Q>,
}

impl<Request, Q> RouterBuilder<Request, Q> {
    pub fn route<S>(mut self, service: S) -> Self
    where
        S: Service<Request, QueryResponse = Q> + Routable + Send + Sync + 'static,
    {
        let index = self.inner.services.len();
        for id in service.query_ids() {
            let prev = self.inner.query_handlers.insert(id, index);
            assert!(prev.is_none(), "duplicate query id: {id:08x}");
        }
        for id in service.message_ids() {
            let prev = self.inner.message_handlers.insert(id, index);
            assert!(prev.is_none(), "duplicate message id: {id:08x}");
        }

        self.inner.services.push(service.boxed());
        self
    }

    pub fn build(self) -> Router<Request, Q> {
        Router {
            inner: Arc::new(self.inner),
        }
    }
}

impl<Request, Q> Default for RouterBuilder<Request, Q> {
    fn default() -> Self {
        Self {
            inner: Inner {
                services: Vec::new(),
                query_handlers: FastHashMap::default(),
                message_handlers: FastHashMap::default(),
                _response: PhantomData,
            },
        }
    }
}

pub struct Router<Request, Q> {
    inner: Arc<Inner<Request, Q>>,
}

impl<Request, Q> Router<Request, Q> {
    pub fn builder() -> RouterBuilder<Request, Q> {
        RouterBuilder::default()
    }
}

impl<Request, Q> Clone for Router<Request, Q> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<Request, Q> Service<Request> for Router<Request, Q>
where
    Request: Send + AsRef<[u8]> + 'static,
    Q: Send + 'static,
{
    type QueryResponse = Q;
    type OnQueryFuture = BoxFutureOrNoop<Option<Self::QueryResponse>>;
    type OnMessageFuture = BoxFutureOrNoop<()>;

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

struct Inner<Request, Q> {
    services: Vec<BoxService<Request, Q>>,
    query_handlers: FastHashMap<u32, usize>,
    message_handlers: FastHashMap<u32, usize>,
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
