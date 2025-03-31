use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use futures_util::future::BoxFuture;

pub trait Service<Request> {
    type QueryResponse: Send + 'static;
    type OnQueryFuture: Future<Output = Option<Self::QueryResponse>> + Send + 'static;
    type OnMessageFuture: Future<Output = ()> + Send + 'static;

    /// Called when a query is received.
    ///
    /// Returns a future that resolves to the either response to the query if `Some`,
    /// or cancellation of the query if `None`.
    fn on_query(&self, req: Request) -> Self::OnQueryFuture;

    /// Called when a message is received.
    fn on_message(&self, req: Request) -> Self::OnMessageFuture;
}

pub trait ServiceExt<Request>: Service<Request> {
    #[inline]
    fn boxed(self) -> BoxService<Request, Self::QueryResponse>
    where
        Self: Sized + Send + Sync + 'static,
        Self::OnQueryFuture: Send + 'static,
        Self::OnMessageFuture: Send + 'static,
    {
        BoxService::new(self)
    }

    #[inline]
    fn boxed_clone(self) -> BoxCloneService<Request, Self::QueryResponse>
    where
        Self: Clone + Sized + Send + Sync + 'static,
        Self::OnQueryFuture: Send + 'static,
        Self::OnMessageFuture: Send + 'static,
    {
        BoxCloneService::new(self)
    }
}

impl<T, Request> ServiceExt<Request> for T where T: Service<Request> + ?Sized {}

impl<'a, S, Request> Service<Request> for &'a S
where
    S: Service<Request> + Sync + 'a,
{
    type QueryResponse = S::QueryResponse;
    type OnQueryFuture = S::OnQueryFuture;
    type OnMessageFuture = S::OnMessageFuture;

    #[inline]
    fn on_query(&self, req: Request) -> Self::OnQueryFuture {
        <S as Service<Request>>::on_query(*self, req)
    }

    #[inline]
    fn on_message(&self, req: Request) -> Self::OnMessageFuture {
        <S as Service<Request>>::on_message(*self, req)
    }
}

impl<S, Request> Service<Request> for Arc<S>
where
    S: Service<Request> + Sync + ?Sized,
{
    type QueryResponse = S::QueryResponse;
    type OnQueryFuture = S::OnQueryFuture;
    type OnMessageFuture = S::OnMessageFuture;

    #[inline]
    fn on_query(&self, req: Request) -> Self::OnQueryFuture {
        <S as Service<Request>>::on_query(self.as_ref(), req)
    }

    #[inline]
    fn on_message(&self, req: Request) -> Self::OnMessageFuture {
        <S as Service<Request>>::on_message(self.as_ref(), req)
    }
}

impl<S, Request> Service<Request> for Box<S>
where
    S: Service<Request> + ?Sized,
{
    type QueryResponse = S::QueryResponse;
    type OnQueryFuture = S::OnQueryFuture;
    type OnMessageFuture = S::OnMessageFuture;

    #[inline]
    fn on_query(&self, req: Request) -> Self::OnQueryFuture {
        <S as Service<Request>>::on_query(self.as_ref(), req)
    }

    #[inline]
    fn on_message(&self, req: Request) -> Self::OnMessageFuture {
        <S as Service<Request>>::on_message(self.as_ref(), req)
    }
}

#[repr(transparent)]
pub struct BoxService<Request, Q> {
    inner: Box<DynBoxService<Request, Q>>,
}

type DynBoxService<Request, Q> = dyn Service<
        Request,
        QueryResponse = Q,
        OnQueryFuture = BoxFuture<'static, Option<Q>>,
        OnMessageFuture = BoxFuture<'static, ()>,
    > + Send
    + Sync;

impl<Request, Q> BoxService<Request, Q> {
    pub fn new<S>(inner: S) -> Self
    where
        S: Service<Request, QueryResponse = Q> + Send + Sync + 'static,
        S::OnQueryFuture: Send + 'static,
        S::OnMessageFuture: Send + 'static,
    {
        BoxService {
            inner: Box::new(BoxPinFutures(inner)),
        }
    }
}

impl<Request, Q> Service<Request> for BoxService<Request, Q>
where
    Request: Send + 'static,
    Q: Send + 'static,
{
    type QueryResponse = Q;
    type OnQueryFuture = BoxFuture<'static, Option<Q>>;
    type OnMessageFuture = BoxFuture<'static, ()>;

    #[inline]
    fn on_query(&self, req: Request) -> Self::OnQueryFuture {
        self.inner.on_query(req)
    }

    #[inline]
    fn on_message(&self, req: Request) -> Self::OnMessageFuture {
        self.inner.on_message(req)
    }
}

#[repr(transparent)]
pub struct BoxCloneService<Request, Q> {
    inner: Box<DynBoxCloneService<Request, Q>>,
}

type DynBoxCloneService<Request, Q> = dyn CloneService<
        Request,
        QueryResponse = Q,
        OnQueryFuture = BoxFuture<'static, Option<Q>>,
        OnMessageFuture = BoxFuture<'static, ()>,
    > + Send
    + Sync;

impl<Request, Q> BoxCloneService<Request, Q>
where
    Q: Send + 'static,
{
    pub fn new<S>(inner: S) -> Self
    where
        S: Service<Request, QueryResponse = Q> + Clone + Send + Sync + 'static,
        S::OnQueryFuture: Send + 'static,
        S::OnMessageFuture: Send + 'static,
    {
        BoxCloneService {
            inner: Box::new(BoxPinFutures(inner)),
        }
    }
}

impl<Request, Q> Service<Request> for BoxCloneService<Request, Q>
where
    Request: Send + 'static,
    Q: Send + 'static,
{
    type QueryResponse = Q;
    type OnQueryFuture = BoxFuture<'static, Option<Q>>;
    type OnMessageFuture = BoxFuture<'static, ()>;

    #[inline]
    fn on_query(&self, req: Request) -> Self::OnQueryFuture {
        self.inner.on_query(req)
    }

    #[inline]
    fn on_message(&self, req: Request) -> Self::OnMessageFuture {
        self.inner.on_message(req)
    }
}

impl<Request, Q> Clone for BoxCloneService<Request, Q>
where
    Q: Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        BoxCloneService {
            inner: self.inner.clone_box(),
        }
    }
}

trait CloneService<Request>: Service<Request> {
    fn clone_box(&self) -> Box<DynCloneService<Self, Request>>;
}

impl<Request, S> CloneService<Request> for S
where
    S: Service<Request> + Clone + Send + Sync + 'static,
    S::OnQueryFuture: Send + 'static,
    S::OnMessageFuture: Send + 'static,
{
    fn clone_box(&self) -> Box<DynCloneService<Self, Request>> {
        Box::new(self.clone())
    }
}

type DynCloneService<S, Request> = dyn CloneService<
        Request,
        QueryResponse = <S as Service<Request>>::QueryResponse,
        OnQueryFuture = <S as Service<Request>>::OnQueryFuture,
        OnMessageFuture = <S as Service<Request>>::OnMessageFuture,
    > + Send
    + Sync;

#[repr(transparent)]
struct BoxPinFutures<S>(S);

impl<S: Clone> Clone for BoxPinFutures<S> {
    #[inline]
    fn clone(&self) -> Self {
        BoxPinFutures(self.0.clone())
    }
}

impl<S, Request> Service<Request> for BoxPinFutures<S>
where
    S: Service<Request>,
{
    type QueryResponse = S::QueryResponse;
    type OnQueryFuture = BoxFuture<'static, Option<S::QueryResponse>>;
    type OnMessageFuture = BoxFuture<'static, ()>;

    #[inline]
    fn on_query(&self, req: Request) -> Self::OnQueryFuture {
        let f = self.0.on_query(req);
        match castaway::cast!(f, Self::OnQueryFuture) {
            Ok(f) => f,
            Err(f) => Box::pin(f),
        }
    }

    #[inline]
    fn on_message(&self, req: Request) -> Self::OnMessageFuture {
        let f = self.0.on_message(req);
        match castaway::cast!(f, Self::OnMessageFuture) {
            Ok(f) => f,
            Err(f) => Box::pin(f),
        }
    }
}

pub fn service_query_fn<T>(f: T) -> ServiceQueryFn<T> {
    ServiceQueryFn { f }
}

pub struct ServiceQueryFn<T> {
    f: T,
}

impl<T: Clone> Clone for ServiceQueryFn<T> {
    #[inline]
    fn clone(&self) -> Self {
        ServiceQueryFn { f: self.f.clone() }
    }
}

impl<Request, Q, T, F> Service<Request> for ServiceQueryFn<T>
where
    Q: Send + 'static,
    T: Fn(Request) -> F + Send + 'static,
    F: Future<Output = Option<Q>> + Send + 'static,
{
    type QueryResponse = Q;
    type OnQueryFuture = F;
    type OnMessageFuture = futures_util::future::Ready<()>;

    #[inline]
    fn on_query(&self, req: Request) -> Self::OnQueryFuture {
        (self.f)(req)
    }

    #[inline]
    fn on_message(&self, _req: Request) -> Self::OnMessageFuture {
        futures_util::future::ready(())
    }
}

pub fn service_message_fn<Q, T>(f: T) -> ServiceMessageFn<Q, T> {
    ServiceMessageFn {
        f,
        _response: PhantomData,
    }
}

impl<Q, T: Clone> Clone for ServiceMessageFn<Q, T> {
    #[inline]
    fn clone(&self) -> Self {
        ServiceMessageFn {
            f: self.f.clone(),
            _response: PhantomData,
        }
    }
}

pub struct ServiceMessageFn<Q, T> {
    f: T,
    _response: PhantomData<Q>,
}

impl<Request, Q, T, F> Service<Request> for ServiceMessageFn<Q, T>
where
    Q: Send + 'static,
    T: Fn(Request) -> F + Send + 'static,
    F: Future<Output = ()> + Send + 'static,
{
    type QueryResponse = Q;
    type OnQueryFuture = futures_util::future::Ready<Option<Q>>;
    type OnMessageFuture = F;

    #[inline]
    fn on_query(&self, _req: Request) -> Self::OnQueryFuture {
        futures_util::future::ready(None)
    }

    #[inline]
    fn on_message(&self, req: Request) -> Self::OnMessageFuture {
        (self.f)(req)
    }
}