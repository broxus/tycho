use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use bytes::Bytes;
use futures_util::{Stream, TryStream};
use pin_project_lite::pin_project;
use serde::{Deserialize, Serialize};

use crate::types::PeerId;

type Error = std::io::Error;
type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u16)]
pub enum Version {
    #[default]
    V1 = 1,
}

impl Version {
    pub fn try_from_u16(value: u16) -> Option<Self> {
        match value {
            1 => Some(Self::V1),
            _ => None,
        }
    }

    pub fn to_u16(self) -> u16 {
        self as u16
    }
}

impl TryFrom<u16> for Version {
    type Error = anyhow::Error;

    fn try_from(value: u16) -> anyhow::Result<Self, Self::Error> {
        match Self::try_from_u16(value) {
            Some(version) => Ok(version),
            None => Err(anyhow::anyhow!("invalid version: {value}")),
        }
    }
}

impl Serialize for Version {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u16(self.to_u16())
    }
}

impl<'de> Deserialize<'de> for Version {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        u16::deserialize(deserializer).and_then(|v| Self::try_from(v).map_err(Error::custom))
    }
}

pub struct Request {
    pub version: Version,
    pub body: Body,
}

impl Request {
    pub fn new_simple<T: Into<Bytes>>(body: T) -> Self {
        Self {
            version: Default::default(),
            body: Body::simple(body),
        }
    }

    pub fn from_tl<T>(body: T) -> Self
    where
        T: tl_proto::TlWrite<Repr = tl_proto::Boxed>,
    {
        Self {
            version: Default::default(),
            body: Body::simple(tl_proto::serialize(body)),
        }
    }
}

#[derive(Debug)]
pub struct Response {
    pub version: Version,
    pub body: Bytes,
}

impl Response {
    pub fn from_tl<T>(body: T) -> Self
    where
        T: tl_proto::TlWrite<Repr = tl_proto::Boxed>,
    {
        Self {
            version: Default::default(),
            body: tl_proto::serialize(body).into(),
        }
    }

    pub fn parse_tl<T>(&self) -> tl_proto::TlResult<T>
    where
        for<'a> T: tl_proto::TlRead<'a, Repr = tl_proto::Boxed>,
    {
        tl_proto::deserialize(self.body.as_ref())
    }
}

impl AsRef<[u8]> for Response {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.body.as_ref()
    }
}

pub struct ServiceRequest {
    pub metadata: Arc<InboundRequestMeta>,
    pub body: Bytes,
}

impl ServiceRequest {
    pub fn parse_tl<T>(&self) -> tl_proto::TlResult<T>
    where
        for<'a> T: tl_proto::TlRead<'a, Repr = tl_proto::Boxed>,
    {
        tl_proto::deserialize(self.body.as_ref())
    }
}

impl AsRef<[u8]> for ServiceRequest {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.body.as_ref()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InboundRequestMeta {
    pub peer_id: PeerId,
    pub origin: Direction,
    #[serde(with = "tycho_util::serde_helpers::socket_addr")]
    pub remote_address: SocketAddr,
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    Inbound,
    Outbound,
}

impl std::fmt::Display for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Inbound => "inbound",
            Self::Outbound => "outbound",
        })
    }
}

// === Body ===

#[derive(Debug)]
#[repr(transparent)]
pub struct Body(BodyContent);

impl Body {
    pub fn new<T: BodyImpl + Send + 'static>(body: T) -> Self {
        match castaway::cast!(body, BoxBody) {
            Ok(body) => Self(BodyContent::Boxed(body)),
            Err(body) => Self(BodyContent::Boxed(BoxBody::new(body))),
        }
    }

    #[inline]
    pub fn simple<T: Into<Bytes>>(body: T) -> Self {
        Self(BodyContent::Simple(body.into()))
    }

    pub fn from_tl<T>(body: T) -> Self
    where
        T: tl_proto::TlWrite<Repr = tl_proto::Boxed>,
    {
        Self(BodyContent::Simple(tl_proto::serialize(body).into()))
    }

    /// Create a new `Body` from a [`Stream`].
    ///
    /// [`Stream`]: https://docs.rs/futures-core/latest/futures_core/stream/trait.Stream.html
    pub fn from_stream<S>(stream: S) -> Self
    where
        S: TryStream<Ok: Into<Bytes>, Error: Into<Error>> + Send + 'static,
    {
        Self::new(StreamToBody {
            inner: SyncWrapper(stream),
        })
    }

    pub fn with_prefix(self, prefix: Bytes) -> Self {
        Self::new(BodyWithPrefix {
            prefix: (!prefix.is_empty()).then_some(prefix),
            body: self,
        })
    }

    pub fn into_stream(self) -> BodyStream {
        BodyStream { inner: self.0 }
    }
}

macro_rules! impl_body_from {
    ($($ty:ty),*$(,)?) => {
        $(impl From<$ty> for Body {
            #[inline]
            fn from(value: $ty) -> Self {
                Self::simple(value)
            }
        })*
    };
}

impl_body_from!(
    &'static [u8],
    &'static str,
    Box<[u8]>,
    Vec<u8>,
    String,
    Bytes
);

impl BodyImpl for Body {
    fn poll_chunk(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        match &mut self.0 {
            BodyContent::Simple(bytes) => Poll::Ready(if bytes.is_empty() {
                None
            } else {
                Some(Ok(std::mem::take(bytes)))
            }),
            BodyContent::Boxed(boxed) => Pin::new(boxed).poll_chunk(cx),
        }
    }

    fn is_end_stream(&self) -> bool {
        match &self.0 {
            BodyContent::Simple(bytes) => bytes.is_empty(),
            BodyContent::Boxed(boxed) => boxed.is_end_stream(),
        }
    }

    fn size_hint(&self) -> SizeHint {
        match &self.0 {
            BodyContent::Simple(bytes) => {
                let len = bytes.len() as u64;
                (len, Some(len))
            }
            BodyContent::Boxed(boxed) => boxed.size_hint(),
        }
    }
}

enum BodyContent {
    Simple(Bytes),
    Boxed(BoxBody),
}

impl std::fmt::Debug for BodyContent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Simple(_) => f.debug_struct("Simple").finish(),
            Self::Boxed(_) => f.debug_struct("Boxed").finish(),
        }
    }
}

// === Body as stream ===

#[derive(Debug)]
#[repr(transparent)]
pub struct BodyStream {
    inner: BodyContent,
}

impl Stream for BodyStream {
    type Item = std::io::Result<Bytes>;

    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.inner {
            BodyContent::Simple(bytes) => Poll::Ready(if bytes.is_empty() {
                None
            } else {
                Some(Ok(std::mem::take(bytes)))
            }),
            BodyContent::Boxed(boxed) => Pin::new(boxed).poll_chunk(cx),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.inner {
            BodyContent::Simple(bytes) => {
                let len = bytes.len();
                (len, Some(len))
            }
            BodyContent::Boxed(boxed) => {
                let size_hint = boxed.size_hint();
                let lower = usize::try_from(size_hint.0).unwrap_or_default();
                let upper = size_hint.1.and_then(|v| usize::try_from(v).ok());
                (lower, upper)
            }
        }
    }
}

// === Body trait ===

pub trait BodyImpl {
    /// Attempt to pull out the next data buffer of this stream.
    fn poll_chunk(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>>;

    /// Returns `true` when the end of stream has been reached.
    ///
    /// An end of stream means that `poll_chunk` will return `None`.
    ///
    /// A return value of `false` **does not** guarantee that a value will be
    /// returned from `poll_chunk`.
    fn is_end_stream(&self) -> bool {
        false
    }

    /// Returns the bounds on the remaining length of the stream.
    ///
    /// When the **exact** remaining length of the stream is known, the upper bound will be set and
    /// will equal the lower bound.
    fn size_hint(&self) -> SizeHint {
        (0, None)
    }
}

pub type SizeHint = (u64, Option<u64>);

impl<T: BodyImpl + Unpin + ?Sized> BodyImpl for &mut T {
    #[inline]
    fn poll_chunk(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        Pin::new(&mut **self).poll_chunk(cx)
    }

    #[inline]
    fn is_end_stream(&self) -> bool {
        Pin::new(&**self).is_end_stream()
    }

    #[inline]
    fn size_hint(&self) -> SizeHint {
        Pin::new(&**self).size_hint()
    }
}

impl<T: Unpin + std::ops::DerefMut<Target: BodyImpl>> BodyImpl for Pin<T> {
    #[inline]
    fn poll_chunk(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        Pin::get_mut(self).as_mut().poll_chunk(cx)
    }

    #[inline]
    fn is_end_stream(&self) -> bool {
        self.as_ref().is_end_stream()
    }

    #[inline]
    fn size_hint(&self) -> SizeHint {
        self.as_ref().size_hint()
    }
}

impl<T: BodyImpl + Unpin + ?Sized> BodyImpl for Box<T> {
    #[inline]
    fn poll_chunk(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        Pin::new(&mut **self).poll_chunk(cx)
    }

    #[inline]
    fn is_end_stream(&self) -> bool {
        Pin::new(&**self).is_end_stream()
    }

    #[inline]
    fn size_hint(&self) -> SizeHint {
        Pin::new(&**self).size_hint()
    }
}

// === Body combinators ===

/// A boxed [`Body`] trait object that is !Sync.
pub struct BoxBody {
    inner: Pin<Box<dyn BodyImpl + Send + 'static>>,
}

impl BoxBody {
    /// Create a new `BoxBody`.
    #[inline]
    pub fn new<B: BodyImpl + Send + 'static>(body: B) -> Self {
        Self {
            inner: Box::pin(body),
        }
    }
}

impl std::fmt::Debug for BoxBody {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnsyncBoxBody").finish()
    }
}

impl BodyImpl for BoxBody {
    #[inline]
    fn poll_chunk(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        self.inner.as_mut().poll_chunk(cx)
    }

    #[inline]
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    #[inline]
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

// === Body stream ===

pin_project! {
    struct StreamToBody<S> {
        #[pin]
        inner: SyncWrapper<S>,
    }
}

impl<S: TryStream<Ok: Into<Bytes>, Error: Into<Error>>> BodyImpl for StreamToBody<S> {
    fn poll_chunk(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        let stream = self.project().inner.get_pin_mut();
        match ready!(stream.try_poll_next(cx)) {
            Some(Ok(chunk)) => Poll::Ready(Some(Ok(chunk.into()))),
            Some(Err(e)) => Poll::Ready(Some(Err(e.into()))),
            None => Poll::Ready(None),
        }
    }
}

struct SyncWrapper<T>(T);

impl<T> SyncWrapper<T> {
    pub fn get_pin_mut(self: Pin<&mut Self>) -> Pin<&mut T> {
        unsafe { Pin::map_unchecked_mut(self, |this| &mut this.0) }
    }
}

unsafe impl<T> Sync for SyncWrapper<T> {}

// === Body with prefix ===

struct BodyWithPrefix {
    prefix: Option<Bytes>,
    body: Body,
}

impl BodyImpl for BodyWithPrefix {
    fn poll_chunk(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        if let Some(prefix) = self.prefix.take() {
            Poll::Ready(Some(Ok(prefix)))
        } else {
            Pin::new(&mut self.body).poll_chunk(cx)
        }
    }

    fn is_end_stream(&self) -> bool {
        self.prefix.is_none() && self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        let prefix_len = self.prefix.as_ref().map(Bytes::len).unwrap_or_default() as u64;
        let (lower, upper) = self.body.size_hint();
        (prefix_len + lower, upper.map(|u| prefix_len + u))
    }
}
