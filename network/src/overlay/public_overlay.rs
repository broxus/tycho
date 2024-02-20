use tycho_util::FastDashMap;

use crate::overlay::OverlayId;
use crate::proto::overlay::{PublicEntry, PublicEntryToSign};
use crate::types::{BoxService, PeerId, Response, Service, ServiceExt, ServiceRequest};

pub struct PublicOverlayBuilder {
    overlay_id: OverlayId,
}

impl PublicOverlayBuilder {
    pub fn build<S>(self, service: S) -> PublicOverlay
    where
        S: Send + Sync + 'static,
        S: Service<ServiceRequest, QueryResponse = Response>,
    {
        PublicOverlay {
            overlay_id: self.overlay_id,
            entries: FastDashMap::default(),
            service: service.boxed(),
        }
    }
}

pub struct PublicOverlay {
    overlay_id: OverlayId,
    entries: FastDashMap<PeerId, StoredEntry>,
    service: BoxService<ServiceRequest, Response>,
}

impl PublicOverlay {
    pub fn builder(overlay_id: OverlayId) -> PublicOverlayBuilder {
        PublicOverlayBuilder { overlay_id }
    }

    #[inline]
    pub fn overlay_id(&self) -> &OverlayId {
        &self.overlay_id
    }

    #[inline]
    pub fn service(&self) -> &BoxService<ServiceRequest, Response> {
        &self.service
    }

    pub fn add_entires(&self, entires: &[PublicEntry]) {
        for entry in entires {
            let Some(pubkey) = entry.peer_id.as_public_key() else {
                continue;
            };

            if !pubkey.verify(
                PublicEntryToSign {
                    overlay_id: self.overlay_id.as_bytes(),
                    peer_id: &entry.peer_id,
                    created_at: entry.created_at,
                },
                &entry.signature,
            ) {
                continue;
            }

            self.entries.insert(
                entry.peer_id,
                StoredEntry {
                    created_at: entry.created_at,
                    signature: entry.signature.clone(), // TODO: maybe use arc?
                },
            );
        }
    }
}

struct StoredEntry {
    created_at: u32,
    signature: Box<[u8; 64]>,
}
