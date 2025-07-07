use std::sync::Arc;

use anyhow::Result;
use pkcs8::EncodePrivateKey;
use rustls::client::danger::HandshakeSignatureValid;
use rustls::crypto::{KeyProvider, WebPkiSupportedAlgorithms};
use rustls::pki_types::CertificateDer;
use rustls::sign::CertifiedKey;
use rustls::{DigitallySignedStruct, DistinguishedName, InconsistentKeys, SignatureScheme};
use webpki::types::{PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, SubjectPublicKeyInfoDer};

use crate::types::PeerId;

pub(crate) fn generate_cert(
    keypair: &ed25519::KeypairBytes,
    key_provider: &dyn KeyProvider,
) -> Result<Arc<CertifiedKey>> {
    // TODO: use zeroize for `rustls::PrivateKey` contents?

    let pkcs8 = keypair.to_pkcs8_der()?;
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(pkcs8.as_bytes().to_vec()));
    let server_private_key = key_provider.load_private_key(key_der)?;
    let server_public_key = server_private_key
        .public_key()
        .ok_or(rustls::Error::InconsistentKeys(InconsistentKeys::Unknown))
        .expect("cannot load public key");
    let server_public_key_as_cert = CertificateDer::from(server_public_key.to_vec());

    let certified_key = Arc::new(CertifiedKey::new(
        vec![server_public_key_as_cert],
        server_private_key,
    ));

    Ok(certified_key)
}

pub(crate) fn peer_id_from_certificate(cert: &CertificateDer<'_>) -> Result<PeerId, rustls::Error> {
    const PUBKEY_PREFIX: &[u8] = &[
        0x30, 42, // DER SEQUENCE of length 42
        0x30, 5, 6, 3, 43, 101, 112, // DER SEQUENCE of length 5 with ed25519 id
        0x03, 33, 0, // DER BIT STRING of length 32 (with one zero byte prefix)
    ];

    fn parse_pubkey(pubkey: &[u8]) -> Option<PeerId> {
        let pubkey: [u8; 32] = pubkey.strip_prefix(PUBKEY_PREFIX)?.try_into().ok()?;
        tycho_crypto::ed25519::PublicKey::from_bytes(pubkey).map(PeerId::from)
    }

    parse_pubkey(cert.as_ref())
        .ok_or_else(|| rustls::Error::InvalidCertificate(rustls::CertificateError::BadEncoding))
}

#[derive(Debug)]
pub(crate) struct CertVerifierWithPeerId {
    peer_id: PeerId,
}

impl CertVerifierWithPeerId {
    pub fn new(peer_id: &PeerId) -> Self {
        Self { peer_id: *peer_id }
    }
}

impl rustls::client::danger::ServerCertVerifier for CertVerifierWithPeerId {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        if !intermediates.is_empty() {
            return Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::BadEncoding,
            ));
        }

        let peer_id = peer_id_from_certificate(end_entity)?;
        if peer_id != self.peer_id {
            return Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::Other(rustls::OtherError(Arc::new(
                    CertificatePeerIdMismatch,
                ))),
            ));
        }

        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &CertificateDer<'_>,
        _: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Err(tls12_unexpected())
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature_with_raw_key(
            message,
            &SubjectPublicKeyInfoDer::from(cert.as_ref()),
            dss,
            &SUPPORTED_SIG_ALGS,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        SUPPORTED_VERIFY_SCHEMES.to_vec()
    }

    fn requires_raw_public_keys(&self) -> bool {
        true
    }
}

/// Verifies self-signed certificates for the specified SNI.
#[derive(Debug)]
pub(crate) struct CertVerifier;

impl rustls::server::danger::ClientCertVerifier for CertVerifier {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _: rustls::pki_types::UnixTime,
    ) -> Result<rustls::server::danger::ClientCertVerified, rustls::Error> {
        if !intermediates.is_empty() {
            return Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::BadEncoding,
            ));
        }

        peer_id_from_certificate(end_entity)
            .map(|_| rustls::server::danger::ClientCertVerified::assertion())
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &CertificateDer<'_>,
        _: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Err(tls12_unexpected())
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature_with_raw_key(
            message,
            &SubjectPublicKeyInfoDer::from(cert.as_ref()),
            dss,
            &SUPPORTED_SIG_ALGS,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        SUPPORTED_VERIFY_SCHEMES.to_vec()
    }

    fn requires_raw_public_keys(&self) -> bool {
        true
    }
}

fn tls12_unexpected() -> rustls::Error {
    rustls::Error::InvalidCertificate(rustls::CertificateError::BadEncoding)
}

pub static SUPPORTED_SIG_ALGS: WebPkiSupportedAlgorithms = WebPkiSupportedAlgorithms {
    all: &[webpki::ring::ED25519],
    mapping: &[(rustls::SignatureScheme::ED25519, &[webpki::ring::ED25519])],
};

static SUPPORTED_VERIFY_SCHEMES: &[SignatureScheme] = &[SignatureScheme::ED25519];

#[derive(thiserror::Error, Debug)]
#[error("certificate peer id mismatch")]
struct CertificatePeerIdMismatch;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gen_cert_works() {
        let secret = rand::random::<tycho_crypto::ed25519::SecretKey>();
        let public = tycho_crypto::ed25519::PublicKey::from(&secret);

        let crypto = rustls::crypto::ring::default_provider();
        let cert = generate_cert(
            &ed25519::KeypairBytes {
                secret_key: secret.to_bytes(),
                public_key: None,
            },
            crypto.key_provider,
        )
        .unwrap();

        let peer_id = peer_id_from_certificate(cert.end_entity_cert().unwrap()).unwrap();
        assert_eq!(peer_id.as_bytes(), public.as_bytes());
    }
}
