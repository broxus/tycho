use std::sync::Arc;

use anyhow::Result;
use pkcs8::EncodePrivateKey;

use crate::types::PeerId;

pub fn generate_cert(
    keypair: &ed25519::KeypairBytes,
    subject_name: &str,
) -> Result<(rustls::Certificate, rustls::PrivateKey)> {
    static ALGO: &rcgen::SignatureAlgorithm = &rcgen::PKCS_ED25519;

    // TODO: use zeroize for `rustls::PrivateKey` contents?
    let key_der = rustls::PrivateKey(keypair.to_pkcs8_der()?.as_bytes().to_vec());
    let key_pair = rcgen::KeyPair::from_der_and_sign_algo(key_der.0.as_ref(), ALGO)?;

    let mut cert_params = rcgen::CertificateParams::new([subject_name.to_owned()]);
    cert_params.key_pair = Some(key_pair);
    cert_params.distinguished_name = rcgen::DistinguishedName::new();
    cert_params.alg = ALGO;

    let cert = rcgen::Certificate::from_params(cert_params)?.serialize_der()?;

    Ok((rustls::Certificate(cert), key_der))
}

pub fn peer_id_from_certificate(
    certificate: &rustls::Certificate,
) -> Result<PeerId, rustls::Error> {
    use pkcs8::DecodePublicKey;
    use x509_parser::prelude::{FromDer, X509Certificate};

    let (_, cert) = X509Certificate::from_der(certificate.0.as_ref())
        .map_err(|_| rustls::Error::InvalidCertificate(rustls::CertificateError::BadEncoding))?;
    let spki = cert.public_key();
    let public_key =
        ed25519::pkcs8::PublicKeyBytes::from_public_key_der(spki.raw).map_err(|e| {
            rustls::Error::InvalidCertificate(rustls::CertificateError::Other(Arc::new(
                InvalidCertificatePublicKey(e),
            )))
        })?;

    Ok(PeerId(public_key.to_bytes()))
}

pub struct CertVerifierWithPeerId {
    inner: CertVerifier,
    peer_id: PeerId,
}

impl CertVerifierWithPeerId {
    pub fn new(service_name: String, peer_id: PeerId) -> Self {
        Self {
            inner: CertVerifier::from(service_name),
            peer_id,
        }
    }
}

impl rustls::client::ServerCertVerifier for CertVerifierWithPeerId {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::Certificate,
        intermediates: &[rustls::Certificate],
        server_name: &rustls::ServerName,
        scts: &mut dyn Iterator<Item = &[u8]>,
        ocsp_response: &[u8],
        now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        let peer_id = peer_id_from_certificate(end_entity)?;
        if peer_id != self.peer_id {
            return Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::Other(Arc::new(CertificatePeerIdMismatch)),
            ));
        }

        self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            scts,
            ocsp_response,
            now,
        )
    }
}

/// Verifies self-signed certificates for the specified SNI.
pub struct CertVerifier {
    service_name: String,
}

impl From<String> for CertVerifier {
    fn from(service_name: String) -> Self {
        Self { service_name }
    }
}

impl rustls::server::ClientCertVerifier for CertVerifier {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }

    fn client_auth_root_subjects(&self) -> &[rustls::DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &rustls::Certificate,
        intermediates: &[rustls::Certificate],
        now: std::time::SystemTime,
    ) -> Result<rustls::server::ClientCertVerified, rustls::Error> {
        // Parse the certificate
        let prepared = prepare_for_self_signed(end_entity, intermediates)?;
        let now = webpki::Time::try_from(now).map_err(|_| rustls::Error::FailedToGetCurrentTime)?;

        // Verify the certificate
        prepared
            .parsed
            .verify_for_usage(
                SIGNATURE_ALGHORITHMS,
                std::slice::from_ref(&prepared.root),
                &prepared.intermediates,
                now,
                webpki::KeyUsage::client_auth(),
                &[],
            )
            .map_err(map_pki_error)?;

        let Ok(subject_name) = webpki::DnsNameRef::try_from_ascii_str(&self.service_name) else {
            return Err(rustls::Error::UnsupportedNameType);
        };

        // Verify subject name in the certificate
        prepared
            .parsed
            .verify_is_valid_for_subject_name(webpki::SubjectNameRef::DnsName(subject_name))
            .map_err(map_pki_error)
            .map(|_| rustls::server::ClientCertVerified::assertion())
    }
}

impl rustls::client::ServerCertVerifier for CertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::Certificate,
        intermediates: &[rustls::Certificate],
        server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        // Filter subject name before verifying the certificate
        let subject_name = 'name: {
            if let rustls::ServerName::DnsName(name) = server_name {
                if let (Ok(name), Ok(target)) = (
                    webpki::DnsNameRef::try_from_ascii_str(name.as_ref()),
                    webpki::DnsNameRef::try_from_ascii_str(&self.service_name),
                ) {
                    if name.as_ref() == target.as_ref() {
                        break 'name name;
                    }
                }
            }
            return Err(rustls::Error::UnsupportedNameType);
        };

        // Parse the certificate
        let prepared = prepare_for_self_signed(end_entity, intermediates)?;
        let now = webpki::Time::try_from(now).map_err(|_| rustls::Error::FailedToGetCurrentTime)?;

        // Verify the certificate
        prepared
            .parsed
            .verify_for_usage(
                SIGNATURE_ALGHORITHMS,
                std::slice::from_ref(&prepared.root),
                &prepared.intermediates,
                now,
                webpki::KeyUsage::server_auth(),
                &[],
            )
            .map_err(map_pki_error)?;

        // Verify subject name in the certificate
        prepared
            .parsed
            .verify_is_valid_for_subject_name(webpki::SubjectNameRef::DnsName(subject_name))
            .map_err(map_pki_error)
            .map(|_| rustls::client::ServerCertVerified::assertion())
    }
}

struct PreparedCert<'a> {
    parsed: webpki::EndEntityCert<'a>,
    intermediates: Vec<&'a [u8]>,
    root: webpki::TrustAnchor<'a>,
}

// This prepares arguments for webpki, including a trust anchor which is the end entity of the certificate
// (which embodies a self-signed certificate by definition)
fn prepare_for_self_signed<'a>(
    end_entity: &'a rustls::Certificate,
    intermediates: &'a [rustls::Certificate],
) -> Result<PreparedCert<'a>, rustls::Error> {
    // EE cert must appear first.
    let parsed = webpki::EndEntityCert::try_from(end_entity.0.as_ref()).map_err(map_pki_error)?;
    let intermediates: Vec<&'a [u8]> = intermediates.iter().map(|cert| cert.0.as_ref()).collect();

    // Reinterpret the certificate as a root
    //
    // TODO: webpki::EndEntityCert and webpki::TrustAnchor do the same job of parsing the same input.
    // Find a way to reuse an inner `webpki::Cert`
    let root =
        webpki::TrustAnchor::try_from_cert_der(end_entity.0.as_ref()).map_err(map_pki_error)?;

    Ok(PreparedCert {
        parsed,
        intermediates,
        root,
    })
}

fn map_pki_error(error: webpki::Error) -> rustls::Error {
    match error {
        webpki::Error::BadDer | webpki::Error::BadDerTime => {
            rustls::Error::InvalidCertificate(rustls::CertificateError::BadEncoding)
        }
        webpki::Error::InvalidSignatureForPublicKey
        | webpki::Error::UnsupportedSignatureAlgorithm
        | webpki::Error::UnsupportedSignatureAlgorithmForPublicKey => {
            rustls::Error::InvalidCertificate(rustls::CertificateError::BadSignature)
        }
        e => rustls::Error::InvalidCertificate(rustls::CertificateError::Other(Arc::new(
            WebpkiCertificateError(e),
        ))),
    }
}

#[derive(thiserror::Error, Debug)]
#[error("invalid peer certificate: {0}")]
struct WebpkiCertificateError(webpki::Error);

#[derive(thiserror::Error, Debug)]
#[error("invalid ed25519 public key: {0}")]
struct InvalidCertificatePublicKey(pkcs8::spki::Error);

#[derive(thiserror::Error, Debug)]
#[error("certificate peer id mismatch")]
struct CertificatePeerIdMismatch;

static SIGNATURE_ALGHORITHMS: &[&webpki::SignatureAlgorithm] = &[&webpki::ED25519];
