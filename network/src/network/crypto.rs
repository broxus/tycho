use std::fmt::{Debug};
use std::sync::Arc;

use anyhow::Result;
use pkcs8::EncodePrivateKey;
use rustls::{DigitallySignedStruct, DistinguishedName, Error, OtherError, SignatureScheme};
use rustls::client::danger::HandshakeSignatureValid;
use rustls::crypto::{verify_tls13_signature, WebPkiSupportedAlgorithms};
use rustls::pki_types::{CertificateDer, SignatureVerificationAlgorithm};
use webpki::ring::ED25519;

use crate::types::PeerId;

pub(crate) fn generate_cert(
    keypair: &ed25519::KeypairBytes,
    subject_name: &str,
) -> Result<(rustls::pki_types::CertificateDer<'static>, rustls::pki_types::PrivatePkcs8KeyDer<'static>)> {
    static ALGO: &rcgen::SignatureAlgorithm = &rcgen::PKCS_ED25519;

    // TODO: use zeroize for `rustls::PrivateKey` contents?

    let key_der = rustls::pki_types::PrivatePkcs8KeyDer::from(keypair.to_pkcs8_der()?.as_bytes());
    let key_pair = rcgen::KeyPair::from_pkcs8_der_and_sign_algo(&key_der, ALGO)?;

    let mut cert_params = rcgen::CertificateParams::new([subject_name.to_owned()])?;
    cert_params.distinguished_name = rcgen::DistinguishedName::new();

    let cert = cert_params.self_signed(&key_pair)?.der();

    //cert_params.key_pair = Some(key_pair);
    //cert_params.alg = ALGO;


    //let cert = rcgen::Certificate:: (cert_params)?.serialize_der()?;

    Ok((rustls::pki_types::CertificateDer::<'static>::from(cert.as_ref()), key_der))
}

pub(crate) fn peer_id_from_certificate(
    certificate: &CertificateDer<'static>,
) -> Result<PeerId, rustls::Error> {
    use pkcs8::DecodePublicKey;
    use x509_parser::prelude::{FromDer, X509Certificate};

    let (_, cert) = X509Certificate::from_der(certificate)
        .map_err(|_e| rustls::Error::InvalidCertificate(rustls::CertificateError::BadEncoding))?;
    let spki = cert.public_key();
    let public_key =
        ed25519::pkcs8::PublicKeyBytes::from_public_key_der(spki.raw).map_err(|e| {
            rustls::Error::InvalidCertificate(rustls::CertificateError::Other(OtherError(Arc::new(
                InvalidCertificatePublicKey(e),
            ))))
        })?;

    Ok(PeerId(public_key.to_bytes()))
}

#[derive(Debug)]
pub(crate) struct CertVerifierWithPeerId {
    inner: CertVerifier,
    peer_id: PeerId,
}

impl CertVerifierWithPeerId {
    pub fn new(service_name: String, peer_id: &PeerId) -> Self {
        Self {
            inner: CertVerifier::from(service_name),
            peer_id: *peer_id,
        }
    }
}

impl rustls::client::danger::ServerCertVerifier for CertVerifierWithPeerId {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        intermediates: &[rustls::pki_types::CertificateDer<'_>],
        server_name: & rustls::pki_types::ServerName<'_>,
        ocsp_response: &[u8],
        now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        let peer_id = peer_id_from_certificate(end_entity)?;
        if peer_id != self.peer_id {
            return Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::Other(OtherError(Arc::new(CertificatePeerIdMismatch))),
            ));
        }

        self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        )
    }

    fn verify_tls12_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> std::result::Result<HandshakeSignatureValid, Error> {
        todo!()
    }

    fn verify_tls13_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> std::result::Result<HandshakeSignatureValid, Error> {
        verify_tls13_signature(message, cert, dss, &WebPkiSupportedAlgorithms {
            all: &[ED25519],
            mapping: &[(SignatureScheme::ED25519, &[ED25519])],
        })
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![SignatureScheme::ED25519]
    }
}

/// Verifies self-signed certificates for the specified SNI.
#[derive(Debug)]
pub(crate) struct CertVerifier {
    service_name: String,
}

impl From<String> for CertVerifier {
    fn from(service_name: String) -> Self {
        Self { service_name }
    }
}

impl rustls::server::danger::ClientCertVerifier for CertVerifier {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }


    fn verify_client_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        intermediates: &[rustls::pki_types::CertificateDer<'_>],
        now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::server::danger::ClientCertVerified, rustls::Error> {
        // Parse the certificate
        let prepared = prepare_for_self_signed(end_entity, intermediates)?;
        let now =
            webpki::types::UnixTime::try_from(now).map_err(|_e| rustls::Error::FailedToGetCurrentTime)?;

        // Verify the certificate
        prepared
            .parsed
            .verify_for_usage(
                SIGNATURE_ALGORITHMS,
                std::slice::from_ref(&prepared.root),
                &prepared.intermediates.iter().map(|x| CertificateDer::from(*x)).collect::<Vec<_>>(),
                now,
                webpki::KeyUsage::client_auth(),
                None,
                None
            )
            .map_err(map_pki_error)?;

        let Ok(subject_name) = webpki::types::DnsName::try_from(self.service_name.as_str()) else {
            return Err(rustls::Error::UnsupportedNameType);
        };

        // Verify subject name in the certificate
        prepared
            .parsed
            .verify_is_valid_for_subject_name(&rustls::pki_types::ServerName::DnsName(subject_name))
            .map_err(map_pki_error)
            .map(|_| rustls::server::danger::ClientCertVerified::assertion())
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_tls12_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> std::result::Result<HandshakeSignatureValid, Error> {
        todo!()
    }

    fn verify_tls13_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> std::result::Result<HandshakeSignatureValid, Error> {
        verify_tls13_signature(message, cert, dss, &WebPkiSupportedAlgorithms {
            all: &[ED25519],
            mapping: &[(SignatureScheme::ED25519, &[ED25519])],
        })
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![SignatureScheme::ED25519]
    }
}


impl rustls::client::danger::ServerCertVerifier for CertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        intermediates: &[rustls::pki_types::CertificateDer<'_>],
        server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        // Filter subject name before verifying the certificate
        let subject_name = 'name: {
            if let  rustls::pki_types::ServerName::DnsName(name) = server_name {
                if let (Ok(name), Ok(target)) = (
                    rustls::pki_types::DnsName::try_from(name.as_ref()),
                    rustls::pki_types::DnsName::try_from(self.service_name.as_str()),
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
        let now =
            webpki::types::UnixTime::try_from(now).map_err(|_e| rustls::Error::FailedToGetCurrentTime)?;

        // Verify the certificate
        prepared
            .parsed
            .verify_for_usage(
                SIGNATURE_ALGORITHMS,
                std::slice::from_ref(&prepared.root),
                &prepared.intermediates.iter().map(|x| CertificateDer::from(*x)).collect::<Vec<_>>(),
                now,
                webpki::KeyUsage::server_auth(),
                None,
                None,
            )
            .map_err(map_pki_error)?;

        // Verify subject name in the certificate
        prepared
            .parsed
            .verify_is_valid_for_subject_name(&rustls::pki_types::ServerName::DnsName(subject_name))
            .map_err(map_pki_error)
            .map(|_| rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> std::result::Result<HandshakeSignatureValid, Error> {
        unimplemented!()
    }

    fn verify_tls13_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> std::result::Result<HandshakeSignatureValid, Error> {
        verify_tls13_signature(message, cert, dss, &WebPkiSupportedAlgorithms {
            all: &[ED25519],
            mapping: &[(SignatureScheme::ED25519, &[ED25519])],
        })
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![SignatureScheme::ED25519]
    }
}

struct PreparedCert<'a> {
    parsed: webpki::EndEntityCert<'a>,
    intermediates: Vec<&'a [u8]>,
    root: rustls::pki_types::TrustAnchor<'a>,
}

// This prepares arguments for webpki, including a trust anchor which is the end entity of the certificate
// (which embodies a self-signed certificate by definition)
fn prepare_for_self_signed<'a>(
    end_entity: &'a rustls::pki_types::CertificateDer<'_>,
    intermediates: &'a [rustls::pki_types::CertificateDer<'_>],
) -> Result<PreparedCert<'a>, rustls::Error> {
    // EE cert must appear first.
    let parsed = webpki::EndEntityCert::try_from(end_entity).map_err(|x| map_pki_error(x))?;
    let intermediates: Vec<&'a [u8]> = intermediates.iter().map(|cert| cert.as_ref()).collect();

    // Reinterpret the certificate as a root
    //
    // TODO: webpki::EndEntityCert and webpki::TrustAnchor do the same job of parsing the same input.
    // Find a way to reuse an inner `webpki::Cert`
    let root =
        webpki::anchor_from_trusted_cert(end_entity).map_err(map_pki_error)?; // TODO: ???

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
        e => rustls::Error::InvalidCertificate(rustls::CertificateError::Other(OtherError(Arc::new(
            WebpkiCertificateError(e),
        )))),
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

static SIGNATURE_ALGORITHMS: &[&dyn SignatureVerificationAlgorithm] = &[ED25519];
