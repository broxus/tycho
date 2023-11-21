use std::sync::Arc;

use anyhow::Result;
use pkcs8::EncodePrivateKey;

pub fn generate_cert(
    keypair: &ed25519_dalek::ed25519::KeypairBytes,
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

/// Verifies self-signed certificates for the specified SNI.
pub struct CertVerifier<'a>(pub webpki::SubjectNameRef<'a>);

impl CertVerifier<'static> {
    /// Creates a verifier from a SNI string (IP or DNS).
    pub fn from_static_str(name: &'static str) -> Result<Self, webpki::InvalidSubjectNameError> {
        webpki::SubjectNameRef::try_from_ascii_str(name).map(CertVerifier)
    }
}

impl rustls::server::ClientCertVerifier for CertVerifier<'_> {
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

        // Verify subject name in the certificate
        prepared
            .parsed
            .verify_is_valid_for_subject_name(self.0)
            .map_err(map_pki_error)
            .map(|_| rustls::server::ClientCertVerified::assertion())
    }
}

impl rustls::client::ServerCertVerifier for CertVerifier<'_> {
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
                if let Ok(name) = webpki::SubjectNameRef::try_from_ascii_str(name.as_ref()) {
                    if name.as_ref() == self.0.as_ref() {
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
            .verify_is_valid_for_subject_name(subject_name)
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
            InvalidCertificateOtherError(e),
        ))),
    }
}

#[derive(thiserror::Error, Debug)]
#[error("invalid peer certificate: {0}")]
struct InvalidCertificateOtherError(webpki::Error);

static SIGNATURE_ALGHORITHMS: &[&webpki::SignatureAlgorithm] = &[&webpki::ED25519];
