use std::sync::Arc;

use anyhow::{Context, Result};

use crate::crypto::CertVerifier;
use crate::types::PeerId;

#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct Config {
    pub max_frame_size: Option<usize>,
}

pub struct EndpointConfig {
    pub peer_id: PeerId,
    pub server_name: String,
    pub client_cert: rustls::Certificate,
    pub pkcs8_der: rustls::PrivateKey,
    pub quinn_server_config: quinn::ServerConfig,
    pub quinn_client_config: quinn::ClientConfig,
    pub transport_config: Arc<quinn::TransportConfig>,
    pub quinn_endpoint_config: quinn::EndpointConfig,
}

impl EndpointConfig {
    pub fn builder<T: Into<String>>(name: T, private_key: [u8; 32]) -> EndpointConfigBuilder {
        EndpointConfigBuilder::new(name, private_key)
    }
}

pub struct EndpointConfigBuilder {
    server_name: String,
    private_key: [u8; 32],
    transport_config: Option<quinn::TransportConfig>,
}

impl EndpointConfigBuilder {
    pub fn new<T: Into<String>>(name: T, private_key: [u8; 32]) -> Self {
        Self {
            server_name: name.into(),
            private_key,
            transport_config: None,
        }
    }

    pub fn with_transport_config(mut self, transport_config: quinn::TransportConfig) -> Self {
        self.transport_config = Some(transport_config);
        self
    }

    pub fn build(self) -> Result<EndpointConfig> {
        let keypair = ed25519::KeypairBytes {
            secret_key: self.private_key,
            public_key: None,
        };

        let transport_config = Arc::new(self.transport_config.unwrap_or_default());

        let reset_key = compute_reset_key(&keypair.secret_key);
        let quinn_endpoint_config = quinn::EndpointConfig::new(Arc::new(reset_key));

        let (cert, pkcs8_der) = crate::crypto::generate_cert(&keypair, &self.server_name)
            .context("Failed to generate a certificate")?;

        let cert_verifier = Arc::new(CertVerifier::from(self.server_name.clone()));
        let quinn_client_config = make_client_config(
            cert.clone(),
            pkcs8_der.clone(),
            cert_verifier.clone(),
            transport_config.clone(),
        )?;

        let quinn_server_config = make_server_config(
            &self.server_name,
            pkcs8_der.clone(),
            cert.clone(),
            cert_verifier,
            transport_config.clone(),
        )?;

        let peer_id = crate::crypto::peer_id_from_certificate(&cert)?;

        Ok(EndpointConfig {
            peer_id,
            server_name: self.server_name,
            client_cert: cert,
            pkcs8_der,
            quinn_server_config,
            quinn_client_config,
            transport_config,
            quinn_endpoint_config,
        })
    }
}

fn make_client_config(
    cert: rustls::Certificate,
    pkcs8_der: rustls::PrivateKey,
    cert_verifier: Arc<CertVerifier>,
    transport_config: Arc<quinn::TransportConfig>,
) -> Result<quinn::ClientConfig> {
    let client_config = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(cert_verifier)
        .with_client_auth_cert(vec![cert], pkcs8_der)?;

    let mut client = quinn::ClientConfig::new(Arc::new(client_config));
    client.transport_config(transport_config);
    Ok(client)
}

fn make_server_config(
    server_name: &str,
    pkcs8_der: rustls::PrivateKey,
    cert: rustls::Certificate,
    cert_verifier: Arc<CertVerifier>,
    transport_config: Arc<quinn::TransportConfig>,
) -> Result<quinn::ServerConfig> {
    let mut server_cert_resolver = rustls::server::ResolvesServerCertUsingSni::new();

    let key = rustls::sign::any_eddsa_type(&pkcs8_der).context("Invalid private key")?;
    let certified_key = rustls::sign::CertifiedKey::new(vec![cert], key);
    server_cert_resolver.add(server_name, certified_key)?;

    let server_crypto = rustls::ServerConfig::builder()
        .with_safe_defaults()
        .with_client_cert_verifier(cert_verifier)
        .with_cert_resolver(Arc::new(server_cert_resolver));

    let mut server = quinn::ServerConfig::with_crypto(Arc::new(server_crypto));
    server.transport = transport_config;
    Ok(server)
}

fn compute_reset_key(private_key: &[u8; 32]) -> ring::hmac::Key {
    const STATELESS_RESET_SALT: &[u8] = b"tycho-stateless-reset";

    let salt = ring::hkdf::Salt::new(ring::hkdf::HKDF_SHA256, STATELESS_RESET_SALT);
    let private_key = salt.extract(private_key);
    let okm = private_key.expand(&[], ring::hmac::HMAC_SHA256).unwrap();

    let mut reset_key = [0; 32];
    okm.fill(&mut reset_key).unwrap();

    ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &reset_key)
}
