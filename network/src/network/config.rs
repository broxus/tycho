use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tycho_util::serde_helpers;

use crate::network::crypto::{
    generate_cert, peer_id_from_certificate, CertVerifier, CertVerifierWithPeerId,
};
use crate::types::PeerId;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct NetworkConfig {
    pub quic: Option<QuicConfig>,

    /// Default: 128.
    pub connection_manager_channel_capacity: usize,

    /// Default: 5 seconds.
    #[serde(with = "serde_helpers::humantime")]
    pub connectivity_check_interval: Duration,

    /// Default: yes.
    pub max_frame_size: Option<usize>,

    /// Default: 10 seconds.
    #[serde(with = "serde_helpers::humantime")]
    pub connect_timeout: Duration,

    /// Default: 10 seconds.
    #[serde(with = "serde_helpers::humantime")]
    pub connection_backoff: Duration,

    /// Default: 1 minute.
    #[serde(with = "serde_helpers::humantime")]
    pub max_connection_backoff: Duration,

    /// Default: 100.
    pub max_concurrent_outstanding_connections: usize,

    /// Default: unlimited.
    pub max_concurrent_connections: Option<usize>,

    /// Default: 128.
    pub active_peers_event_channel_capacity: usize,

    /// Default: 1 minute.
    #[serde(with = "serde_helpers::humantime")]
    pub shutdown_idle_timeout: Duration,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            quic: None,
            connection_manager_channel_capacity: 128,
            connectivity_check_interval: Duration::from_millis(5000),
            max_frame_size: None,
            connect_timeout: Duration::from_secs(10),
            connection_backoff: Duration::from_secs(10),
            max_connection_backoff: Duration::from_secs(60),
            max_concurrent_outstanding_connections: 100,
            max_concurrent_connections: None,
            active_peers_event_channel_capacity: 128,
            shutdown_idle_timeout: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct QuicConfig {
    /// Default: 100.
    pub max_concurrent_bidi_streams: u64,
    /// Default: 100.
    pub max_concurrent_uni_streams: u64,
    /// Default: auto.
    pub stream_receive_window: Option<u64>,
    /// Default: auto.
    pub receive_window: Option<u64>,
    /// Default: auto.
    pub send_window: Option<u64>,

    // TODO: add all other fields from quin::TransportConfig
    /// Default: auto.
    pub socket_send_buffer_size: Option<usize>,
    /// Default: auto.
    pub socket_recv_buffer_size: Option<usize>,
}

impl Default for QuicConfig {
    fn default() -> Self {
        Self {
            max_concurrent_bidi_streams: 100,
            max_concurrent_uni_streams: 100,
            stream_receive_window: None,
            receive_window: None,
            send_window: None,
            socket_send_buffer_size: None,
            socket_recv_buffer_size: None,
        }
    }
}

impl QuicConfig {
    pub fn make_transport_config(&self) -> quinn::TransportConfig {
        fn make_varint(value: u64) -> quinn::VarInt {
            quinn::VarInt::from_u64(value).unwrap_or(quinn::VarInt::MAX)
        }

        let mut config = quinn::TransportConfig::default();
        config.max_concurrent_bidi_streams(make_varint(self.max_concurrent_bidi_streams));
        config.max_concurrent_uni_streams(make_varint(self.max_concurrent_uni_streams));

        if let Some(stream_receive_window) = self.stream_receive_window {
            config.stream_receive_window(make_varint(stream_receive_window));
        }
        if let Some(receive_window) = self.receive_window {
            config.receive_window(make_varint(receive_window));
        }
        if let Some(send_window) = self.send_window {
            config.receive_window(make_varint(send_window));
        }

        config
    }
}

pub(crate) struct EndpointConfig {
    pub peer_id: PeerId,
    pub service_name: String,
    pub client_cert: rustls::Certificate,
    pub pkcs8_der: rustls::PrivateKey,
    pub quinn_server_config: quinn::ServerConfig,
    pub quinn_client_config: quinn::ClientConfig,
    pub transport_config: Arc<quinn::TransportConfig>,
    pub quinn_endpoint_config: quinn::EndpointConfig,
}

impl EndpointConfig {
    pub fn builder() -> EndpointConfigBuilder<((), ())> {
        EndpointConfigBuilder {
            mandatory_fields: ((), ()),
            optional_fields: Default::default(),
        }
    }

    pub fn make_client_config_for_peer_id(&self, peer_id: PeerId) -> Result<quinn::ClientConfig> {
        let client_config = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_custom_certificate_verifier(Arc::new(CertVerifierWithPeerId::new(
                self.service_name.clone(),
                peer_id,
            )))
            .with_client_auth_cert(vec![self.client_cert.clone()], self.pkcs8_der.clone())?;

        let mut client = quinn::ClientConfig::new(Arc::new(client_config));
        client.transport_config(self.transport_config.clone());
        Ok(client)
    }
}

pub(crate) struct EndpointConfigBuilder<MandatoryFields = (String, [u8; 32])> {
    mandatory_fields: MandatoryFields,
    optional_fields: EndpointConfigBuilderFields,
}

#[derive(Default)]
struct EndpointConfigBuilderFields {
    transport_config: Option<quinn::TransportConfig>,
}

impl<MandatoryFields> EndpointConfigBuilder<MandatoryFields> {
    pub fn with_transport_config(mut self, transport_config: quinn::TransportConfig) -> Self {
        self.optional_fields.transport_config = Some(transport_config);
        self
    }
}

impl<T2> EndpointConfigBuilder<((), T2)> {
    pub fn with_service_name<T: Into<String>>(
        self,
        service_name: T,
    ) -> EndpointConfigBuilder<(String, T2)> {
        let (_, private_key) = self.mandatory_fields;
        EndpointConfigBuilder {
            mandatory_fields: (service_name.into(), private_key),
            optional_fields: self.optional_fields,
        }
    }
}

impl<T1> EndpointConfigBuilder<(T1, ())> {
    pub fn with_private_key(self, private_key: [u8; 32]) -> EndpointConfigBuilder<(T1, [u8; 32])> {
        let (service_name, _) = self.mandatory_fields;
        EndpointConfigBuilder {
            mandatory_fields: (service_name, private_key),
            optional_fields: self.optional_fields,
        }
    }
}

impl EndpointConfigBuilder {
    pub fn build(self) -> Result<EndpointConfig> {
        let (service_name, private_key) = self.mandatory_fields;

        let keypair = ed25519::KeypairBytes {
            secret_key: private_key,
            public_key: None,
        };

        let transport_config = Arc::new(self.optional_fields.transport_config.unwrap_or_default());

        let reset_key = compute_reset_key(&keypair.secret_key);
        let quinn_endpoint_config = quinn::EndpointConfig::new(Arc::new(reset_key));

        let (cert, pkcs8_der) =
            generate_cert(&keypair, &service_name).context("Failed to generate a certificate")?;

        let cert_verifier = Arc::new(CertVerifier::from(service_name.clone()));
        let quinn_client_config = make_client_config(
            cert.clone(),
            pkcs8_der.clone(),
            cert_verifier.clone(),
            transport_config.clone(),
        )?;

        let quinn_server_config = make_server_config(
            &service_name,
            pkcs8_der.clone(),
            cert.clone(),
            cert_verifier,
            transport_config.clone(),
        )?;

        let peer_id = peer_id_from_certificate(&cert)?;

        Ok(EndpointConfig {
            peer_id,
            service_name,
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
    service_name: &str,
    pkcs8_der: rustls::PrivateKey,
    cert: rustls::Certificate,
    cert_verifier: Arc<CertVerifier>,
    transport_config: Arc<quinn::TransportConfig>,
) -> Result<quinn::ServerConfig> {
    let mut server_cert_resolver = rustls::server::ResolvesServerCertUsingSni::new();

    let key = rustls::sign::any_eddsa_type(&pkcs8_der).context("Invalid private key")?;
    let certified_key = rustls::sign::CertifiedKey::new(vec![cert], key);
    server_cert_resolver.add(service_name, certified_key)?;

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
