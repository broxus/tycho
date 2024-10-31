use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use rustls::crypto::CryptoProvider;
use rustls::sign::CertifiedKey;
use rustls::SupportedCipherSuite;
use serde::{Deserialize, Serialize};
use tycho_util::serde_helpers;

use crate::network::crypto::{
    generate_cert, peer_id_from_certificate, CertVerifier, CertVerifierWithPeerId,
    SUPPORTED_SIG_ALGS,
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

    /// Default: 8 MiB.
    pub max_frame_size: bytesize::ByteSize,

    /// Default: 10 seconds.
    #[serde(with = "serde_helpers::humantime")]
    pub connect_timeout: Duration,

    /// Default: 10 seconds.
    #[serde(with = "serde_helpers::humantime")]
    pub connection_backoff: Duration,

    /// Default: 1 minute.
    #[serde(with = "serde_helpers::humantime")]
    pub max_connection_backoff: Duration,

    /// Optimistic guess for some errors that there will be an incoming connection.
    ///
    /// Default: 3 seconds.
    #[serde(with = "serde_helpers::humantime")]
    pub connection_error_delay: Duration,

    /// Default: 100.
    pub max_concurrent_outstanding_connections: usize,

    /// Default: unlimited.
    pub max_concurrent_connections: Option<usize>,

    /// Default: 128.
    pub active_peers_event_channel_capacity: usize,

    /// Default: 1 minute.
    #[serde(with = "serde_helpers::humantime")]
    pub shutdown_idle_timeout: Duration,

    /// Default: no.
    pub enable_0rtt: bool,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            quic: None,
            connection_manager_channel_capacity: 128,
            connectivity_check_interval: Duration::from_millis(5000),
            max_frame_size: bytesize::ByteSize::mib(8),
            connect_timeout: Duration::from_secs(10),
            connection_backoff: Duration::from_secs(10),
            max_connection_backoff: Duration::from_secs(60),
            connection_error_delay: Duration::from_secs(3),
            max_concurrent_outstanding_connections: 100,
            max_concurrent_connections: None,
            active_peers_event_channel_capacity: 128,
            shutdown_idle_timeout: Duration::from_secs(60),
            enable_0rtt: false,
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
    /// Default: true.
    pub use_pmtu: bool,
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
            use_pmtu: true,
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
        if self.use_pmtu {
            let mtu = quinn::MtuDiscoveryConfig::default();
            config.mtu_discovery_config(Some(mtu));
        }

        config
    }
}

pub(crate) struct EndpointConfig {
    pub peer_id: PeerId,
    pub cert_resolver: Arc<rustls::client::AlwaysResolvesClientRawPublicKeys>,
    pub quinn_server_config: quinn::ServerConfig,
    pub transport_config: Arc<quinn::TransportConfig>,
    pub quinn_endpoint_config: quinn::EndpointConfig,
    pub enable_early_data: bool,
    pub crypto_provider: Arc<CryptoProvider>,
}

impl EndpointConfig {
    pub fn builder() -> EndpointConfigBuilder<((),)> {
        EndpointConfigBuilder {
            mandatory_fields: ((),),
            optional_fields: Default::default(),
        }
    }

    pub fn make_client_config_for_peer_id(&self, peer_id: &PeerId) -> Result<quinn::ClientConfig> {
        let mut client_config =
            rustls::ClientConfig::builder_with_provider(self.crypto_provider.clone())
                .with_protocol_versions(DEFAULT_PROTOCOL_VERSIONS)
                .unwrap()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(CertVerifierWithPeerId::new(peer_id)))
                .with_client_cert_resolver(self.cert_resolver.clone());

        client_config.enable_early_data = self.enable_early_data;
        let quinn_config = QuicClientConfig::try_from(client_config)?;

        let mut client = quinn::ClientConfig::new(Arc::new(quinn_config));
        client.transport_config(self.transport_config.clone());
        Ok(client)
    }
}

pub(crate) struct EndpointConfigBuilder<MandatoryFields = ([u8; 32],)> {
    mandatory_fields: MandatoryFields,
    optional_fields: EndpointConfigBuilderFields,
}

#[derive(Default)]
struct EndpointConfigBuilderFields {
    enable_0rtt: bool,
    transport_config: Option<quinn::TransportConfig>,
}

impl<MandatoryFields> EndpointConfigBuilder<MandatoryFields> {
    pub fn with_0rtt_enabled(mut self, enable_0rtt: bool) -> Self {
        self.optional_fields.enable_0rtt = enable_0rtt;
        self
    }

    pub fn with_transport_config(mut self, transport_config: quinn::TransportConfig) -> Self {
        self.optional_fields.transport_config = Some(transport_config);
        self
    }
}

impl EndpointConfigBuilder<((),)> {
    pub fn with_private_key(self, private_key: [u8; 32]) -> EndpointConfigBuilder<([u8; 32],)> {
        EndpointConfigBuilder {
            mandatory_fields: (private_key,),
            optional_fields: self.optional_fields,
        }
    }
}

impl EndpointConfigBuilder {
    pub fn build(self) -> Result<EndpointConfig> {
        let (private_key,) = self.mandatory_fields;

        let keypair = ed25519::KeypairBytes {
            secret_key: private_key,
            public_key: None,
        };

        let transport_config = Arc::new(self.optional_fields.transport_config.unwrap_or_default());

        let reset_key = compute_reset_key(&keypair.secret_key);
        let quinn_endpoint_config = quinn::EndpointConfig::new(reset_key);

        let crypto_provider = Arc::new(CryptoProvider {
            cipher_suites: DEFAULT_CIPHER_SUITES.to_vec(),
            kx_groups: DEFAULT_KX_GROUPS.to_vec(),
            signature_verification_algorithms: SUPPORTED_SIG_ALGS,
            ..rustls::crypto::ring::default_provider()
        });

        let certified_key = generate_cert(&keypair, crypto_provider.key_provider)
            .context("Failed to generate a certificate")?;

        let cert_resolver = Arc::new(rustls::client::AlwaysResolvesClientRawPublicKeys::new(
            certified_key.clone(),
        ));
        let cert_verifier = Arc::new(CertVerifier);

        let quinn_server_config = make_server_config(
            certified_key.clone(),
            cert_verifier,
            transport_config.clone(),
            crypto_provider.clone(),
            self.optional_fields.enable_0rtt,
        )?;

        let peer_id = peer_id_from_certificate(certified_key.end_entity_cert()?)?;

        Ok(EndpointConfig {
            peer_id,
            cert_resolver,
            quinn_server_config,
            transport_config,
            quinn_endpoint_config,
            enable_early_data: self.optional_fields.enable_0rtt,
            crypto_provider,
        })
    }
}

fn make_server_config(
    certified_key: Arc<CertifiedKey>,
    cert_verifier: Arc<CertVerifier>,
    transport_config: Arc<quinn::TransportConfig>,
    crypto_provider: Arc<CryptoProvider>,
    enable_0rtt: bool,
) -> Result<quinn::ServerConfig> {
    let server_cert_resolver =
        rustls::server::AlwaysResolvesServerRawPublicKeys::new(certified_key);

    let mut server_crypto = rustls::ServerConfig::builder_with_provider(crypto_provider.clone())
        .with_protocol_versions(DEFAULT_PROTOCOL_VERSIONS)
        .unwrap()
        .with_client_cert_verifier(cert_verifier)
        .with_cert_resolver(Arc::new(server_cert_resolver));

    if enable_0rtt {
        server_crypto.max_early_data_size = u32::MAX;

        // TODO: Should we enable this?
        // server_crypto.send_half_rtt_data = true;
    }
    let server_config = QuicServerConfig::try_from(server_crypto)?;

    let mut server = quinn::ServerConfig::with_crypto(Arc::new(server_config));
    server.transport = transport_config;
    Ok(server)
}

fn compute_reset_key(private_key: &[u8; 32]) -> Arc<ring::hmac::Key> {
    const STATELESS_RESET_SALT: &[u8] = b"tycho-stateless-reset";

    let salt = ring::hkdf::Salt::new(ring::hkdf::HKDF_SHA256, STATELESS_RESET_SALT);
    let private_key = salt.extract(private_key);
    let okm = private_key.expand(&[], ring::hmac::HMAC_SHA256).unwrap();

    let mut reset_key = [0; 32];
    okm.fill(&mut reset_key).unwrap();

    Arc::new(ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &reset_key))
}

static DEFAULT_CIPHER_SUITES: &[SupportedCipherSuite] = &[
    // TLS1.3 suites
    rustls::crypto::ring::cipher_suite::TLS13_AES_256_GCM_SHA384,
    rustls::crypto::ring::cipher_suite::TLS13_AES_128_GCM_SHA256,
    rustls::crypto::ring::cipher_suite::TLS13_CHACHA20_POLY1305_SHA256,
];

static DEFAULT_KX_GROUPS: &[&dyn rustls::crypto::SupportedKxGroup] =
    &[rustls::crypto::ring::kx_group::X25519];

static DEFAULT_PROTOCOL_VERSIONS: &[&rustls::SupportedProtocolVersion] = &[&rustls::version::TLS13];
