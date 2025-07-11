use std::path::PathBuf;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::WuEvent;
use crate::config::WuTunerConfig;
use crate::tuner::WuTuner;

pub struct WuTunerServiceBuilder {
    config: Arc<WuTunerConfig>,
    config_path: Option<PathBuf>,
}

impl WuTunerServiceBuilder {
    pub fn with_config(config: Arc<WuTunerConfig>) -> Self {
        Self {
            config,
            config_path: None,
        }
    }

    pub fn with_config_path(path: PathBuf) -> Self {
        let config = WuTunerConfig::from_file(&path).unwrap_or_default();
        Self {
            config: Arc::new(config),
            config_path: Some(path),
        }
    }

    pub fn build(self) -> WuTunerService {
        let (config_sender, mut config_receiver) = tokio::sync::watch::channel(self.config.clone());
        let _ = *config_receiver.borrow_and_update();

        let (event_sender, event_receiver) = tokio::sync::mpsc::channel(1000);

        WuTunerService {
            config_sender,
            config_receiver,
            event_sender,
            event_receiver,
            tuner: WuTuner::with_config(self.config),
            config_path: self.config_path,
        }
    }
}

pub struct WuTunerService {
    config_receiver: tokio::sync::watch::Receiver<Arc<WuTunerConfig>>,
    config_sender: tokio::sync::watch::Sender<Arc<WuTunerConfig>>,
    event_receiver: tokio::sync::mpsc::Receiver<WuEvent>,
    event_sender: tokio::sync::mpsc::Sender<WuEvent>,
    tuner: WuTuner,
    config_path: Option<PathBuf>,
}

impl WuTunerService {
    pub fn start(self) -> RunningWuTunerService {
        let event_sender = self.event_sender.clone();
        let cancel_token = CancellationToken::new();

        // spawn config watch
        if let Some(path) = &self.config_path {
            tokio::spawn(WuTunerConfig::watch_changes(
                path.clone(),
                self.config_sender.clone(),
            ));
        }

        // spawn wu tuner
        tokio::spawn({
            let cancel_token = cancel_token.clone();
            async move {
                tracing::info!("WuTuner service started");
                let mut service = self;
                let _config_sender = service.config_sender.clone();
                loop {
                    tokio::select! {
                        event = service.event_receiver.recv() => match event {
                            Some(event) => {
                                if let Err(err) = service.tuner.handle_wu_event(event).await {
                                    tracing::error!(?err, "Error handling wu event");
                                    break;
                                }
                            },
                            None => {
                                tracing::error!("Wu events channel closed");
                                break;
                            }
                        },
                        config_changed = service.config_receiver.changed() => match config_changed {
                            Ok(_) => {
                                let config = service.config_receiver.borrow_and_update();
                                service.tuner.update_config(config.clone());
                                tracing::info!(?config, "WuTuner config updated");
                            },
                            Err(err) => {
                                tracing::warn!(%err, "Error receive WuTuner config update");
                                break;
                            }
                        } ,
                        _ = cancel_token.cancelled() => {
                            break;
                        }
                    }
                }
                tracing::info!("WuTuner service stopped");
            }
        });

        RunningWuTunerService {
            event_sender,
            cancel_token,
        }
    }
}

pub struct RunningWuTunerService {
    pub event_sender: tokio::sync::mpsc::Sender<WuEvent>,
    pub cancel_token: CancellationToken,
}

impl Drop for RunningWuTunerService {
    fn drop(&mut self) {
        self.cancel_token.cancel();
    }
}
