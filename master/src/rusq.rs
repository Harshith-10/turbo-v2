use futures::stream::StreamExt;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tracing::{error, info, warn};

/// Commands sent to the background manager
enum ManagerCommand {
    Subscribe(String),
    Unsubscribe(String),
}

/// A Redis-backed Pub/Sub engine
///
/// It uses a single Redis connection to listen for all topics (multiplexing)
/// and dispatches messages to local broadcast channels.
#[derive(Clone)]
pub struct BroadcastEngine<T> {
    /// Connection for publishing messages (multiplexed, cloneable)
    /// Channel to send publish commands ensuring order
    pub_tx: mpsc::UnboundedSender<(String, T)>,
    /// Channel to send commands to the background manager
    cmd_tx: mpsc::UnboundedSender<ManagerCommand>,
    /// Local broadcast channels map: Topic -> Sender
    local_channels: Arc<dashmap::DashMap<String, broadcast::Sender<T>>>,
    /// Capacity for local broadcast channels
    capacity: usize,
}

impl<T> BroadcastEngine<T>
where
    T: Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static + std::fmt::Debug,
{
    pub async fn new(redis_url: &str, capacity: usize) -> Self {
        let client = redis::Client::open(redis_url).expect("Invalid Redis URL");
        
        // Connection for publishing
        let mut publisher_conn = client.get_multiplexed_async_connection().await
            .expect("Failed to get redis publish connection");

        // Channel for ordered publishing
        let (pub_tx, mut pub_rx) = mpsc::unbounded_channel::<(String, T)>();
        
        // Spawn publisher task
        tokio::spawn(async move {
            info!("Starting Redis Publisher Task");
            while let Some((topic, message)) = pub_rx.recv().await {
                 let payload = serde_json::to_string(&message).unwrap_or_default();
                 if let Err(e) = publisher_conn.publish::<_, _, ()>(topic, payload).await {
                     error!("Redis publish failed: {}", e);
                 }
            }
        });

        // Channel for manager commands
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();
        
        // Shared map of local channels
        let local_channels: Arc<dashmap::DashMap<String, broadcast::Sender<T>>> = Arc::new(dashmap::DashMap::new());
        let local_channels_clone = local_channels.clone();

        let client_clone = client.clone();
        
        // Spawn background manager
        tokio::spawn(async move {
            info!("Starting Redis Pub/Sub Manager");
            
            // Dedicated connection for subscriptions
            // We use a loop to handle reconnections if needed
            loop {
                // Remove publisher cloning/connection here since it's handled in separate task
                // We only need subscriber connection
                let mut pubsub_conn = match client_clone.get_async_connection().await {
                    Ok(conn) => conn.into_pubsub(),
                    Err(e) => {
                        error!("Manager failed to connect to Redis: {}. Retrying in 5s...", e);
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        continue;
                    }
                };
                
                info!("Manager connected to Redis Pub/Sub");
                
                enum Action {
                    Continue,
                    RunCmd(ManagerCommand),
                    Reconnect,
                    Quit,
                }
                
                // PSubscribe to all jobs
                if let Err(e) = pubsub_conn.psubscribe("job:*").await {
                    error!("Manager failed to psubscribe to job:*: {}", e);
                }

                let mut stream = pubsub_conn.on_message();
                
                loop {
                    
                    let action = tokio::select! {
                        cmd = cmd_rx.recv() => {
                            match cmd {
                                Some(ManagerCommand::Unsubscribe(_)) => Action::Continue, // Ignore
                                Some(ManagerCommand::Subscribe(_)) => Action::Continue, // Ignore
                                None => Action::Quit,
                            }
                        }
                        msg = stream.next() => {
                            match msg {
                                Some(msg) => {
                                    let channel_name = msg.get_channel_name().to_string();
                                    // Messages come from pattern subscription, so channel_name is the actual topic
                                    let payload: String = match msg.get_payload() {
                                        Ok(p) => p,
                                        Err(_) => {
                                            warn!("Received message with invalid payload on {}", channel_name);
                                            String::new()
                                        }
                                    };
                             
                                    if !payload.is_empty() {
                                        // Forward to local subscribers
                                        if let Some(sender) = local_channels_clone.get(&channel_name) {
                                            match serde_json::from_str::<T>(&payload) {
                                                Ok(val) => {
                                                    let _ = sender.send(val);
                                                }
                                                Err(e) => {
                                                    error!("Failed to deserialize message on {}: {}", channel_name, e);
                                                }
                                            }
                                        }
                                    }
                                    Action::Continue
                                }
                                None => Action::Reconnect,
                            }
                        }
                    };
                    
                    match action {
                        Action::Continue => {}
                        Action::Reconnect => {
                            warn!("Manager loop ended (Redis stream interrupted). Reconnecting...");
                            break; // Reconnect
                        }
                        Action::Quit => {
                             info!("Manager command channel closed");
                             return;
                        }
                        Action::RunCmd(_) => {} // Should not happen
                    }
                }
            }
        });

        Self {
            pub_tx,
            cmd_tx,
            local_channels,
            capacity,
        }
    }

    /// Publish a message to a topic (Redis)
    pub fn publish(&self, topic: String, message: T) {
        if let Err(e) = self.pub_tx.send((topic, message)) {
            error!("Failed to queue publish message: {}", e);
        }
    }

    /// Subscribe to a topic
    /// Returns a broadcast receiver.
    pub fn subscribe(&self, topic: String) -> broadcast::Receiver<T> {
        // If channel exists, just subscribe
        if let Some(sender) = self.local_channels.get(&topic) {
            return sender.subscribe();
        }

        // Create new channel
        let (tx, rx) = broadcast::channel(self.capacity);
        self.local_channels.insert(topic.clone(), tx);
        
        // No need to send command as we use PSUBSCRIBE

        rx
    }

    /// Remove a topic (cleanup)
    pub fn remove_topic(&self, topic: &str) {
        if self.local_channels.remove(topic).is_some() {
            let _ = self.cmd_tx.send(ManagerCommand::Unsubscribe(topic.to_string()));
        }
    }
}
