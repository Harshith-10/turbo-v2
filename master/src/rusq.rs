use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use dashmap::DashMap;
// use std::sync::atomic::{AtomicBool, AtomicU64, Ordering}; // AtomicBool unused if queue is gone
use std::sync::atomic::{AtomicU64, Ordering};
// use std::sync::Arc; // Unused if queue is gone
// use std::time::{Duration, Instant}; // Unused
use tokio::sync::broadcast;

/// Message priority levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Priority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

/// A message wrapper that contains metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message<T> {
    pub id: u64,
    pub payload: T,
    pub priority: Priority,
    pub timestamp: u64,
    pub retry_count: u32,
    pub topic: String,
}

impl<T> Message<T> {
    pub fn new(payload: T, topic: String) -> Self {
        Self {
            id: generate_message_id(),
            payload,
            priority: Priority::Normal,
            timestamp: current_timestamp_millis(),
            retry_count: 0,
            topic,
        }
    }

    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }
}

// Utility functions
fn generate_message_id() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    COUNTER.fetch_add(1, Ordering::SeqCst)
}

fn current_timestamp_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// A simple Pub/Sub engine using Tokio broadcast channels
pub struct BroadcastEngine<T> {
    /// Map of topic -> broadcast sender
    channels: DashMap<String, broadcast::Sender<T>>,
    /// Capacity of each broadcast channel
    capacity: usize,
}

impl<T> BroadcastEngine<T>
where
    T: Clone + Send + 'static,
{
    pub fn new(capacity: usize) -> Self {
        Self {
            channels: DashMap::new(),
            capacity,
        }
    }

    /// Publish a message to a topic
    /// Returns the number of active subscribers
    pub fn publish(&self, topic: String, message: T) -> usize {
        if let Some(sender) = self.channels.get(&topic) {
            // Send returns the number of receivers
            // We ignore errors (no receivers is fine, effectively 0)
            sender.send(message).unwrap_or(0)
        } else {
            0
        }
    }

    /// Subscribe to a topic
    pub fn subscribe(&self, topic: String) -> broadcast::Receiver<T> {
        let sender = self.channels.entry(topic).or_insert_with(|| {
            let (tx, _) = broadcast::channel(self.capacity);
            tx
        });
        sender.subscribe()
    }

    /// Remove a topic (cleanup)
    pub fn remove_topic(&self, topic: &str) {
        self.channels.remove(topic);
    }
}
