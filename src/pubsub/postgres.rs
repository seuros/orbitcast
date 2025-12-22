//! PostgreSQL pub/sub backend using LISTEN/NOTIFY
//!
//! Uses PostgreSQL's native pub/sub mechanism for cross-node broadcasting.

use crate::pubsub::PubSub;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_postgres::{AsyncMessage, NoTls};

/// PostgreSQL-backed pub/sub using LISTEN/NOTIFY
pub struct PostgresPubSub {
    connection_string: String,
    subscriptions: Arc<RwLock<Vec<String>>>,
}

impl PostgresPubSub {
    /// Create a new PostgreSQL pub/sub backend
    ///
    /// # Arguments
    /// * `database_url` - PostgreSQL connection string
    ///
    /// # Example
    /// ```ignore
    /// let pubsub = PostgresPubSub::new("postgres://user:pass@localhost/db").await?;
    /// ```
    pub async fn new(database_url: &str) -> anyhow::Result<Self> {
        // Verify connection works
        let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;

        // Spawn connection to keep it alive for the test
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("Connection error during init: {}", e);
            }
        });

        // Simple connectivity test
        client.execute("SELECT 1", &[]).await?;

        Ok(Self {
            connection_string: database_url.to_string(),
            subscriptions: Arc::new(RwLock::new(Vec::new())),
        })
    }

    /// Sanitize channel name for PostgreSQL
    /// Channel names must be valid identifiers
    fn sanitize_channel(stream: &str) -> String {
        let sanitized: String = stream
            .chars()
            .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
            .collect();

        // Ensure it starts with a letter or underscore
        if sanitized.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(true) {
            format!("ch_{}", sanitized)
        } else {
            sanitized
        }
    }
}

#[async_trait]
impl PubSub for PostgresPubSub {
    async fn publish(&self, stream: &str, payload: &[u8]) -> anyhow::Result<()> {
        let (client, connection) = tokio_postgres::connect(&self.connection_string, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("Publish connection error: {}", e);
            }
        });

        let channel = Self::sanitize_channel(stream);

        // Encode payload as base64 for safe transmission
        let encoded = base64_encode(payload);

        // Use pg_notify function for safe parameter binding
        client
            .execute("SELECT pg_notify($1, $2)", &[&channel, &encoded])
            .await?;

        Ok(())
    }

    async fn subscribe(&self, stream: &str) -> anyhow::Result<()> {
        let channel = Self::sanitize_channel(stream);

        let mut subs = self.subscriptions.write().await;
        if !subs.contains(&channel) {
            subs.push(channel);
        }

        Ok(())
    }

    async fn unsubscribe(&self, stream: &str) -> anyhow::Result<()> {
        let channel = Self::sanitize_channel(stream);

        let mut subs = self.subscriptions.write().await;
        subs.retain(|s| s != &channel);

        Ok(())
    }

    async fn listen<F>(&self, callback: F) -> anyhow::Result<()>
    where
        F: Fn(String, Vec<u8>) + Send + Sync + 'static,
    {
        let callback = Arc::new(callback);
        let subscriptions = self.subscriptions.clone();
        let connection_string = self.connection_string.clone();

        loop {
            match Self::run_listener(&connection_string, &subscriptions, callback.clone()).await {
                Ok(()) => break,
                Err(e) => {
                    tracing::error!("Listener error, reconnecting: {}", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            }
        }

        Ok(())
    }
}

impl PostgresPubSub {
    async fn run_listener<F>(
        connection_string: &str,
        subscriptions: &Arc<RwLock<Vec<String>>>,
        callback: Arc<F>,
    ) -> anyhow::Result<()>
    where
        F: Fn(String, Vec<u8>) + Send + Sync + 'static,
    {
        let (client, mut connection) =
            tokio_postgres::connect(connection_string, NoTls).await?;

        // Subscribe to all channels
        {
            let subs = subscriptions.read().await;
            for channel in subs.iter() {
                client
                    .batch_execute(&format!("LISTEN {}", channel))
                    .await?;
                tracing::info!("Subscribed to channel: {}", channel);
            }
        }

        // Keep track of current subscriptions
        let mut current_subs: Vec<String> = subscriptions.read().await.clone();

        loop {
            // Check for subscription changes
            {
                let new_subs = subscriptions.read().await;
                for channel in new_subs.iter() {
                    if !current_subs.contains(channel) {
                        client.batch_execute(&format!("LISTEN {}", channel)).await?;
                        current_subs.push(channel.clone());
                        tracing::info!("Subscribed to channel: {}", channel);
                    }
                }
                for channel in current_subs.clone().iter() {
                    if !new_subs.contains(channel) {
                        client.batch_execute(&format!("UNLISTEN {}", channel)).await?;
                        current_subs.retain(|c| c != channel);
                        tracing::info!("Unsubscribed from channel: {}", channel);
                    }
                }
            }

            // Poll for messages with timeout
            tokio::select! {
                msg = std::future::poll_fn(|cx| connection.poll_message(cx)) => {
                    match msg {
                        Some(Ok(AsyncMessage::Notification(notification))) => {
                            let channel = notification.channel().to_string();
                            let payload = base64_decode(notification.payload());
                            callback(channel, payload);
                        }
                        Some(Ok(_)) => {
                            // Other message types (notices, etc.)
                        }
                        Some(Err(e)) => {
                            return Err(anyhow::anyhow!("Connection error: {}", e));
                        }
                        None => {
                            return Err(anyhow::anyhow!("Connection closed"));
                        }
                    }
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {
                    // Keepalive - send empty query to check connection
                    if let Err(e) = client.execute("SELECT 1", &[]).await {
                        return Err(anyhow::anyhow!("Keepalive failed: {}", e));
                    }
                }
            }
        }
    }
}

/// Base64 encode bytes for safe NOTIFY payload
fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let mut result = String::new();
    let mut i = 0;

    while i < data.len() {
        let b0 = data[i] as usize;
        let b1 = data.get(i + 1).copied().unwrap_or(0) as usize;
        let b2 = data.get(i + 2).copied().unwrap_or(0) as usize;

        result.push(ALPHABET[b0 >> 2] as char);
        result.push(ALPHABET[((b0 & 0x03) << 4) | (b1 >> 4)] as char);

        if i + 1 < data.len() {
            result.push(ALPHABET[((b1 & 0x0f) << 2) | (b2 >> 6)] as char);
        } else {
            result.push('=');
        }

        if i + 2 < data.len() {
            result.push(ALPHABET[b2 & 0x3f] as char);
        } else {
            result.push('=');
        }

        i += 3;
    }

    result
}

/// Base64 decode string to bytes
fn base64_decode(data: &str) -> Vec<u8> {
    fn char_to_val(c: char) -> u8 {
        match c {
            'A'..='Z' => c as u8 - b'A',
            'a'..='z' => c as u8 - b'a' + 26,
            '0'..='9' => c as u8 - b'0' + 52,
            '+' => 62,
            '/' => 63,
            _ => 0,
        }
    }

    let chars: Vec<char> = data.chars().filter(|&c| c != '=').collect();
    let mut result = Vec::new();

    for chunk in chars.chunks(4) {
        if chunk.len() >= 2 {
            let b0 = char_to_val(chunk[0]);
            let b1 = char_to_val(chunk[1]);
            result.push((b0 << 2) | (b1 >> 4));

            if chunk.len() >= 3 {
                let b2 = char_to_val(chunk[2]);
                result.push((b1 << 4) | (b2 >> 2));

                if chunk.len() >= 4 {
                    let b3 = char_to_val(chunk[3]);
                    result.push((b2 << 6) | b3);
                }
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base64_roundtrip() {
        let original = b"Hello, OrbitCast!";
        let encoded = base64_encode(original);
        let decoded = base64_decode(&encoded);
        assert_eq!(original.to_vec(), decoded);
    }

    #[test]
    fn test_sanitize_channel() {
        assert_eq!(PostgresPubSub::sanitize_channel("test"), "test");
        assert_eq!(PostgresPubSub::sanitize_channel("test-channel"), "test_channel");
        assert_eq!(PostgresPubSub::sanitize_channel("123"), "ch_123");
        assert_eq!(PostgresPubSub::sanitize_channel("my.channel"), "my_channel");
    }
}
