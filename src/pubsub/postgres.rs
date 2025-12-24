//! PostgreSQL pub/sub backend using LISTEN/NOTIFY
//!
//! Uses PostgreSQL's native pub/sub mechanism for cross-node broadcasting.
//! Supports both TLS and non-TLS connections based on URL parameters.

use crate::pubsub::PubSub;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_postgres::AsyncMessage;
use tokio_postgres_rustls::MakeRustlsConnect;

/// PostgreSQL-backed pub/sub using LISTEN/NOTIFY
pub struct PostgresPubSub {
    connection_string: String,
    subscriptions: Arc<RwLock<HashMap<String, String>>>,
}

impl PostgresPubSub {
    /// Create TLS connector for PostgreSQL connections
    fn make_tls_connector() -> MakeRustlsConnect {
        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        let config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        MakeRustlsConnect::new(config)
    }

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
        // Verify connection works - use TLS by default for cloud databases
        let tls = Self::make_tls_connector();
        let (client, connection) = tokio_postgres::connect(database_url, tls).await?;

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
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Stable channel name for PostgreSQL LISTEN/NOTIFY.
    ///
    /// Channel names must be valid identifiers and <= 63 bytes. Use a fixed-length
    /// hash to avoid collisions from sanitization and to stay under the limit.
    fn channel_for_stream(stream: &str) -> String {
        const FNV_OFFSET: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x100000001b3;

        let mut hash = FNV_OFFSET;
        for byte in stream.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }

        format!("oc_{:016x}", hash)
    }
}

#[async_trait]
impl PubSub for PostgresPubSub {
    async fn publish(&self, stream: &str, payload: &[u8]) -> anyhow::Result<()> {
        let tls = Self::make_tls_connector();
        let (client, connection) = tokio_postgres::connect(&self.connection_string, tls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("Publish connection error: {}", e);
            }
        });

        let channel = Self::channel_for_stream(stream);

        // Encode payload as base64 for safe transmission
        let encoded = base64_encode(payload);

        // Use pg_notify function for safe parameter binding
        client
            .execute("SELECT pg_notify($1, $2)", &[&channel, &encoded])
            .await?;

        Ok(())
    }

    async fn subscribe(&self, stream: &str) -> anyhow::Result<()> {
        let channel = Self::channel_for_stream(stream);

        let mut subs = self.subscriptions.write().await;
        if let Some(existing) = subs.get(&channel) {
            if existing != stream {
                return Err(anyhow::anyhow!(
                    "channel hash collision: {} maps to both '{}' and '{}'",
                    channel,
                    existing,
                    stream
                ));
            }
        } else {
            subs.insert(channel, stream.to_string());
        }

        Ok(())
    }

    async fn unsubscribe(&self, stream: &str) -> anyhow::Result<()> {
        let channel = Self::channel_for_stream(stream);

        let mut subs = self.subscriptions.write().await;
        subs.remove(&channel);

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
        subscriptions: &Arc<RwLock<HashMap<String, String>>>,
        callback: Arc<F>,
    ) -> anyhow::Result<()>
    where
        F: Fn(String, Vec<u8>) + Send + Sync + 'static,
    {
        let tls = Self::make_tls_connector();
        let (client, mut connection) =
            tokio_postgres::connect(connection_string, tls).await?;

        // Subscribe to all channels
        {
            let subs = subscriptions.read().await;
            for channel in subs.keys() {
                client
                    .batch_execute(&format!("LISTEN {}", channel))
                    .await?;
                tracing::info!("Subscribed to channel: {}", channel);
            }
        }

        // Keep track of current subscriptions
        let mut current_subs: std::collections::HashSet<String> =
            subscriptions.read().await.keys().cloned().collect();

        loop {
            // Check for subscription changes
            {
                let new_subs = subscriptions.read().await;
                let mut to_add = Vec::new();
                let mut to_remove = Vec::new();

                for channel in new_subs.keys() {
                    if !current_subs.contains(channel) {
                        to_add.push(channel.clone());
                    }
                }
                for channel in current_subs.iter() {
                    if !new_subs.contains_key(channel) {
                        to_remove.push(channel.clone());
                    }
                }

                for channel in to_add {
                    client.batch_execute(&format!("LISTEN {}", channel)).await?;
                    current_subs.insert(channel.clone());
                    tracing::info!("Subscribed to channel: {}", channel);
                }
                for channel in to_remove {
                    client.batch_execute(&format!("UNLISTEN {}", channel)).await?;
                    current_subs.remove(&channel);
                    tracing::info!("Unsubscribed from channel: {}", channel);
                }
            }

            // Poll for messages with timeout
            tokio::select! {
                msg = std::future::poll_fn(|cx| connection.poll_message(cx)) => {
                    match msg {
                        Some(Ok(AsyncMessage::Notification(notification))) => {
                            let channel = notification.channel().to_string();
                            let payload = base64_decode(notification.payload());
                            let stream = {
                                let subs = subscriptions.read().await;
                                subs.get(&channel).cloned()
                            };
                            if let Some(stream) = stream {
                                callback(stream, payload);
                            } else {
                                tracing::warn!("Received notification for unknown channel: {}", channel);
                            }
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
#[allow(dead_code)]
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
    fn test_channel_for_stream() {
        assert_eq!(
            PostgresPubSub::channel_for_stream("test"),
            "oc_f9e6e6ef197c2b25"
        );
        assert_eq!(
            PostgresPubSub::channel_for_stream("test-channel"),
            "oc_35532a9354f87833"
        );
        assert_eq!(
            PostgresPubSub::channel_for_stream("123"),
            "oc_456fc2181822c4db"
        );
        assert_eq!(
            PostgresPubSub::channel_for_stream("my.channel"),
            "oc_f3e42e433c3c0e76"
        );
    }
}
