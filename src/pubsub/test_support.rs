//! A scripted in-process backend for unit tests that drive the server.
//!
//! MOCKED: the whole backend. Every call is recorded, every answer is chosen
//! by the test, and events reach the listener only when the test pushes them.
//! What this exercises is the server's bookkeeping around the backend - the
//! interest refcount, the readiness barrier, gap handling - never the backend
//! itself, which has its own live-PostgreSQL tests.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::protocol::v1::publication::Publication;
use crate::pubsub::{EventHandler, PubSub, PubSubError, PubSubEvent, PubSubStats, StatsSnapshot};
use crate::sync::lock;

/// What the next `subscribe` answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscribeAnswer {
    Ok,
    AckTimeout,
    NotReady,
}

/// The scripted backend.
pub struct ScriptedPubSub {
    ready: Mutex<bool>,
    subscribe_answer: Mutex<SubscribeAnswer>,
    /// Every `subscribe`/`unsubscribe`, in order: `("LISTEN", stream)` /
    /// `("UNLISTEN", stream)`.
    commands: Mutex<Vec<(&'static str, String)>>,
    events: mpsc::UnboundedSender<PubSubEvent>,
    receiver: tokio::sync::Mutex<Option<mpsc::UnboundedReceiver<PubSubEvent>>>,
    stats: Arc<PubSubStats>,
}

impl ScriptedPubSub {
    /// A ready backend that acknowledges everything.
    pub fn new() -> Arc<Self> {
        let (events, receiver) = mpsc::unbounded_channel();
        Arc::new(Self {
            ready: Mutex::new(true),
            subscribe_answer: Mutex::new(SubscribeAnswer::Ok),
            commands: Mutex::new(Vec::new()),
            events,
            receiver: tokio::sync::Mutex::new(Some(receiver)),
            stats: Arc::new(PubSubStats::default()),
        })
    }

    /// A backend that never becomes ready (every call is refused).
    pub fn unready() -> Arc<Self> {
        let backend = Self::new();
        backend.set_ready(false);
        backend
    }

    pub fn set_ready(&self, ready: bool) {
        *lock(&self.ready) = ready;
    }

    pub fn answer_subscribes_with(&self, answer: SubscribeAnswer) {
        *lock(&self.subscribe_answer) = answer;
    }

    /// The recorded command log.
    pub fn commands(&self) -> Vec<(&'static str, String)> {
        lock(&self.commands).clone()
    }

    /// Hand one event to the listener, as the backend would.
    pub fn emit(&self, event: PubSubEvent) {
        self.events.send(event).expect("the listener's queue is unbounded and never closed");
    }
}

#[async_trait]
impl PubSub for ScriptedPubSub {
    async fn publish(&self, stream: &str, json_payload: &str) -> Result<(), PubSubError> {
        self.publish_publication(&Publication::new("scripted", stream, json_payload)).await
    }

    async fn publish_publication(&self, publication: &Publication) -> Result<(), PubSubError> {
        if !self.is_ready() {
            return Err(PubSubError::NotReady);
        }
        self.emit(PubSubEvent::Publication {
            stream: publication.stream.clone(),
            payload: bytes::Bytes::from(publication.json_payload.clone()),
        });
        Ok(())
    }

    async fn subscribe(&self, stream: &str) -> Result<(), PubSubError> {
        lock(&self.commands).push(("LISTEN", stream.to_string()));
        if !self.is_ready() {
            return Err(PubSubError::NotReady);
        }
        let answer = *lock(&self.subscribe_answer);
        match answer {
            SubscribeAnswer::Ok => Ok(()),
            SubscribeAnswer::AckTimeout => Err(PubSubError::AckTimeout {
                command: "LISTEN",
                stream: stream.to_string(),
                timeout: Duration::from_millis(1),
            }),
            SubscribeAnswer::NotReady => Err(PubSubError::NotReady),
        }
    }

    async fn unsubscribe(&self, stream: &str) -> Result<(), PubSubError> {
        lock(&self.commands).push(("UNLISTEN", stream.to_string()));
        Ok(())
    }

    fn is_ready(&self) -> bool {
        *lock(&self.ready)
    }

    fn stats(&self) -> StatsSnapshot {
        self.stats.snapshot()
    }

    async fn listen(&self, on_event: EventHandler) -> Result<(), PubSubError> {
        let mut events =
            self.receiver.lock().await.take().ok_or(PubSubError::ListenerAlreadyStarted)?;
        while let Some(event) = events.recv().await {
            on_event(event);
        }
        Ok(())
    }
}
