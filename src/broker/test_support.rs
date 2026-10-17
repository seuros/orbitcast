//! An in-memory [`Broker`], so recovery flows are testable without
//! PostgreSQL. Shared by the recovery and server tests.
//!
//! MOCKED: the store. REAL: the contract the recovery controller relies on -
//! per-stream contiguous sequences, deduplication by publication id (identical
//! vs conflicting), per-session fencing, snapshot memory, and the counters.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use super::{
    Broker, BrokerError, BrokerStats, DuplicateKind, Retained, RetainedMessage, StreamRange,
};
use crate::protocol::v1::publication::{MAX_NOTIFY_PAYLOAD_LEN, Publication, PublicationNotice};
use crate::protocol::v1::recovery::Cursor;
use crate::protocol::{Envelope as _, ProtocolError};
use crate::pubsub::PubSubError;

/// An in-memory broker standing in for the PostgreSQL one.
#[derive(Default)]
pub struct FakeBroker {
    streams: Mutex<HashMap<String, FakeStream>>,
    /// `session_id -> (fencing generation, snapshot)`.
    sessions: Mutex<HashMap<String, FakeSession>>,
    touches: Mutex<Vec<String>>,
    stats: BrokerStats,
    pub fail_range: AtomicBool,
    /// Pretend the overflow relay is enabled: oversized payloads are retained
    /// and reported as relayed instead of refused.
    pub relay_enabled: AtomicBool,
}

#[derive(Default)]
struct FakeStream {
    generation: u64,
    next_seq: u64,
    messages: Vec<RetainedMessage>,
}

#[derive(Default)]
struct FakeSession {
    lease: u64,
    snapshot: Option<String>,
}

impl FakeBroker {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Append a message with a fresh id, the way the real ordering
    /// transaction would.
    pub fn publish(&self, stream: &str, payload: &str) -> Cursor {
        self.append(stream, &uuid::Uuid::new_v4().to_string(), payload)
    }

    fn append(&self, stream: &str, publication_id: &str, payload: &str) -> Cursor {
        let mut streams = self.streams.lock().unwrap();
        let entry = streams.entry(stream.to_string()).or_insert(FakeStream {
            generation: 1,
            next_seq: 1,
            messages: Vec::new(),
        });
        let position = Cursor::new(entry.generation, entry.next_seq);
        entry.next_seq += 1;
        entry.messages.push(RetainedMessage {
            stream: stream.to_string(),
            publication_id: publication_id.to_string(),
            position,
            json_payload: payload.to_string(),
        });
        drop(streams);
        position
    }

    /// Drop everything before `sequence`, as retention would.
    pub fn expire_before(&self, stream: &str, sequence: u64) {
        self.remove_sequences(stream, 0..sequence);
    }

    /// Drop the messages whose sequence falls in `range`, as a namespace
    /// quota trim or a byte-budget trim would: a hole, not a prefix.
    pub fn remove_sequences(&self, stream: &str, range: std::ops::Range<u64>) {
        let mut streams = self.streams.lock().unwrap();
        if let Some(entry) = streams.get_mut(stream) {
            entry.messages.retain(|message| !range.contains(&message.position.sequence));
        }
    }

    pub fn snapshot_for(&self, session_id: &str) -> Option<String> {
        self.sessions.lock().unwrap().get(session_id).and_then(|s| s.snapshot.clone())
    }

    /// Plant a snapshot, as a previous process would have left it.
    pub fn store_snapshot(&self, session_id: &str, snapshot: &str) {
        self.sessions.lock().unwrap().entry(session_id.to_string()).or_default().snapshot =
            Some(snapshot.to_string());
    }

    /// How many times each session's expiry was extended by a live tail.
    pub fn touches(&self, session_id: &str) -> usize {
        self.touches.lock().unwrap().iter().filter(|s| *s == session_id).count()
    }

    /// Every payload retained for `stream`, in sequence order.
    pub fn payloads(&self, stream: &str) -> Vec<String> {
        self.streams
            .lock()
            .unwrap()
            .get(stream)
            .map(|entry| entry.messages.iter().map(|m| m.json_payload.clone()).collect::<Vec<_>>())
            .unwrap_or_default()
    }
}

#[async_trait]
impl Broker for FakeBroker {
    async fn retain(&self, publication: &Publication) -> Result<Retained, BrokerError> {
        publication.validate().map_err(|e: ProtocolError| BrokerError::Publication(e.into()))?;

        // The same size rule the real store applies: inline when it fits,
        // relayed when the relay is on, refused otherwise.
        let inline = serde_json::to_string(&PublicationNotice::inline(publication.clone()))
            .expect("a validated publication encodes");
        let relayed = !PublicationNotice::fits_notify_payload(&inline);
        if relayed && !self.relay_enabled.load(Ordering::Relaxed) {
            return Err(BrokerError::Publication(PubSubError::PayloadTooLarge {
                len: inline.len(),
                max: MAX_NOTIFY_PAYLOAD_LEN,
            }));
        }

        let existing = self.streams.lock().unwrap().get(&publication.stream).and_then(|entry| {
            entry
                .messages
                .iter()
                .find(|m| m.publication_id == publication.id)
                .map(|m| (m.position, m.json_payload == publication.json_payload))
        });
        if let Some((position, identical)) = existing {
            let kind =
                if identical { DuplicateKind::Identical } else { DuplicateKind::Conflicting };
            self.stats.record_duplicate(kind);
            return Ok(Retained { position, duplicate: Some(kind), relayed });
        }

        let position = self.append(&publication.stream, &publication.id, &publication.json_payload);
        self.stats.record_retained();
        Ok(Retained { position, duplicate: None, relayed })
    }

    async fn range(&self, stream: &str) -> Result<StreamRange, BrokerError> {
        if self.fail_range.load(Ordering::Relaxed) {
            return Err(BrokerError::NotReady);
        }

        let range = self.streams.lock().unwrap().get(stream).map(|entry| StreamRange {
            watermark: Cursor::new(entry.generation, entry.next_seq - 1),
            oldest: entry.messages.first().map(|m| m.position.sequence),
        });
        // A stream never published to starts before sequence 1, as in the
        // real store.
        Ok(range.unwrap_or(StreamRange { watermark: Cursor::new(1, 0), oldest: None }))
    }

    async fn fetch_after(
        &self,
        stream: &str,
        after: Cursor,
        limit: usize,
    ) -> Result<Vec<RetainedMessage>, BrokerError> {
        Ok(self
            .streams
            .lock()
            .unwrap()
            .get(stream)
            .map(|entry| {
                entry
                    .messages
                    .iter()
                    .filter(|message| {
                        message.position.generation == after.generation
                            && message.position.sequence > after.sequence
                    })
                    .take(limit)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn fence(&self, session_id: &str) -> Result<u64, BrokerError> {
        let mut sessions = self.sessions.lock().unwrap();
        let session = sessions.entry(session_id.to_string()).or_default();
        session.lease += 1;
        let lease = session.lease;
        drop(sessions);
        self.stats.record_fenced();
        Ok(lease)
    }

    async fn fencing_generation(&self, session_id: &str) -> Result<u64, BrokerError> {
        Ok(self.sessions.lock().unwrap().get(session_id).map_or(0, |s| s.lease))
    }

    async fn touch_session(&self, session_id: &str) -> Result<(), BrokerError> {
        self.touches.lock().unwrap().push(session_id.to_string());
        Ok(())
    }

    async fn save_snapshot(&self, session_id: &str, snapshot: &str) -> Result<(), BrokerError> {
        self.store_snapshot(session_id, snapshot);
        Ok(())
    }

    async fn load_snapshot(&self, session_id: &str) -> Result<Option<String>, BrokerError> {
        Ok(self.snapshot_for(session_id))
    }

    fn counters(&self) -> &BrokerStats {
        &self.stats
    }
}
