use std::assert_matches;

use super::*;
use crate::metrics::LagReason;
use serde_json::Value;
use std::cell::Cell;
use std::pin::{Pin, pin};
use std::task::{Context, Poll, Waker};
use tokio::sync::oneshot;

const TURBO_A: &str = r#"{"channel":"Turbo::StreamsChannel","signed_stream_name":"aaa"}"#;
const TURBO_B: &str = r#"{"channel":"Turbo::StreamsChannel","signed_stream_name":"bbb"}"#;
const PAYLOAD: &[u8] = br#"{"text":"hello"}"#;
const CHAT: &str = "chat:42";

fn hub_with_queue(capacity: usize) -> (Hub, mpsc::Receiver<Outgoing>) {
    let (tx, rx) = mpsc::channel(capacity);
    (Hub::new(tx), rx)
}

/// Collect the data frames queued right now, as JSON.
fn drain_frames(rx: &mut mpsc::Receiver<Outgoing>) -> Vec<Value> {
    let mut out = Vec::new();
    while let Ok(Outgoing::Cargo(cargo)) = rx.try_recv() {
        out.push(serde_json::from_slice(&cargo.data).expect("frame is JSON"));
    }
    out
}

fn session(conn_id: u32) -> Session {
    Session::new(conn_id, "/cable".to_string())
}

/// Drain the outbound queue into `(conn_id, identifier, message)` triples.
fn drain(rx: &mut mpsc::Receiver<Outgoing>) -> Vec<(u32, String, Value)> {
    let mut frames = Vec::new();

    while let Ok(message) = rx.try_recv() {
        match message {
            Outgoing::Cargo(cargo) => {
                let parsed: Value = serde_json::from_slice(&cargo.data)
                    .expect("delivered frame must be valid JSON");
                let identifier = parsed["identifier"]
                    .as_str()
                    .expect("delivered frame must carry an identifier")
                    .to_string();
                frames.push((cargo.conn_id, identifier, parsed["message"].clone()));
            }
            Outgoing::Disembark(_) => {}
        }
    }

    frames
}

#[test]
fn two_identifiers_on_one_connection_each_receive_their_own_frame() {
    let (hub, mut rx) = hub_with_queue(16);
    hub.add_session(session(1));

    assert!(hub.add_membership(1, TURBO_A, "posts:1"));
    assert!(!hub.add_membership(1, TURBO_B, "posts:1"));

    assert_eq!(hub.deliver("posts:1", PAYLOAD), 2);

    let frames = drain(&mut rx);
    assert_eq!(frames.len(), 2);
    assert!(
        frames.iter().all(|(conn_id, _, message)| { *conn_id == 1 && message["text"] == "hello" })
    );

    let mut identifiers: Vec<&str> = frames.iter().map(|(_, id, _)| id.as_str()).collect();
    identifiers.sort_unstable();
    assert_eq!(identifiers, vec![TURBO_A, TURBO_B]);
}

#[test]
fn two_connections_on_one_stream_each_receive_their_own_identifier() {
    let (hub, mut rx) = hub_with_queue(16);
    hub.add_session(session(1));
    hub.add_session(session(2));

    assert!(hub.add_membership(1, TURBO_A, "posts:1"));
    assert!(!hub.add_membership(2, TURBO_B, "posts:1"));

    assert_eq!(hub.deliver("posts:1", PAYLOAD), 2);

    let mut frames = drain(&mut rx);
    frames.sort_by_key(|(conn_id, _, _)| *conn_id);
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].0, 1);
    assert_eq!(frames[0].1, TURBO_A);
    assert_eq!(frames[1].0, 2);
    assert_eq!(frames[1].1, TURBO_B);
    assert_eq!(frames[0].2, frames[1].2);
}

#[test]
fn identical_identifiers_share_one_encoded_frame() {
    let (hub, _rx) = hub_with_queue(16);
    hub.add_session(session(1));
    hub.add_session(session(2));

    hub.add_membership(1, TURBO_A, "posts:1");
    hub.add_membership(2, TURBO_A, "posts:1");

    let frames = hub.addressed_frames("posts:1", PAYLOAD, None);
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].1.as_ptr(), frames[1].1.as_ptr(), "shared payload bytes expected");
}

#[test]
fn duplicate_subscribe_creates_a_single_membership() {
    let (hub, mut rx) = hub_with_queue(16);
    hub.add_session(session(1));

    assert!(hub.add_membership(1, TURBO_A, "posts:1"));
    assert!(hub.has_subscription(1, TURBO_A));
    // Repeat subscribe for an already-installed membership: no-op.
    assert!(!hub.add_membership(1, TURBO_A, "posts:1"));

    assert_eq!(hub.stream_member_count("posts:1"), 1);
    assert_eq!(hub.deliver("posts:1", PAYLOAD), 1);
    assert_eq!(drain(&mut rx).len(), 1);
}

#[test]
fn unsubscribe_keeps_sibling_identifier_on_the_shared_stream() {
    let (hub, mut rx) = hub_with_queue(16);
    hub.add_session(session(1));

    hub.add_membership(1, TURBO_A, "posts:1");
    hub.add_membership(1, TURBO_B, "posts:1");

    // Last membership of TURBO_A leaves, but the stream keeps a member, so
    // backend interest is retained.
    let released = hub.remove_subscription(1, TURBO_A);
    assert!(released.is_empty(), "backend interest must be retained");
    assert!(!hub.has_subscription(1, TURBO_A));
    assert_eq!(hub.stream_member_count("posts:1"), 1);

    assert_eq!(hub.deliver("posts:1", PAYLOAD), 1);
    let frames = drain(&mut rx);
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].1, TURBO_B);

    // Last member leaving releases backend interest.
    let released = hub.remove_subscription(1, TURBO_B);
    assert_eq!(released, vec!["posts:1".to_string()]);
    assert_eq!(hub.stream_member_count("posts:1"), 0);
    assert_eq!(hub.stream_count(), 0);
    assert_eq!(hub.deliver("posts:1", PAYLOAD), 0);
}

#[test]
fn unsubscribe_of_one_connection_keeps_the_other_connection() {
    let (hub, mut rx) = hub_with_queue(16);
    hub.add_session(session(1));
    hub.add_session(session(2));

    hub.add_membership(1, TURBO_A, "posts:1");
    hub.add_membership(2, TURBO_A, "posts:1");

    assert_matches!(hub.remove_subscription(1, TURBO_A).as_slice(), []);
    assert_eq!(hub.stream_member_count("posts:1"), 1);

    assert_eq!(hub.deliver("posts:1", PAYLOAD), 1);
    let frames = drain(&mut rx);
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].0, 2);
}

#[test]
fn teardown_removes_every_membership_and_empties_the_indexes() {
    let (hub, mut rx) = hub_with_queue(16);
    hub.add_session(session(1));
    hub.add_session(session(2));

    hub.add_membership(1, TURBO_A, "posts:1");
    hub.add_membership(1, TURBO_A, "posts:2");
    hub.add_membership(1, TURBO_B, "posts:1");
    hub.add_membership(2, TURBO_A, "posts:1");

    // conn 2 still holds posts:1, so only posts:2 is released.
    let mut released = hub.remove_session(1);
    released.sort();
    assert_eq!(released, vec!["posts:2".to_string()]);
    assert!(!hub.has_subscription(1, TURBO_A));
    assert!(hub.subscriptions.get(&1).is_none());
    assert!(hub.get_session(1).is_none());
    assert_eq!(hub.session_count(), 1);

    let released = hub.remove_session(2);
    assert_eq!(released, vec!["posts:1".to_string()]);
    assert_eq!(hub.session_count(), 0);
    assert_eq!(hub.stream_count(), 0);
    assert!(hub.subscriptions.is_empty());
    assert!(hub.stream_members.is_empty());

    assert_eq!(hub.deliver("posts:1", PAYLOAD), 0);
    assert_matches!(drain(&mut rx).as_slice(), []);
}

#[test]
fn removing_an_unknown_session_or_subscription_is_a_no_op() {
    let (hub, _rx) = hub_with_queue(4);

    assert_matches!(hub.remove_session(99).as_slice(), []);
    assert_matches!(hub.remove_subscription(99, TURBO_A).as_slice(), []);
    assert!(!hub.remove_membership(99, TURBO_A, "posts:1"));
    assert_matches!(hub.clear_streams(99, TURBO_A).as_slice(), []);

    hub.add_session(session(1));
    assert_matches!(hub.remove_subscription(1, TURBO_A).as_slice(), []);
    assert!(!hub.remove_membership(1, TURBO_A, "posts:1"));
    assert_matches!(hub.clear_streams(1, TURBO_A).as_slice(), []);
}

#[test]
fn remove_membership_drops_one_stream_and_keeps_the_subscription() {
    let (hub, mut rx) = hub_with_queue(16);
    hub.add_session(session(1));

    hub.add_membership(1, TURBO_A, "posts:1");
    hub.add_membership(1, TURBO_A, "posts:2");

    // Last member of posts:1 leaves: interest is released, but the
    // subscription keeps posts:2 and stays addressable.
    assert!(hub.remove_membership(1, TURBO_A, "posts:1"));
    assert!(hub.has_subscription(1, TURBO_A));
    assert_eq!(hub.stream_member_count("posts:1"), 0);

    // Removing a stream the subscription does not hold is a no-op.
    assert!(!hub.remove_membership(1, TURBO_A, "posts:1"));

    assert_eq!(hub.deliver("posts:2", PAYLOAD), 1);
    assert_eq!(drain(&mut rx).len(), 1);
}

#[test]
fn remove_membership_keeps_a_shared_stream_alive_for_siblings() {
    let (hub, _rx) = hub_with_queue(16);
    hub.add_session(session(1));

    hub.add_membership(1, TURBO_A, "posts:1");
    hub.add_membership(1, TURBO_B, "posts:1");

    assert!(!hub.remove_membership(1, TURBO_A, "posts:1"), "a sibling still holds it");
    assert_eq!(hub.stream_member_count("posts:1"), 1);
    assert!(hub.remove_membership(1, TURBO_B, "posts:1"));
    assert_eq!(hub.stream_count(), 0);
}

#[test]
fn clear_streams_keeps_the_subscription_addressable() {
    let (hub, _rx) = hub_with_queue(16);
    hub.add_session(session(1));
    hub.add_session(session(2));

    hub.add_membership(1, TURBO_A, "posts:1");
    hub.add_membership(1, TURBO_A, "posts:2");
    hub.add_membership(2, TURBO_A, "posts:1");

    // conn 2 still holds posts:1, so only posts:2 is released.
    let mut released = hub.clear_streams(1, TURBO_A);
    released.sort();
    assert_eq!(released, vec!["posts:2".to_string()]);
    assert!(hub.has_subscription(1, TURBO_A), "stop_all_streams is not unsubscribe");
    assert_eq!(hub.deliver("posts:1", PAYLOAD), 1);

    // Clearing twice is a no-op.
    assert_matches!(hub.clear_streams(1, TURBO_A).as_slice(), []);
}

#[test]
fn ensure_subscription_installs_a_streamless_subscription() {
    let (hub, _rx) = hub_with_queue(16);
    hub.add_session(session(1));

    hub.ensure_subscription(1, TURBO_A).expect("session 1 exists");
    assert!(hub.has_subscription(1, TURBO_A));
    assert_eq!(hub.stream_count(), 0);

    // Idempotent, and it never disturbs installed streams.
    hub.add_membership(1, TURBO_A, "posts:1");
    hub.ensure_subscription(1, TURBO_A).expect("session 1 exists");
    assert_eq!(hub.stream_member_count("posts:1"), 1);

    assert_eq!(hub.remove_subscription(1, TURBO_A), vec!["posts:1".to_string()]);
    assert!(!hub.has_subscription(1, TURBO_A));
}

#[tokio::test]
async fn whisper_excludes_the_sending_connection() {
    let (hub, mut rx) = hub_with_queue(16);
    hub.add_session(session(1));
    hub.add_session(session(2));

    hub.add_membership(1, TURBO_A, "chat:1");
    hub.add_membership(2, TURBO_B, "chat:1");

    assert_eq!(hub.deliver_excluding("chat:1", PAYLOAD, 1).await, 1);

    let frames = drain(&mut rx);
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].0, 2);
    assert_eq!(frames[0].1, TURBO_B);
}

#[test]
fn ping_reaches_every_session_unaddressed() {
    let (hub, mut rx) = hub_with_queue(16);
    hub.add_session(session(1));
    hub.add_session(session(2));

    assert_eq!(hub.broadcast_ping(br#"{"type":"ping","message":1}"#), 2);

    let mut conn_ids = Vec::new();
    while let Ok(Outgoing::Cargo(cargo)) = rx.try_recv() {
        let parsed: Value = serde_json::from_slice(&cargo.data).unwrap();
        assert_eq!(parsed["type"], "ping");
        conn_ids.push(cargo.conn_id);
    }
    conn_ids.sort_unstable();
    assert_eq!(conn_ids, vec![1, 2]);
}

// --- joins: backend interest under the per-stream gate ------------------

/// Poll a future exactly once, without a runtime.
fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
    Pin::new(future).poll(&mut Context::from_waker(Waker::noop()))
}

#[tokio::test]
async fn a_member_joining_during_a_pending_acquire_waits_for_its_outcome() {
    // (the first member's acquisition succeeds, runs of the second's)
    for (first_acquired, second_runs) in [(true, 0), (false, 1)] {
        let case = format!("first acquired: {first_acquired}");
        let (hub, _rx) = hub_with_queue(8);
        hub.add_session(session(1));
        hub.add_session(session(2));
        let (ack, acked) = oneshot::channel();
        let second_calls = Cell::new(0);
        // An acquisition that counts how often it ran: the body runs when the
        // join polls it, never when the future is merely built.
        let counted = async |acquired: bool| {
            second_calls.set(second_calls.get() + 1);
            acquired
        };

        let mut first = pin!(hub.join(1, TURBO_A, CHAT, async { acked.await.expect("acked") }));
        let mut second = pin!(hub.join(2, TURBO_A, CHAT, counted(true)));
        assert!(poll_once(&mut first).is_pending(), "{case}: the first member awaits its LISTEN");
        assert!(poll_once(&mut second).is_pending(), "{case}: the second waits at the gate");
        assert_eq!(hub.stream_member_count(CHAT), 1, "{case}: a waiter is not a member yet");

        ack.send(first_acquired).expect("the first join is still pending");
        assert_eq!(first.await.is_ok(), first_acquired, "{case}");
        assert!(second.await.is_ok(), "{case}: the second member joins either way");

        // A refused first member leaves the stream empty, so the waiter
        // becomes the first member and acquires the interest itself.
        assert_eq!(second_calls.get(), second_runs, "{case}");
        assert_eq!(hub.has_subscription(1, TURBO_A), first_acquired, "{case}");
        assert_eq!(hub.stream_member_count(CHAT), 1 + usize::from(first_acquired), "{case}");

        assert!(hub.join(2, TURBO_A, CHAT, counted(false)).await.is_ok(), "{case}");
        assert_eq!(second_calls.get(), second_runs, "{case}: re-joining acquires nothing");
        assert!(hub.stream_gates.is_empty(), "{case}: idle gates are dropped");
    }
}

#[tokio::test]
async fn a_refused_join_keeps_a_subscription_record_it_did_not_create() {
    let (hub, _rx) = hub_with_queue(8);
    hub.add_session(session(1));
    hub.ensure_subscription(1, TURBO_A).expect("session 1 exists");

    assert_matches!(
        hub.join(1, TURBO_A, CHAT, async { false }).await,
        Err(HubError::Refused(stream)) if stream == CHAT
    );
    assert!(hub.has_subscription(1, TURBO_A), "Rails-dispatch records outlive a refused stream");
    assert_eq!(hub.stream_member_count(CHAT), 0);
}

#[test]
fn a_join_dropped_mid_acquire_rolls_back() {
    let (hub, _rx) = hub_with_queue(8);
    hub.add_session(session(1));
    let mut join = Box::pin(hub.join(1, TURBO_A, CHAT, std::future::pending()));
    assert!(poll_once(&mut join).is_pending());
    assert_eq!(hub.stream_member_count(CHAT), 1);

    drop(join);
    assert_eq!(hub.stream_member_count(CHAT), 0, "no member is left without backend interest");
    assert!(!hub.has_subscription(1, TURBO_A));
    assert!(hub.stream_gates.is_empty());
}

// --- explicit sessions, pending buffers, and reuse -------------------------

#[tokio::test]
async fn mutations_for_an_unknown_session_are_refused() {
    let (hub, _rx) = hub_with_queue(8);

    assert_matches!(hub.ensure_subscription(9, TURBO_A), Err(HubError::UnknownSession(9)));
    assert_matches!(hub.set_identifier(9, "user:7".to_string()), Err(HubError::UnknownSession(9)));
    assert_matches!(
        hub.join(9, TURBO_A, CHAT, async { true }).await,
        Err(HubError::UnknownSession(9))
    );
    assert_eq!(hub.stream_count(), 0, "nothing was created for the unknown session");
    assert_eq!(hub.get_connections_by_identifier("user:7"), [] as [u32; 0]);
    assert_eq!(hub.subscription_count(), 0);
}

#[tokio::test]
async fn a_session_torn_down_mid_acquire_tells_the_joiner_to_release() {
    let (hub, _rx) = hub_with_queue(8);
    hub.add_session(session(1));
    let (ack, acked) = oneshot::channel::<bool>();
    let mut join = pin!(hub.join(1, TURBO_A, CHAT, async { acked.await.expect("acked") }));
    assert!(poll_once(&mut join).is_pending());

    // Teardown while the LISTEN is in flight: the hub holds no interest yet,
    // so it reports nothing to release...
    assert_matches!(hub.remove_session(1).as_slice(), []);
    assert_eq!(hub.stream_count(), 0);

    // ...and the join, once acknowledged, hands that release to its caller.
    ack.send(true).expect("join pending");
    assert_matches!(join.await, Err(HubError::UnknownSession(1)));
}

#[test]
fn a_pending_membership_buffers_until_activated_and_flushes_in_order() {
    let (hub, mut rx) = hub_with_queue(16);
    hub.add_session(session(1));
    hub.install(1, TURBO_A, "posts:1").expect("session").expect("installed");

    assert_eq!(hub.deliver("posts:1", br#"{"n":1}"#), 0, "held, not sent");
    assert_eq!(hub.deliver("posts:1", br#"{"n":2}"#), 0);
    assert_eq!(drain_frames(&mut rx), Vec::<Value>::new());

    assert_eq!(hub.activate(1, TURBO_A), 2);
    let flushed: Vec<i64> =
        drain_frames(&mut rx).iter().map(|f| f["message"]["n"].as_i64().unwrap()).collect();
    assert_eq!(flushed, vec![1, 2]);

    // Active from here on; a second activation is a no-op.
    assert_eq!(hub.deliver("posts:1", br#"{"n":3}"#), 1);
    assert_eq!(hub.activate(1, TURBO_A), 0);
}

#[test]
fn a_whisper_skips_pending_members() {
    let (hub, mut rx) = hub_with_queue(16);
    hub.add_session(session(1));
    hub.add_session(session(2));
    hub.add_membership(1, TURBO_A, "chat:1");
    hub.install(2, TURBO_B, "chat:1").expect("session").expect("installed");

    let rt = tokio::runtime::Builder::new_current_thread().build().expect("runtime");
    assert_eq!(rt.block_on(hub.deliver_excluding("chat:1", PAYLOAD, 3)), 1);
    assert_eq!(drain_frames(&mut rx).len(), 1, "an unconfirmed member receives no whisper");
}

#[tokio::test]
async fn a_pending_buffer_overflow_is_lag() {
    let (hub, mut rx) = hub_with_queue(256);
    hub.add_session(session(1));
    hub.install(1, TURBO_A, "posts:1").expect("session").expect("installed");

    // One frame larger than the byte bound trips it at once.
    let big = format!(r#"{{"blob":"{}"}}"#, "x".repeat(PENDING_BUFFER_BYTES));
    assert_eq!(hub.deliver("posts:1", big.as_bytes()), 0);

    let mut kinds = Vec::new();
    for _ in 0..2 {
        match tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv()).await {
            Ok(Some(Outgoing::Cargo(cargo))) => {
                let frame: Value = serde_json::from_slice(&cargo.data).unwrap();
                kinds.push(format!("{}:{}", frame["type"], frame["reconnect"]));
            }
            Ok(Some(Outgoing::Disembark(disembark))) => {
                kinds.push(format!("close:{}", disembark.conn_id));
            }
            other => panic!("expected a lag close, got {other:?}"),
        }
    }
    assert_eq!(kinds, vec!["\"disconnect\":true", "close:1"]);
    assert_eq!(hub.metrics.socket_lag_disconnects(LagReason::PendingBuffer), 1);

    // Activation after an overflow flushes nothing: the connection is going.
    assert_eq!(hub.activate(1, TURBO_A), 0);
}

#[test]
fn a_reused_connection_id_is_cleared_and_counted() {
    let (hub, _rx) = hub_with_queue(8);
    hub.add_session(session(1));
    hub.add_membership(1, TURBO_A, "posts:1");
    hub.add_membership(1, TURBO_B, "posts:2");

    // The same id again, without a teardown in between.
    let mut released_sorted = hub.add_session(session(1));
    released_sorted.sort();
    assert_eq!(released_sorted, vec!["posts:1".to_string(), "posts:2".to_string()]);
    assert_eq!(hub.stream_count(), 0);
    assert_eq!(hub.subscription_count(), 0);
    assert_eq!(hub.session_count(), 1);
    assert_eq!(hub.metrics.hub_queue_overflows(), 0);
    assert_eq!(
        hub.metrics_snapshot_session_reuse(),
        3,
        "two memberships and one session were stale"
    );
}

/// A docking link queue with no room is a loss for the connection whose frame
/// did not fit, so that connection is closed with a reconnectable
/// `disconnect` (and never a silent gap); the frame loss is counted on the
/// docking-only overflow counter, the close on the lag counter.
#[tokio::test]
async fn a_full_docking_link_queue_is_lag_for_that_connection() {
    let (hub, mut rx) = hub_with_queue(1);
    hub.add_session(session(1));
    hub.add_session(session(2));

    assert_eq!(hub.broadcast_ping(br#"{"type":"ping"}"#), 1, "one fits, one does not");
    assert_eq!(hub.metrics.hub_queue_overflows(), 1);
    assert_eq!(hub.metrics.socket_lag_disconnects(LagReason::Queue), 1);

    // Repeating the ping before the close is through costs nothing more:
    // the lagging connection is already closing and is skipped, the other
    // one's frame does not fit behind the close either (queue of one).
    assert_eq!(hub.broadcast_ping(br#"{"type":"ping"}"#), 0);
    assert_eq!(hub.metrics.socket_lag_disconnects(LagReason::Queue), 2, "both lag now");

    // Drain: the one ping, then disconnect + close for each lagged session,
    // in whichever order the two lag-close tasks reach the queue.
    let mut pinged = Vec::new();
    let mut lag_closes: Vec<(u32, String)> = Vec::new();
    while lag_closes.len() < 4 {
        match tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv()).await {
            Ok(Some(Outgoing::Cargo(cargo))) => {
                let frame: Value = serde_json::from_slice(&cargo.data).unwrap();
                if frame["type"] == "ping" {
                    pinged.push(cargo.conn_id);
                } else {
                    assert_eq!(frame["type"], "disconnect");
                    assert_eq!(frame["reconnect"], true);
                    lag_closes.push((cargo.conn_id, "disconnect".to_string()));
                }
            }
            Ok(Some(Outgoing::Disembark(disembark))) => {
                lag_closes.push((disembark.conn_id, "close".to_string()));
            }
            other => panic!("expected the lag closes, got {other:?}"),
        }
    }
    assert_eq!(pinged.len(), 1);
    for conn_id in [1, 2] {
        let sequence: Vec<&str> = lag_closes
            .iter()
            .filter(|(id, _)| *id == conn_id)
            .map(|(_, kind)| kind.as_str())
            .collect();
        assert_eq!(sequence, ["disconnect", "close"], "conn {conn_id}: disconnect precedes close");
    }
}

// MOCKED: an outbound link that lags one connection on purpose and records
// what the hub asks of it. REAL: the hub's lag policy on top of it.
#[derive(Default)]
struct LaggingLink {
    lagging: std::sync::Mutex<HashSet<u32>>,
    sent: std::sync::Mutex<Vec<(u32, Value)>>,
    closes: std::sync::Mutex<Vec<(u32, Value, Disembark)>>,
}

impl LaggingLink {
    fn lag(&self, conn_id: u32) {
        self.lagging.lock().unwrap().insert(conn_id);
    }

    fn sent_to(&self, conn_id: u32) -> Vec<Value> {
        self.sent
            .lock()
            .unwrap()
            .iter()
            .filter(|(id, _)| *id == conn_id)
            .map(|(_, v)| v.clone())
            .collect()
    }

    fn closes(&self) -> Vec<(u32, Value, Disembark)> {
        self.closes.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl OutboundLink for LaggingLink {
    fn try_frame(&self, conn_id: u32, frame: Bytes) -> LinkOutcome {
        if self.lagging.lock().unwrap().contains(&conn_id) {
            return LinkOutcome::Lagged;
        }
        self.sent.lock().unwrap().push((conn_id, serde_json::from_slice(&frame).unwrap()));
        LinkOutcome::Sent
    }

    async fn frame(&self, conn_id: u32, frame: Bytes) -> LinkOutcome {
        self.try_frame(conn_id, frame)
    }

    async fn disconnect(&self, close: Disembark) {
        self.closes.lock().unwrap().push((close.conn_id, Value::Null, close));
    }

    fn lag_close(&self, disconnect: Bytes, close: Disembark) {
        let frame = serde_json::from_slice(&disconnect).unwrap();
        self.closes.lock().unwrap().push((close.conn_id, frame, close));
    }
}

fn hub_with_lagging_link() -> (Hub, Arc<LaggingLink>) {
    let link = Arc::new(LaggingLink::default());
    let hub = Hub::with_link(Arc::clone(&link) as Arc<dyn OutboundLink>, Arc::new(Metrics::new()));
    (hub, link)
}

/// Invariant 5 at the hub: a socket whose own queue refuses a frame is not
/// left with a silent gap - it is sent the reconnectable `disconnect` and
/// closed, exactly once, counted once, and receives nothing further; the
/// other members of the stream are unaffected.
#[tokio::test]
async fn a_lagged_socket_receives_the_close_rather_than_a_dropped_frame() {
    let (hub, link) = hub_with_lagging_link();
    hub.add_session(session(1));
    hub.add_session(session(2));
    hub.add_membership(1, TURBO_A, "posts:1");
    hub.add_membership(2, TURBO_A, "posts:1");

    link.lag(2);
    assert_eq!(hub.deliver("posts:1", br#"{"n":1}"#), 1, "the healthy member is delivered");
    assert_eq!(hub.deliver("posts:1", br#"{"n":2}"#), 1);
    hub.send(2, br#"{"type":"transmit"}"#).await;
    assert_eq!(hub.broadcast_ping(br#"{"type":"ping"}"#), 1);

    assert_eq!(link.sent_to(1).len(), 3);
    assert_eq!(link.sent_to(2), Vec::<Value>::new(), "a closing socket receives nothing more");

    let closes = link.closes();
    assert_eq!(closes.len(), 1, "closed once, however many frames it refused");
    let (conn_id, disconnect, close) = &closes[0];
    assert_eq!(*conn_id, 2);
    assert_eq!(disconnect["type"], "disconnect");
    assert_eq!(disconnect["reconnect"], true);
    assert_eq!((close.code, close.reason.as_str()), (CLOSE_NORMAL, CLOSE_REASON_LAG));
    assert_eq!(hub.metrics.socket_lag_disconnects(LagReason::Queue), 1, "counted once");
    assert_eq!(hub.metrics.hub_queue_overflows(), 0, "no shared queue was involved");

    // Teardown clears the mark: the reused id starts clean and can lag again.
    hub.remove_session(2);
    hub.add_session(session(2));
    hub.add_membership(2, TURBO_A, "posts:1");
    assert_eq!(hub.deliver("posts:1", br#"{"n":3}"#), 1);
    assert_eq!(link.closes().len(), 2);
    assert_eq!(hub.metrics.socket_lag_disconnects(LagReason::Queue), 2);
}

/// A gap close (`disconnect_members`) and a lag close for the same
/// connection collapse into one close, and a lag after a gap close is not
/// counted as lag: the connection was already going.
#[tokio::test]
async fn a_gap_close_and_a_lag_close_for_one_connection_collapse() {
    let (hub, link) = hub_with_lagging_link();
    hub.add_session(session(1));
    hub.add_membership(1, TURBO_A, "posts:1");

    assert_eq!(hub.disconnect_members("posts:1"), 1);
    link.lag(1);
    assert_eq!(hub.deliver("posts:1", PAYLOAD), 0);

    assert_eq!(link.closes().len(), 1);
    assert_eq!(hub.metrics.socket_lag_disconnects(LagReason::Queue), 0);
}
