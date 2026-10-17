//! Rails dispatch: the session reducer
//!
//! These drive `SessionDispatch::apply` directly with synthesized replies.
//! The RPC endpoint they are configured with is never reached: the reducer
//! is the part under test, and it must be provably correct without a Rails.

use super::*;
use crate::config::RpcSettings;
use crate::protocol::v1::rpc::{CLOSE_REASON_UNAUTHORIZED, Outcome};
use crate::rpc_client::{RpcClient, RpcConfig};
use crate::streams::SecretBytes;

const IDENTIFIER: &str = r#"{"channel":"ChatChannel","room":"42"}"#;
const OTHER: &str = r#"{"channel":"ChatChannel","room":"43"}"#;

fn dispatch_config() -> Config {
    let mut config = test_config();
    config.streams = crate::streams::StreamsConfig::default();
    config.rpc = RpcSettings {
        endpoint_url: Some("http://127.0.0.1:1/_orbitcast/v1/rails".to_string()),
        token: Some(SecretBytes::new(b"token".to_vec())),
        deadline: Duration::from_millis(200),
        mailbox_capacity: 2,
        ..RpcSettings::default()
    };
    config
}

fn context(
    backend: Arc<Backend>,
    hub: Arc<Hub>,
    presence: Option<Arc<dyn PresenceBackend>>,
) -> Arc<DispatchContext> {
    let rpc = Arc::new(
        RpcClient::new(RpcConfig::new(
            "http://127.0.0.1:1/_orbitcast/v1/rails",
            SecretBytes::new(b"token".to_vec()),
        ))
        .expect("client builds"),
    );
    let (cleanup_tx, _cleanup_rx) = mpsc::channel(8);

    Arc::new(DispatchContext {
        hub,
        backend,
        rpc,
        streams: None,
        recovery: None,
        namespace: "test".to_string(),
        node_id: "node-test".to_string(),
        deadline_ms: 1_000,
        settings: RpcSettings::default(),
        presence,
        presence_sessions: Arc::new(PresenceSessions::new()),
        presence_ttl: Duration::from_millis(crate::presence::DEFAULT_PRESENCE_TTL_MS),
        cleanup_tx,
    })
}

/// A session with one accepted subscription and nothing installed yet.
fn session(hub: &Arc<Hub>, backend: Arc<Backend>) -> SessionDispatch {
    session_with_presence(hub, backend, None)
}

/// [`session`], on a node whose distributed presence store is `presence`.
fn session_with_presence(
    hub: &Arc<Hub>,
    backend: Arc<Backend>,
    presence: Option<Arc<dyn PresenceBackend>>,
) -> SessionDispatch {
    let mut session = SessionDispatch::new(
        context(backend, Arc::clone(hub), presence),
        1,
        Arc::new(SessionGate::new(1)),
        "/cable?tab=1".to_string(),
        "127.0.0.1".to_string(),
        &HashMap::new(),
    );
    session.identified = true;
    session.sequence = 1;
    session.subscriptions.insert(IDENTIFIER.to_string(), SubscriptionRecord::default());
    hub.ensure_subscription(1, IDENTIFIER).expect("session 1 exists");
    session
}

fn parts() -> (Arc<Hub>, Arc<Backend>, mpsc::Receiver<Outgoing>) {
    let (tx, rx) = mpsc::channel(64);
    let hub = Arc::new(Hub::new(tx));
    hub.add_session(Session::new(1, "/cable".to_string()));
    (hub, Arc::new(Backend::with_pubsub(ScriptedPubSub::new())), rx)
}

/// A reply that answers the session's current sequence.
fn reply(session: &SessionDispatch, effects: Vec<Effect>) -> RpcReply {
    RpcReply {
        version: 1,
        operation: Operation::OpenChannel,
        outcome: Outcome::Accepted,
        request_id: uuid::Uuid::new_v4().to_string(),
        sequence: session.sequence,
        new_revision: session.revision + 1,
        state: StateReplacement::Unchanged,
        subscription_state: StateReplacement::Unchanged,
        identity: Map::new(),
        effects,
        reason: None,
        describe: None,
    }
}

#[tokio::test]
async fn add_stream_effects_install_memberships_and_bump_the_revision() {
    let (hub, backend, mut rx) = parts();
    let mut session = session(&hub, backend);

    let reply = reply(
        &session,
        vec![
            Effect::AddStream { identifier: IDENTIFIER.to_string(), stream: "chat:42".to_string() },
            Effect::AddStream {
                identifier: IDENTIFIER.to_string(),
                stream: "chat:all".to_string(),
            },
        ],
    );
    session.apply(&reply, Some(IDENTIFIER)).await.expect("applied");

    assert_eq!(session.revision, 1);
    assert_eq!(hub.stream_member_count("chat:42"), 1);
    assert_eq!(hub.stream_member_count("chat:all"), 1);
    assert_eq!(session.subscriptions[IDENTIFIER].streams.len(), 2);

    // Readiness barrier: a delivery before the confirmation is held back,
    // and released in order once the subscription is activated.
    hub.deliver("chat:42", br#"{"body":"early"}"#);
    assert!(frames(&mut rx).is_empty(), "nothing precedes the confirmation");
    assert_eq!(hub.activate(1, IDENTIFIER), 1);
    let sent = frames(&mut rx);
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0]["message"]["body"], "early");

    // Delivery is addressed with the client's own identifier.
    hub.deliver("chat:42", br#"{"body":"hi"}"#);
    let sent = frames(&mut rx);
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0]["identifier"], IDENTIFIER);
}

#[tokio::test]
async fn a_reply_arriving_after_teardown_is_stale() {
    let (hub, backend, _rx) = parts();
    let mut session = session(&hub, backend);

    let reply = reply(
        &session,
        vec![Effect::AddStream {
            identifier: IDENTIFIER.to_string(),
            stream: "chat:42".to_string(),
        }],
    );
    // The driver tore the session down while the call was in flight.
    session.gate.close();
    hub.remove_session(1);

    assert_eq!(session.apply(&reply, Some(IDENTIFIER)).await, Err(ApplyError::Stale));
    assert_eq!(hub.stream_count(), 0, "a stale reply installs nothing");
    assert_eq!(session.revision, 0);
}

#[tokio::test]
async fn remove_stream_and_remove_all_streams_release_interest() {
    let (hub, backend, _rx) = parts();
    let mut session = session(&hub, backend);

    let install = reply(
        &session,
        vec![
            Effect::AddStream { identifier: IDENTIFIER.to_string(), stream: "chat:42".to_string() },
            Effect::AddStream {
                identifier: IDENTIFIER.to_string(),
                stream: "chat:all".to_string(),
            },
        ],
    );
    session.apply(&install, Some(IDENTIFIER)).await.expect("applied");

    session.sequence += 1;
    let remove_one = reply(
        &session,
        vec![Effect::RemoveStream {
            identifier: IDENTIFIER.to_string(),
            stream: "chat:42".to_string(),
        }],
    );
    session.apply(&remove_one, Some(IDENTIFIER)).await.expect("applied");
    assert_eq!(hub.stream_member_count("chat:42"), 0);
    assert_eq!(hub.stream_member_count("chat:all"), 1);

    session.sequence += 1;
    let remove_all =
        reply(&session, vec![Effect::RemoveAllStreams { identifier: IDENTIFIER.to_string() }]);
    session.apply(&remove_all, Some(IDENTIFIER)).await.expect("applied");
    assert_eq!(hub.stream_count(), 0);
    assert!(session.subscriptions[IDENTIFIER].streams.is_empty());
    assert!(
        hub.has_subscription(1, IDENTIFIER),
        "stop_all_streams keeps the subscription addressable"
    );
}

#[tokio::test]
async fn transmit_effects_preserve_the_payload_bytes() {
    let (hub, backend, mut rx) = parts();
    let mut session = session(&hub, backend);

    let payload = r#"{"z":1,  "a":[1,2,3]}"#;
    let reply = reply(
        &session,
        vec![Effect::Transmit {
            identifier: IDENTIFIER.to_string(),
            json_payload: payload.to_string(),
        }],
    );
    session.apply(&reply, Some(IDENTIFIER)).await.expect("applied");

    let Some(Outgoing::Cargo(cargo)) = rx.recv().await else {
        panic!("a transmit frame was expected");
    };
    let text = std::str::from_utf8(&cargo.data).expect("utf-8");
    assert_eq!(text, format!(r#"{{"identifier":{IDENTIFIER:?},"message":{payload}}}"#));
}

#[tokio::test]
async fn effects_for_an_unowned_identifier_are_rejected_before_anything_is_applied() {
    let (hub, backend, _rx) = parts();
    let mut session = session(&hub, backend);

    let reply = reply(
        &session,
        vec![
            Effect::AddStream { identifier: IDENTIFIER.to_string(), stream: "chat:42".to_string() },
            // A second effect naming somebody else's subscription.
            Effect::AddStream { identifier: OTHER.to_string(), stream: "chat:43".to_string() },
        ],
    );

    assert_eq!(session.apply(&reply, Some(IDENTIFIER)).await, Err(ApplyError::Unowned));
    assert_eq!(hub.stream_count(), 0, "ownership is checked before anything is installed");
    assert_eq!(session.revision, 0, "a rejected reply does not move the revision");
}

#[tokio::test]
async fn a_stale_reply_is_discarded() {
    let (hub, backend, _rx) = parts();
    let mut session = session(&hub, backend);

    // A reply for an earlier command.
    let mut stale = reply(&session, Vec::new());
    stale.sequence = session.sequence - 1;
    assert_eq!(session.apply(&stale, Some(IDENTIFIER)).await, Err(ApplyError::Stale));

    // A reply whose revision moved backwards.
    session.revision = 7;
    let mut backwards = reply(&session, Vec::new());
    backwards.new_revision = 6;
    assert_eq!(session.apply(&backwards, Some(IDENTIFIER)).await, Err(ApplyError::Stale));
    assert_eq!(session.revision, 7);
}

#[tokio::test]
async fn a_refused_backend_rolls_every_installed_stream_back() {
    let (hub, _backend, _rx) = parts();
    let pubsub = Arc::new(MemoryPubSub::new());
    let backend = Arc::new(Backend::with_pubsub(Arc::clone(&pubsub) as Arc<dyn PubSub>));
    let mut session = session(&hub, Arc::clone(&backend));

    // The second stream name is past the backend's own limit, so
    // `Backend::acquire` refuses it. This reply is deliberately one the
    // envelope validator would already have rejected: the reducer must
    // still be safe when the readiness barrier says no for any reason.
    let refused = "x".repeat(crate::protocol::v1::MAX_STREAM_LEN + 1);
    let reply = reply(
        &session,
        vec![
            Effect::AddStream { identifier: IDENTIFIER.to_string(), stream: "chat:ok".to_string() },
            Effect::AddStream { identifier: IDENTIFIER.to_string(), stream: refused },
        ],
    );

    // The *second* stream is refused, so the first must be rolled back
    // rather than left as a membership Rails does not know about.
    assert_eq!(session.apply(&reply, Some(IDENTIFIER)).await, Err(ApplyError::Backend));
    assert_eq!(hub.stream_count(), 0);
    assert!(!pubsub.is_subscribed("chat:ok"));
    assert_eq!(backend.interest_count("chat:ok"), 0, "the rolled-back stream holds no interest");
    assert_eq!(session.revision, 0);
}

#[tokio::test]
async fn state_replacements_follow_the_absent_null_empty_rules() {
    let (hub, backend, _rx) = parts();
    let mut session = session(&hub, backend);

    let mut set = reply(&session, Vec::new());
    set.state =
        StateReplacement::Replace(Map::from_iter([("cart_id".to_string(), Value::from(7))]));
    set.subscription_state =
        StateReplacement::Replace(Map::from_iter([("room".to_string(), Value::from("42"))]));
    session.apply(&set, Some(IDENTIFIER)).await.expect("applied");
    assert_eq!(session.connection.state["cart_id"], 7);
    assert_eq!(session.subscriptions[IDENTIFIER].state["room"], "42");

    // Unchanged (absent or null on the wire) leaves both alone.
    session.sequence += 1;
    let unchanged = reply(&session, Vec::new());
    session.apply(&unchanged, Some(IDENTIFIER)).await.expect("applied");
    assert_eq!(session.connection.state["cart_id"], 7);
    assert_eq!(session.subscriptions[IDENTIFIER].state["room"], "42");

    // An empty object clears.
    session.sequence += 1;
    let mut cleared = reply(&session, Vec::new());
    cleared.state = StateReplacement::clear();
    cleared.subscription_state = StateReplacement::clear();
    session.apply(&cleared, Some(IDENTIFIER)).await.expect("applied");
    assert!(session.connection.state.is_empty());
    assert!(session.subscriptions[IDENTIFIER].state.is_empty());
}

#[tokio::test]
async fn a_close_effect_closes_after_the_earlier_effects_are_queued() {
    let (hub, backend, mut rx) = parts();
    let mut session = session(&hub, backend);

    let reply = reply(
        &session,
        vec![
            Effect::Transmit {
                identifier: IDENTIFIER.to_string(),
                json_payload: r#"{"bye":true}"#.to_string(),
            },
            Effect::Close { reason: CLOSE_REASON_UNAUTHORIZED.to_string(), reconnect: false },
        ],
    );
    session.apply(&reply, Some(IDENTIFIER)).await.expect("applied");

    assert!(session.closed);
    assert_eq!(session.close_reason, (CLOSE_REASON_UNAUTHORIZED, false));

    // Order on the single outbound queue: payload, disconnect, close.
    let mut kinds = Vec::new();
    while let Ok(message) = rx.try_recv() {
        match message {
            Outgoing::Cargo(cargo) => {
                let frame: serde_json::Value = serde_json::from_slice(&cargo.data).expect("json");
                kinds.push(frame["type"].as_str().unwrap_or("message").to_string());
            }
            Outgoing::Disembark(_) => kinds.push("close".to_string()),
        }
    }
    assert_eq!(kinds, vec!["message", "disconnect", "close"]);
}

mod presence;

// --- mailbox wiring --------------------------------------------------

#[tokio::test]
async fn dispatch_mode_sends_no_welcome_before_identification() {
    let (tx, mut rx) = mpsc::channel(64);
    let backend = Arc::new(Backend::with_pubsub(ScriptedPubSub::new()));
    let connections = Connections::new(backend, &dispatch_config(), tx).expect("driver");
    assert!(connections.is_rails_dispatch());

    connections.accept(1, "/cable".into(), "127.0.0.1".into(), HashMap::new()).await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    // The stub endpoint is unreachable, so identification fails: the
    // session is closed with `reconnect: true` and never welcomed.
    let sent = frames(&mut rx);
    assert!(
        !sent.iter().any(|frame| frame["type"] == "welcome"),
        "welcome must wait for an accepted identify: {sent:?}"
    );
}

#[tokio::test]
async fn an_unreachable_rails_closes_with_reconnect_true() {
    let (tx, mut rx) = mpsc::channel(64);
    let backend = Arc::new(Backend::with_pubsub(ScriptedPubSub::new()));
    let connections = Connections::new(backend, &dispatch_config(), tx).expect("driver");

    connections.accept(1, "/cable".into(), "127.0.0.1".into(), HashMap::new()).await;

    let disconnect = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(Outgoing::Cargo(cargo)) = rx.recv().await {
                let frame: serde_json::Value = serde_json::from_slice(&cargo.data).expect("json");
                if frame["type"] == "disconnect" {
                    return frame;
                }
            }
        }
    })
    .await
    .expect("a disconnect frame arrives");

    assert_eq!(disconnect["reason"], CLOSE_REASON_SERVER_RESTART);
    assert_eq!(
        disconnect["reconnect"], true,
        "a backend outage invites a reconnect; it never accepts locally"
    );
}

#[tokio::test]
async fn a_full_mailbox_closes_the_session_with_reconnect_true() {
    let (tx, mut rx) = mpsc::channel(64);
    let backend = Arc::new(Backend::with_pubsub(ScriptedPubSub::new()));
    let mut config = dispatch_config();
    // One slot, already taken by the queued `identify`, and a session
    // task that is stuck on an unreachable endpoint.
    config.rpc.mailbox_capacity = 1;
    config.rpc.deadline = Duration::from_secs(5);
    let connections = Connections::new(backend, &config, tx).expect("driver");

    connections.accept(1, "/cable".into(), "127.0.0.1".into(), HashMap::new()).await;
    for index in 0..4 {
        connections
            .handle_command(
                1,
                ClientCommand::Subscribe { identifier: format!("{{\"n\":{index}}}") },
            )
            .await;
    }

    let frames = frames(&mut rx);
    let disconnect = frames
        .iter()
        .find(|frame| frame["type"] == "disconnect")
        .expect("an overloaded session is closed");
    assert_eq!(disconnect["reason"], CLOSE_REASON_SERVER_RESTART);
    assert_eq!(disconnect["reconnect"], true);
}

#[tokio::test]
async fn teardown_drops_the_mailbox() {
    let (tx, _rx) = mpsc::channel(64);
    let backend = Arc::new(Backend::with_pubsub(ScriptedPubSub::new()));
    let connections = Connections::new(backend, &dispatch_config(), tx).expect("driver");

    connections.accept(1, "/cable".into(), "127.0.0.1".into(), HashMap::new()).await;
    assert_eq!(connections.task_count(), 1);

    connections.teardown(1).await;
    assert_eq!(connections.task_count(), 0);
    assert_eq!(connections.session_count(), 0);
}

#[tokio::test]
async fn a_refused_rails_endpoint_admits_no_dispatch_connection() {
    let (tx, mut rx) = mpsc::channel(64);
    let backend = Arc::new(Backend::with_pubsub(ScriptedPubSub::new()));
    let connections =
        Connections::new(Arc::clone(&backend), &dispatch_config(), tx).expect("driver");
    backend.set_rails_status(RailsStatus::Refused(DescribeRefusal::MissingCapability));

    connections.accept(1, "/cable".into(), "127.0.0.1".into(), HashMap::new()).await;

    let frames = frames(&mut rx);
    let disconnect = frames.iter().find(|frame| frame["type"] == "disconnect").expect("refused");
    assert_eq!(
        disconnect["reconnect"], true,
        "a refused endpoint is temporary from the client's view"
    );
    assert!(!backend.is_ready());
    assert_eq!(backend.readiness(false).rails, Some("missing_capability"));
}

#[test]
fn identity_keys_prefer_an_explicit_attribute() {
    assert_eq!(identity_key(&Map::new()), None, "anonymous is not addressable");

    let explicit = Map::from_iter([("identity_key".to_string(), Value::from("user:7"))]);
    assert_eq!(identity_key(&explicit).as_deref(), Some("user:7"));

    // Without one, the canonical (key-sorted) JSON of the map is used.
    let derived = Map::from_iter([
        ("current_user".to_string(), Value::from("gid://app/User/7")),
        ("account".to_string(), Value::from(3)),
    ]);
    assert_eq!(
        identity_key(&derived).as_deref(),
        Some(r#"{"account":3,"current_user":"gid://app/User/7"}"#)
    );
}
