use std::assert_matches;
use std::time::Duration;

use serde_json::Value;
use tokio::sync::mpsc;

use super::*;
use crate::broker::test_support::FakeBroker;
use crate::session::Session;
use crate::transport::docking::Outgoing;

const NAMESPACE: &str = "example_production";
const SECRET: &[u8] = b"recovery-test-secret-32-bytes-000";
const NOW_PLUS: i64 = 3_600;

/// The identifier of the ordinary application subscription whose membership
/// authorizes a replay. Any identifier does; the hub keys memberships by it.
const APP_IDENTIFIER: &str = r#"{"channel":"ChatChannel","room":"42"}"#;

/// Interest that always succeeds, recording what was asked for.
#[derive(Default)]
struct RecordingInterest {
    acquired: Mutex<Vec<String>>,
    released: Mutex<Vec<String>>,
    refuse: AtomicBool,
}

#[async_trait]
impl StreamInterest for RecordingInterest {
    async fn acquire(&self, stream: &str) -> bool {
        if self.refuse.load(Ordering::Relaxed) {
            return false;
        }
        self.acquired.lock().unwrap().push(stream.to_string());
        true
    }

    async fn release(&self, streams: Vec<String>) {
        self.released.lock().unwrap().extend(streams);
    }
}

struct Harness {
    controller: Arc<RecoveryController>,
    broker: Arc<FakeBroker>,
    hub: Arc<Hub>,
    interest: Arc<RecordingInterest>,
    frames: mpsc::Receiver<Outgoing>,
    token: String,
}

fn config() -> BrokerConfig {
    BrokerConfig {
        enabled: true,
        // Short enough that a test never waits on it, long enough that it
        // is not the reason a test passes.
        reconcile_interval: Duration::from_millis(20),
        ..BrokerConfig::default()
    }
}

fn verifier() -> RecoveryVerifier {
    RecoveryVerifier::new(SecretBytes::new(SECRET.to_vec()))
}

fn claims_for(session_id: &str, streams: &[&str], expires_at: i64) -> RecoveryClaims {
    RecoveryClaims::recover(
        NAMESPACE,
        session_id,
        streams.iter().map(|s| (*s).to_string()).collect(),
        expires_at,
    )
}

fn token_for(streams: &[&str]) -> String {
    verifier().sign(&claims_for("session:7", streams, now_unix() + NOW_PLUS))
}

fn identifier_for(token: &str) -> String {
    serde_json::json!({ "channel": RECOVERY_CHANNEL, "token": token }).to_string()
}

/// Install `conn_id` as an active member of `streams`, the way a confirmed
/// application subscription would.
async fn subscribe_streams(hub: &Hub, conn_id: u32, streams: &[&str]) {
    for stream in streams {
        hub.join(conn_id, APP_IDENTIFIER, stream, async { true }).await.expect("membership");
    }
    hub.activate(conn_id, APP_IDENTIFIER);
}

/// A hub with one session per `conn_ids`, subscribed to `streams`.
async fn hub_with(conn_ids: &[u32], streams: &[&str]) -> (Arc<Hub>, mpsc::Receiver<Outgoing>) {
    let (tx, frames) = mpsc::channel(4096);
    let hub = Arc::new(Hub::new(tx));
    for conn_id in conn_ids {
        hub.add_session(Session::new(*conn_id, "/cable".into()));
        subscribe_streams(&hub, *conn_id, streams).await;
    }
    (hub, frames)
}

fn controller_with(
    broker: &Arc<FakeBroker>,
    config: BrokerConfig,
    hub: &Arc<Hub>,
    interest: &Arc<RecordingInterest>,
) -> Arc<RecoveryController> {
    RecoveryController::new(
        NAMESPACE.to_string(),
        SecretBytes::new(SECRET.to_vec()),
        Arc::clone(broker) as Arc<dyn Broker>,
        config,
        Arc::clone(hub),
        Arc::new(StreamWaker::new()),
        Arc::clone(interest) as Arc<dyn StreamInterest>,
    )
}

/// One identified connection (id 1), subscribed to `chat:42`, with a token
/// for exactly that stream.
async fn harness_with(broker: Arc<FakeBroker>, config: BrokerConfig) -> Harness {
    let (hub, frames) = hub_with(&[1], &["chat:42"]).await;
    let interest = Arc::new(RecordingInterest::default());
    let controller = controller_with(&broker, config, &hub, &interest);
    let token = token_for(&["chat:42"]);
    Harness { controller, broker, hub, interest, frames, token }
}

async fn harness() -> Harness {
    harness_with(FakeBroker::new(), config()).await
}

/// Collect every frame currently queued, decoded as JSON.
fn drain(frames: &mut mpsc::Receiver<Outgoing>) -> Vec<Value> {
    let mut out = Vec::new();
    while let Ok(Outgoing::Cargo(cargo)) = frames.try_recv() {
        out.push(serde_json::from_slice(&cargo.data).expect("frames are JSON"));
    }
    out
}

/// Wait until `predicate` holds over the accumulated frames, or fail.
async fn collect_until(
    frames: &mut mpsc::Receiver<Outgoing>,
    collected: &mut Vec<Value>,
    what: &str,
    predicate: impl Fn(&[Value]) -> bool,
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        collected.extend(drain(frames));
        if predicate(collected) {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}: {collected:#?}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Give the tails `quiet` to act, then collect whatever they produced.
async fn settle(
    frames: &mut mpsc::Receiver<Outgoing>,
    collected: &mut Vec<Value>,
    quiet: Duration,
) {
    tokio::time::sleep(quiet).await;
    collected.extend(drain(frames));
}

/// The `message` bodies of every frame.
fn messages(frames: &[Value]) -> impl Iterator<Item = &Value> {
    frames.iter().filter_map(|frame| frame.get("message"))
}

fn deliveries(frames: &[Value], stream: &str) -> Vec<(u64, String)> {
    messages(frames)
        .filter(|message| message["type"] == "orbitcast_delivery" && message["stream"] == stream)
        .map(|message| {
            (
                message["sequence"].as_str().unwrap().parse::<u64>().unwrap(),
                message["publication_id"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

/// Delivered sequence numbers for `stream`, in arrival order.
fn sequences(frames: &[Value], stream: &str) -> Vec<u64> {
    deliveries(frames, stream).iter().map(|(sequence, _)| *sequence).collect()
}

fn events(frames: &[Value]) -> Vec<String> {
    messages(frames)
        .filter(|message| message["type"] == "orbitcast_recovery")
        .map(|message| message["event"].as_str().unwrap().to_string())
        .collect()
}

fn gap_reasons(frames: &[Value]) -> Vec<String> {
    messages(frames)
        .filter(|message| message["event"] == "gap")
        .map(|message| message["reason"].as_str().unwrap().to_string())
        .collect()
}

fn gaps_for(frames: &[Value], stream: &str) -> Vec<String> {
    messages(frames)
        .filter(|message| message["event"] == "gap" && message["stream"] == stream)
        .map(|message| message["reason"].as_str().unwrap().to_string())
        .collect()
}

async fn subscribe(controller: &RecoveryController, conn_id: u32, identifier: &str) {
    controller
        .handle(conn_id, ClientCommand::Subscribe { identifier: identifier.to_string() }, true)
        .await;
}

async fn perform(controller: &RecoveryController, identifier: &str, data: &str) {
    perform_as(controller, 1, identifier, data, true).await;
}

async fn perform_as(
    controller: &RecoveryController,
    conn_id: u32,
    identifier: &str,
    data: &str,
    welcomed: bool,
) {
    controller
        .handle(
            conn_id,
            ClientCommand::Message { identifier: identifier.to_string(), data: data.to_string() },
            welcomed,
        )
        .await;
}

fn resume_data(cursors: &[(&str, Option<Cursor>)]) -> String {
    let mut command = RecoveryCommand::new(RecoveryOp::Resume);
    for (stream, cursor) in cursors {
        if let Some(cursor) = cursor {
            command = command.with_cursor(*stream, cursor);
        }
    }
    serde_json::to_string(&command).unwrap()
}

/// Subscribe connection 1 and resume it from `cursors`, returning the frames
/// up to and including `replay_done`.
async fn subscribe_and_resume(
    harness: &mut Harness,
    cursors: &[(&str, Option<Cursor>)],
) -> Vec<Value> {
    let identifier = identifier_for(&harness.token);
    subscribe(&harness.controller, 1, &identifier).await;
    drain(&mut harness.frames);

    perform(&harness.controller, &identifier, &resume_data(cursors)).await;

    let mut frames = Vec::new();
    collect_until(&mut harness.frames, &mut frames, "replay_done", |frames| {
        events(frames).contains(&"replay_done".to_string())
    })
    .await;
    frames
}

// --- token verification -------------------------------------------------

#[test]
fn the_recovery_verifier_round_trips_its_own_tokens() {
    let claims = claims_for("session:7", &["chat:42"], now_unix() + NOW_PLUS);
    let token = verifier().sign(&claims);
    let verified = verifier().verify(&token, NAMESPACE, now_unix()).unwrap();
    assert_eq!(verified, claims);
}

#[test]
fn the_recovery_verifier_refuses_another_profiles_key_and_purpose() {
    let claims = claims_for("s", &["chat:42"], now_unix() + NOW_PLUS);
    let token = verifier().sign(&claims);

    // The streams secret can never verify a recovery token.
    let other = RecoveryVerifier::new(SecretBytes::new(b"streams-secret".to_vec()));
    assert_matches!(
        other.verify(&token, NAMESPACE, now_unix()).unwrap_err(),
        VerifyError::InvalidSignature
    );

    // A correctly signed token for the wrong namespace is still refused.
    assert_matches!(
        verifier().verify(&token, "other_app", now_unix()).unwrap_err(),
        VerifyError::NamespaceMismatch { .. }
    );

    // And a subscribe-purpose token signed with the recovery key is not a
    // recovery capability.
    let mut wrong_purpose = claims.clone();
    wrong_purpose.purpose = "subscribe".to_string();
    let token = verifier().sign(&wrong_purpose);
    assert_matches!(
        verifier().verify(&token, NAMESPACE, now_unix()).unwrap_err(),
        VerifyError::Claims(_)
    );

    // Expired, tampered, and malformed tokens all fail closed.
    let expired = claims_for("s", &["chat:42"], 1);
    assert!(verifier().verify(&verifier().sign(&expired), NAMESPACE, now_unix()).is_err());

    let token = verifier().sign(&claims);
    let (encoded, _) = token.split_once("--").unwrap();
    assert_matches!(
        verifier()
            .verify(&format!("{encoded}--{}", "0".repeat(64)), NAMESPACE, now_unix())
            .unwrap_err(),
        VerifyError::InvalidSignature
    );
    assert_matches!(
        verifier().verify("no-separator", NAMESPACE, now_unix()).unwrap_err(),
        VerifyError::InvalidFormat
    );
}

/// The shared token fixtures, through the real verifier, against the
/// *deployment* namespace a node is configured with: a token minted for
/// another deployment verifies as a token but is refused by this one.
#[test]
fn the_token_fixtures_are_scoped_to_the_deployment_namespace() {
    let doc: Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/recovery/tokens.json"
    )))
    .unwrap();

    let mut foreign = 0;
    for case in doc["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let input = &case["input"];
        let expect = &case["expect"];
        let token = input["token"].as_str().unwrap();
        let secret = input["secret"].as_str().unwrap();
        let now = input["now"].as_i64().unwrap();
        let verifier = RecoveryVerifier::new(SecretBytes::new(secret.as_bytes().to_vec()));

        // Every case names the namespace the token was minted for ...
        let minted_for = expect["claims"]["namespace"].as_str().unwrap();
        let outcome = verifier.verify(token, minted_for, now);
        assert_eq!(outcome.is_ok(), expect["valid"].as_bool().unwrap(), "{name}");

        // ... and a foreign-deployment case additionally names the deployment
        // that refuses it, and why.
        let Some(deployment) = input.get("verify_namespace").and_then(Value::as_str) else {
            continue;
        };
        foreign += 1;
        assert_ne!(deployment, minted_for, "{name}: the case must cross a namespace");
        let err = verifier.verify(token, deployment, now).expect_err(name);
        assert_matches!(err, VerifyError::NamespaceMismatch { .. }, "{name}");
        assert_eq!(expect["verify_error_kind"].as_str().unwrap(), "namespace_mismatch", "{name}");
    }
    assert!(foreign > 0, "the fixtures must carry a wrong-namespace token");
}

// --- routing ------------------------------------------------------------

#[tokio::test]
async fn only_recovery_identifiers_are_routed_here() {
    let harness = harness().await;
    let recovery = ClientCommand::Subscribe { identifier: identifier_for(&harness.token) };
    assert!(harness.controller.handles(&recovery));

    for identifier in [
        r#"{"channel":"Orbitcast::StreamsChannel","signed_stream_name":"x"}"#,
        r#"{"channel":"Turbo::StreamsChannel","signed_stream_name":"x"}"#,
        "not json",
        "{}",
    ] {
        let command = ClientCommand::Subscribe { identifier: identifier.to_string() };
        assert!(!harness.controller.handles(&command), "{identifier} must not be ours");
    }
}

#[tokio::test]
async fn a_valid_token_confirms_and_holds_interest() {
    let mut harness = harness().await;
    let identifier = identifier_for(&harness.token);
    subscribe(&harness.controller, 1, &identifier).await;

    let frames = drain(&mut harness.frames);
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0]["type"], "confirm_subscription");
    assert_eq!(frames[0]["identifier"], identifier);
    assert_eq!(*harness.interest.acquired.lock().unwrap(), vec!["chat:42".to_string()]);
    assert_eq!(harness.controller.session_count(), 1);

    // Teardown releases exactly what the token granted.
    let released = harness.controller.forget(1);
    assert_eq!(released, vec!["chat:42".to_string()]);
    assert_eq!(harness.controller.session_count(), 0);
}

#[tokio::test]
async fn an_unusable_token_is_rejected_without_holding_interest() {
    for token in [
        String::new(),
        "garbage".to_string(),
        // Signed with the wrong key.
        RecoveryVerifier::new(SecretBytes::new(b"another-key".to_vec())).sign(&claims_for(
            "s",
            &["chat:42"],
            now_unix() + NOW_PLUS,
        )),
        // Correctly signed, but already expired.
        verifier().sign(&claims_for("s", &["chat:42"], 1)),
    ] {
        let mut harness = harness().await;
        subscribe(&harness.controller, 1, &identifier_for(&token)).await;

        let frames = drain(&mut harness.frames);
        assert_eq!(frames[0]["type"], "reject_subscription", "token {token:?}");
        assert!(harness.interest.acquired.lock().unwrap().is_empty());
        assert_eq!(harness.controller.session_count(), 0);
    }
}

#[tokio::test]
async fn a_refused_backend_rejects_the_subscription_and_releases_interest() {
    let mut harness = harness().await;
    harness.interest.refuse.store(true, Ordering::Relaxed);
    subscribe(&harness.controller, 1, &identifier_for(&harness.token)).await;

    let frames = drain(&mut harness.frames);
    assert_eq!(frames[0]["type"], "reject_subscription");
    assert_eq!(harness.controller.session_count(), 0);
}

#[tokio::test]
async fn commands_from_unconfirmed_or_foreign_subscriptions_are_ignored() {
    let mut harness = harness().await;
    // No subscribe first.
    perform(&harness.controller, &identifier_for(&harness.token), &resume_data(&[])).await;
    assert_eq!(drain(&mut harness.frames), Vec::<Value>::new());

    subscribe(&harness.controller, 1, &identifier_for(&harness.token)).await;
    drain(&mut harness.frames);

    // Right connection, wrong identifier.
    perform(&harness.controller, r#"{"channel":"Orbitcast::RecoveryChannel"}"#, &resume_data(&[]))
        .await;
    // A presence action on the recovery channel is not ours either.
    perform(
        &harness.controller,
        &identifier_for(&harness.token),
        r#"{"action":"orbitcast_presence","op":"join"}"#,
    )
    .await;
    assert_eq!(drain(&mut harness.frames), Vec::<Value>::new());
}

// --- authorization ------------------------------------------------------

#[tokio::test]
async fn an_unidentified_connection_can_neither_subscribe_nor_resume() {
    let mut harness = harness().await;
    let identifier = identifier_for(&harness.token);

    // Dispatch mode, before Rails welcomed the connection: the channel does
    // not exist for it.
    harness
        .controller
        .handle(1, ClientCommand::Subscribe { identifier: identifier.clone() }, false)
        .await;
    let frames = drain(&mut harness.frames);
    assert_eq!(frames[0]["type"], "reject_subscription");
    assert_eq!(harness.controller.session_count(), 0);

    // Confirmed while identified, then a resume that arrives unidentified
    // (the session was never welcomed on this path): refused in full, no
    // lease taken, nothing replayed.
    harness.broker.publish("chat:42", r#"{"n":1}"#);
    subscribe(&harness.controller, 1, &identifier).await;
    drain(&mut harness.frames);
    perform_as(
        &harness.controller,
        1,
        &identifier,
        &resume_data(&[("chat:42", Some(Cursor::new(1, 0)))]),
        false,
    )
    .await;

    let mut frames = Vec::new();
    collect_until(&mut harness.frames, &mut frames, "the refusal", |frames| {
        !gap_reasons(frames).is_empty()
    })
    .await;
    assert_eq!(gap_reasons(&frames), vec!["unauthorized"]);
    assert!(events(&frames).iter().all(|event| event == "gap"), "no replay_start: {frames:?}");
    assert!(deliveries(&frames, "chat:42").is_empty(), "nothing replayed: {frames:?}");
    assert_eq!(harness.broker.fencing_generation("session:7").await.unwrap(), 0, "no lease");
    assert_eq!(harness.broker.stats().gaps, 1, "the refusal is a counted gap");
}

#[tokio::test]
async fn resume_replays_only_the_streams_the_connection_subscribes_to() {
    let broker = FakeBroker::new();
    broker.publish("chat:42", r#"{"n":1}"#);
    broker.publish("posts:1", r#"{"n":1}"#);

    // The token grants both streams; the connection subscribes to one.
    let mut harness = harness_with(Arc::clone(&broker), config()).await;
    harness.token = token_for(&["chat:42", "posts:1"]);
    let identifier = identifier_for(&harness.token);
    subscribe(&harness.controller, 1, &identifier).await;
    drain(&mut harness.frames);

    perform(
        &harness.controller,
        &identifier,
        &resume_data(&[("chat:42", Some(Cursor::new(1, 0))), ("posts:1", Some(Cursor::new(1, 0)))]),
    )
    .await;

    let mut frames = Vec::new();
    collect_until(&mut harness.frames, &mut frames, "replay_done", |frames| {
        events(frames).contains(&"replay_done".to_string())
    })
    .await;

    assert_eq!(gaps_for(&frames, "posts:1"), vec!["unauthorized"]);
    assert!(gaps_for(&frames, "chat:42").is_empty(), "the subscribed stream has no gap");
    assert_eq!(deliveries(&frames, "chat:42").len(), 1);
    assert!(deliveries(&frames, "posts:1").is_empty(), "an unsubscribed stream is never replayed");

    let start =
        messages(&frames).find(|message| message["event"] == "replay_start").expect("replay_start");
    assert_eq!(
        start["streams"],
        serde_json::json!(["chat:42"]),
        "replay covers the granted subset"
    );

    // Nor is it tailed: a later publication on it goes nowhere.
    broker.publish("posts:1", r#"{"n":2}"#);
    settle(&mut harness.frames, &mut frames, Duration::from_millis(80)).await;
    assert!(deliveries(&frames, "posts:1").is_empty(), "the unsubscribed stream stays silent");
}

#[tokio::test]
async fn a_tail_stops_when_the_connection_unsubscribes_from_its_stream() {
    let broker = FakeBroker::new();
    broker.publish("chat:42", r#"{"n":1}"#);
    let mut harness = harness_with(Arc::clone(&broker), config()).await;
    let mut frames =
        subscribe_and_resume(&mut harness, &[("chat:42", Some(Cursor::new(1, 0)))]).await;
    assert_eq!(deliveries(&frames, "chat:42").len(), 1);

    // The application subscription goes away (an unsubscribe, or Rails
    // revoking it on reconnect): the recovery tail loses its authorization.
    harness.hub.remove_subscription(1, APP_IDENTIFIER);
    collect_until(&mut harness.frames, &mut frames, "the unauthorized gap", |frames| {
        gaps_for(frames, "chat:42").contains(&"unauthorized".to_string())
    })
    .await;

    broker.publish("chat:42", r#"{"n":2}"#);
    settle(&mut harness.frames, &mut frames, Duration::from_millis(80)).await;
    assert_eq!(deliveries(&frames, "chat:42").len(), 1, "nothing is delivered after revocation");
}

// --- replay -------------------------------------------------------------

#[tokio::test]
async fn resume_replays_history_after_the_cursor_then_goes_live() {
    let broker = FakeBroker::new();
    for index in 0..5 {
        broker.publish("chat:42", &format!(r#"{{"n":{index}}}"#));
    }

    let mut harness = harness_with(Arc::clone(&broker), config()).await;
    // Resume from sequence 2: 3, 4 and 5 are replayed, 1 and 2 are not.
    let mut frames =
        subscribe_and_resume(&mut harness, &[("chat:42", Some(Cursor::new(1, 2)))]).await;

    assert_eq!(events(&frames), vec!["replay_start", "replay_done"]);
    let replayed = sequences(&frames, "chat:42");
    assert_eq!(replayed, vec![3, 4, 5], "replay is strictly after the cursor, in order");
    assert_eq!(harness.broker.stats().replayed, 3, "replayed deliveries are counted");

    // Live: a publication after the handoff arrives exactly once, in order.
    // The fake store never wakes the tail, so this delivery is found by the
    // reconcile pass - and counted as such.
    broker.publish("chat:42", r#"{"n":"live"}"#);
    collect_until(&mut harness.frames, &mut frames, "the live delivery", |frames| {
        deliveries(frames, "chat:42").len() == 4
    })
    .await;

    let sequences = sequences(&frames, "chat:42");
    assert_eq!(sequences, vec![3, 4, 5, 6], "the handoff is contiguous");

    // Once the handoff flip has passed, the timer is the tail's only wake
    // source here: a row published after a full reconcile interval of
    // silence is found by a reconciliation pass, and counted as such.
    tokio::time::sleep(config().reconcile_interval * 3).await;
    broker.publish("chat:42", r#"{"n":"quiet"}"#);
    collect_until(&mut harness.frames, &mut frames, "the reconciled delivery", |frames| {
        deliveries(frames, "chat:42").len() == 5
    })
    .await;
    assert!(harness.broker.stats().reconciled >= 1, "a timer pass that found rows is counted");
    assert_eq!(harness.broker.stats().gaps, 0);
}

#[tokio::test]
async fn a_resume_without_a_cursor_or_snapshot_asks_for_no_history_at_all() {
    let broker = FakeBroker::new();
    broker.publish("chat:42", r#"{"n":1}"#);

    let mut harness = harness_with(Arc::clone(&broker), config()).await;
    let mut frames = subscribe_and_resume(&mut harness, &[]).await;
    assert!(deliveries(&frames, "chat:42").is_empty(), "history was not requested");

    broker.publish("chat:42", r#"{"n":2}"#);
    collect_until(&mut harness.frames, &mut frames, "the live delivery", |frames| {
        deliveries(frames, "chat:42").len() == 1
    })
    .await;
    assert_eq!(deliveries(&frames, "chat:42")[0].0, 2);
}

#[tokio::test]
async fn a_resume_without_cursors_continues_from_the_stored_snapshot() {
    let broker = FakeBroker::new();
    for index in 0..4 {
        broker.publish("chat:42", &format!(r#"{{"n":{index}}}"#));
    }
    // The session acknowledged up to 2 in an earlier life (another process,
    // a page before the reload).
    let snapshot = SessionSnapshot::new(
        "session:7",
        BTreeMap::from([("chat:42".to_string(), Cursor::new(1, 2).encode())]),
    );
    broker.store_snapshot("session:7", &snapshot.to_json().unwrap());

    let mut harness = harness_with(Arc::clone(&broker), config()).await;
    let frames = subscribe_and_resume(&mut harness, &[]).await;
    let replayed = sequences(&frames, "chat:42");
    assert_eq!(replayed, vec![3, 4], "server-side memory fills in the missing cursors");

    // An explicit cursor always wins over the stored one.
    let frames = subscribe_and_resume(&mut harness, &[("chat:42", Some(Cursor::new(1, 3)))]).await;
    let replayed = sequences(&frames, "chat:42");
    assert_eq!(replayed, vec![4], "the client's cursor is trusted over the snapshot");
}

#[tokio::test]
async fn an_unusable_or_foreign_snapshot_is_ignored() {
    for stored in [
        "not json",
        r#"{"version":9,"session_id":"session:7","cursors":{}}"#,
        // Another session's cursors must never seed this one.
        concat!(
            r#"{"version":1,"session_id":"session:8","#,
            r#""cursors":{"chat:42":"eyJ2IjoxLCJnIjoiMSIsInMiOiIxIn0="}}"#
        ),
    ] {
        let broker = FakeBroker::new();
        broker.publish("chat:42", r#"{"n":1}"#);
        broker.publish("chat:42", r#"{"n":2}"#);
        broker.store_snapshot("session:7", stored);

        let mut harness = harness_with(Arc::clone(&broker), config()).await;
        let frames = subscribe_and_resume(&mut harness, &[]).await;
        assert!(deliveries(&frames, "chat:42").is_empty(), "snapshot {stored:?} must be ignored");
        assert!(gap_reasons(&frames).is_empty(), "an ignored snapshot is not a gap");
    }
}

#[tokio::test]
async fn expired_history_is_reported_as_a_gap_and_delivery_continues() {
    let broker = FakeBroker::new();
    for index in 0..4 {
        broker.publish("chat:42", &format!(r#"{{"n":{index}}}"#));
    }
    // Retention dropped 1 and 2: a cursor at 1 can no longer be filled.
    broker.expire_before("chat:42", 3);

    let mut harness = harness_with(Arc::clone(&broker), config()).await;
    let mut frames =
        subscribe_and_resume(&mut harness, &[("chat:42", Some(Cursor::new(1, 1)))]).await;

    assert_eq!(gap_reasons(&frames), vec!["expired"]);
    assert!(
        deliveries(&frames, "chat:42").is_empty(),
        "a gapped stream is not partially replayed: the application resyncs"
    );
    assert_eq!(harness.broker.stats().gaps, 1);

    // The session is still live afterwards.
    broker.publish("chat:42", r#"{"n":"after"}"#);
    collect_until(&mut harness.frames, &mut frames, "live delivery after a gap", |frames| {
        deliveries(frames, "chat:42").len() == 1
    })
    .await;
    assert_eq!(deliveries(&frames, "chat:42")[0].0, 5);
}

#[tokio::test]
async fn a_hole_opened_during_replay_is_an_expired_gap_not_a_silent_skip() {
    let broker = FakeBroker::new();
    for index in 1..=5 {
        broker.publish("chat:42", &format!(r#"{{"n":{index}}}"#));
    }
    // 1 is still retained, so a cursor at 1 is covered when the resume is
    // planned - but 2 and 3 were trimmed: the first batch jumps from 1 to 4.
    broker.remove_sequences("chat:42", 2..4);

    let mut harness = harness_with(Arc::clone(&broker), config()).await;
    let mut frames =
        subscribe_and_resume(&mut harness, &[("chat:42", Some(Cursor::new(1, 1)))]).await;

    assert_eq!(gap_reasons(&frames), vec!["expired"], "the jump 1 -> 4 is reported");
    assert!(
        deliveries(&frames, "chat:42").is_empty(),
        "nothing after the hole is replayed as if it were contiguous"
    );
    assert_eq!(harness.broker.stats().gaps, 1);

    // The tail detects the same thing on the live side: 6 and 7 are trimmed
    // before it reads, so 8 arrives behind an explicit gap.
    for index in 6..=8 {
        broker.publish("chat:42", &format!(r#"{{"n":{index}}}"#));
    }
    broker.remove_sequences("chat:42", 6..8);
    collect_until(&mut harness.frames, &mut frames, "the delivery of 8", |frames| {
        deliveries(frames, "chat:42").iter().any(|(sequence, _)| *sequence == 8)
    })
    .await;
    assert_eq!(gap_reasons(&frames), vec!["expired", "expired"], "the jump 5 -> 8 is reported");
    let sequences = sequences(&frames, "chat:42");
    assert_eq!(sequences, vec![8], "delivery continues past the hole");
}

#[tokio::test]
async fn a_cursor_from_a_previous_generation_is_an_unknown_generation_gap() {
    let broker = FakeBroker::new();
    broker.publish("chat:42", r#"{"n":1}"#);

    let mut harness = harness_with(Arc::clone(&broker), config()).await;
    let identifier = identifier_for(&harness.token);
    subscribe(&harness.controller, 1, &identifier).await;
    drain(&mut harness.frames);

    perform(
        &harness.controller,
        &identifier,
        &resume_data(&[("chat:42", Some(Cursor::new(7, 1)))]),
    )
    .await;

    let mut frames = Vec::new();
    collect_until(&mut harness.frames, &mut frames, "the gap", |frames| {
        !gap_reasons(frames).is_empty()
    })
    .await;
    assert_eq!(gap_reasons(&frames), vec!["unknown_generation"]);
}

#[tokio::test]
async fn an_unreadable_store_is_a_gap_not_a_silent_stall() {
    let broker = FakeBroker::new();
    broker.fail_range.store(true, Ordering::Relaxed);

    let mut harness = harness_with(Arc::clone(&broker), config()).await;
    let identifier = identifier_for(&harness.token);
    subscribe(&harness.controller, 1, &identifier).await;
    drain(&mut harness.frames);

    perform(&harness.controller, &identifier, &resume_data(&[])).await;

    let mut frames = Vec::new();
    collect_until(&mut harness.frames, &mut frames, "the gap", |frames| {
        !gap_reasons(frames).is_empty()
    })
    .await;
    assert_eq!(gap_reasons(&frames), vec!["unavailable"]);
}

#[tokio::test]
async fn replay_never_delivers_a_publication_twice() {
    let broker = FakeBroker::new();
    for index in 0..3 {
        broker.publish("chat:42", &format!(r#"{{"n":{index}}}"#));
    }

    let mut harness = harness_with(Arc::clone(&broker), config()).await;
    let identifier = identifier_for(&harness.token);
    subscribe(&harness.controller, 1, &identifier).await;
    drain(&mut harness.frames);

    // Two resumes from the same cursor: the second one replays the same
    // rows, and the deduplication window drops them.
    for _ in 0..2 {
        perform(
            &harness.controller,
            &identifier,
            &resume_data(&[("chat:42", Some(Cursor::new(1, 0)))]),
        )
        .await;
    }

    let mut frames = Vec::new();
    collect_until(&mut harness.frames, &mut frames, "two replays", |frames| {
        events(frames).iter().filter(|event| *event == "replay_done").count() == 2
    })
    .await;

    let ids: Vec<String> = deliveries(&frames, "chat:42").into_iter().map(|(_, id)| id).collect();
    let unique: HashSet<&String> = ids.iter().collect();
    assert_eq!(ids.len(), 3, "three messages, delivered once each");
    assert_eq!(unique.len(), ids.len(), "publication ids are never repeated");
}

// --- fencing ------------------------------------------------------------

/// Two connections, each presenting `tokens[i]`, resume one after the other;
/// exactly one loses the fencing race and the winner keeps delivering.
async fn assert_second_resume_fences_first(tokens: [String; 2]) {
    let broker = FakeBroker::new();
    broker.publish("chat:42", r#"{"n":1}"#);

    let (hub, mut frames) = hub_with(&[1, 2], &["chat:42"]).await;
    let interest = Arc::new(RecordingInterest::default());
    let controller = controller_with(&broker, config(), &hub, &interest);

    for (conn_id, token) in [1u32, 2].into_iter().zip(&tokens) {
        subscribe(&controller, conn_id, &identifier_for(token)).await;
    }
    for (conn_id, token) in [1u32, 2].into_iter().zip(&tokens) {
        perform_as(
            &controller,
            conn_id,
            &identifier_for(token),
            &resume_data(&[("chat:42", Some(Cursor::new(1, 0)))]),
            true,
        )
        .await;
    }

    // The loser's tail reports a fencing gap; the winner keeps delivering.
    let mut collected = Vec::new();
    collect_until(&mut frames, &mut collected, "the fencing gap", |frames| {
        gap_reasons(frames).contains(&"fenced".to_string())
    })
    .await;

    broker.publish("chat:42", r#"{"n":2}"#);
    collect_until(&mut frames, &mut collected, "the winner's live delivery", |frames| {
        deliveries(frames, "chat:42").iter().any(|(sequence, _)| *sequence == 2)
    })
    .await;

    assert_eq!(
        gap_reasons(&collected).iter().filter(|reason| *reason == "fenced").count(),
        1,
        "exactly one resume loses the race"
    );
    assert_eq!(broker.stats().fenced, 2, "two leases were claimed");
}

#[tokio::test]
async fn a_second_resume_of_one_session_fences_the_first_out() {
    // The same token on two connections, as two nodes would present it.
    let token = token_for(&["chat:42"]);
    assert_second_resume_fences_first([token.clone(), token]).await;
}

#[tokio::test]
async fn two_different_tokens_for_one_session_fence_each_other() {
    // Fencing is per session, not per token: a refreshed token for the same
    // session_id takes the lease over from the older one.
    let older = verifier().sign(&claims_for("session:7", &["chat:42"], now_unix() + NOW_PLUS));
    let newer = verifier().sign(&claims_for("session:7", &["chat:42"], now_unix() + NOW_PLUS + 60));
    assert_ne!(older, newer, "the tokens must differ for the test to mean anything");
    assert_second_resume_fences_first([older, newer]).await;
}

#[tokio::test]
async fn a_live_tail_keeps_the_session_row_alive() {
    // A TTL this short makes the touch cadence a quarter of it (10 ms): a
    // quiet session must still be touched well within the TTL.
    let broker = FakeBroker::new();
    let config = BrokerConfig { session_ttl: Duration::from_millis(40), ..config() };
    let mut harness = harness_with(Arc::clone(&broker), config).await;
    subscribe_and_resume(&mut harness, &[]).await;

    let deadline = Instant::now() + Duration::from_secs(2);
    while broker.touches("session:7") < 2 {
        assert!(Instant::now() < deadline, "the tail never extended the session row");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // Bounded: well under one touch per reconcile pass.
    let touched_after = broker.touches("session:7");
    tokio::time::sleep(Duration::from_millis(100)).await;
    let later = broker.touches("session:7");
    assert!(later - touched_after <= 12, "touches are rate limited: {touched_after} -> {later}");
}

#[test]
fn touches_are_claimed_at_most_once_per_interval() {
    let session = RecoverySession::new(
        1,
        String::new(),
        claims_for("session:7", &["chat:42"], now_unix() + NOW_PLUS),
    );
    // Freshly created: the first slot is not due yet.
    assert!(!session.touch_due(Duration::from_secs(60)));
    // A zero interval is always due, and claiming it moves the clock.
    assert!(session.touch_due(Duration::ZERO));
    assert!(!session.touch_due(Duration::from_secs(60)));
}

// --- acknowledgements ---------------------------------------------------

#[tokio::test]
async fn ack_records_a_versioned_snapshot_and_never_moves_the_lease() {
    let broker = FakeBroker::new();
    broker.publish("chat:42", r#"{"n":1}"#);

    let mut harness = harness_with(Arc::clone(&broker), config()).await;
    subscribe_and_resume(&mut harness, &[("chat:42", Some(Cursor::new(1, 0)))]).await;
    let identifier = identifier_for(&harness.token);

    let lease = harness.broker.fencing_generation("session:7").await.unwrap();
    assert_eq!(lease, 1);

    let ack = RecoveryCommand::new(RecoveryOp::Ack).with_cursor("chat:42", &Cursor::new(1, 1));
    perform(&harness.controller, &identifier, &serde_json::to_string(&ack).unwrap()).await;

    let stored = harness.broker.snapshot_for("session:7").expect("an ack is recorded");
    let snapshot = SessionSnapshot::decode(&stored).unwrap();
    assert_eq!(snapshot.version, 1);
    assert_eq!(snapshot.session_id, "session:7");
    assert_eq!(snapshot.cursors["chat:42"], Cursor::new(1, 1).encode());

    assert_eq!(
        harness.broker.fencing_generation("session:7").await.unwrap(),
        lease,
        "an acknowledgement is a receipt, not a new lease"
    );
    assert!(drain(&mut harness.frames).is_empty(), "an ack answers nothing");
}

// --- construction -------------------------------------------------------

#[tokio::test]
async fn recovery_is_never_enabled_implicitly() {
    let (tx, _frames) = mpsc::channel(8);
    let hub = Arc::new(Hub::new(tx));
    let broker = FakeBroker::new() as Arc<dyn Broker>;
    let interest = Arc::new(RecordingInterest::default()) as Arc<dyn StreamInterest>;
    let secret = SecretBytes::new(SECRET.to_vec());
    let waker = Arc::new(StreamWaker::new());

    // No secret, no broker, or a disabled broker: no controller.
    assert!(
        build(
            NAMESPACE.into(),
            None,
            Some(Arc::clone(&broker)),
            &config(),
            Arc::clone(&hub),
            Some(Arc::clone(&waker)),
            Arc::clone(&interest)
        )
        .is_none()
    );
    assert!(
        build(
            NAMESPACE.into(),
            Some(secret.clone()),
            None,
            &config(),
            Arc::clone(&hub),
            Some(Arc::clone(&waker)),
            Arc::clone(&interest)
        )
        .is_none()
    );
    assert!(
        build(
            NAMESPACE.into(),
            Some(secret.clone()),
            Some(Arc::clone(&broker)),
            &BrokerConfig::default(),
            Arc::clone(&hub),
            Some(Arc::clone(&waker)),
            Arc::clone(&interest)
        )
        .is_none()
    );
    assert!(
        build(NAMESPACE.into(), Some(secret), Some(broker), &config(), hub, Some(waker), interest)
            .is_some()
    );
}

#[test]
fn the_waker_forgets_streams_nobody_tails() {
    let waker = StreamWaker::new();
    waker.wake("chat:42"); // nobody listening: a no-op

    let mut receiver = waker.subscribe("chat:42");
    waker.wake("chat:42");
    assert!(receiver.try_recv().is_ok());

    drop(receiver);
    waker.wake("chat:42");
    assert!(waker.channels.is_empty(), "a stream with no tail is dropped");
}

#[test]
fn the_dedup_window_is_bounded() {
    let mut window = DedupWindow::new();
    assert!(window.accept("a"));
    assert!(!window.accept("a"));

    for index in 0..DEDUP_WINDOW {
        window.accept(&format!("id-{index}"));
    }
    assert!(window.seen.len() <= DEDUP_WINDOW + 1);
    // The oldest id has fallen out of the window, so it would be accepted
    // again: deduplication is explicitly bounded, never a leak.
    assert!(window.accept("a"));
}

#[test]
fn resume_refusals_name_their_cause() {
    assert!(ResumeRefusal::Unidentified.to_string().contains("not identified"));
    let refusal = ResumeRefusal::NotSubscribed { stream: "chat:42".into() };
    assert!(refusal.to_string().contains("chat:42"));
    let refusal = ResumeRefusal::TokenInvalid(ProtocolError::Expired { expires_at: 1, now: 2 });
    assert!(refusal.to_string().contains("no longer valid"));
}

// ---- Integration test: requires a live PostgreSQL ----------------------
//
// Run with:
//   DATABASE_URL=postgres://... cargo test -p orbitcast -- --ignored

#[cfg(all(feature = "postgres", not(feature = "memory")))]
mod live {
    use super::*;
    use crate::broker::postgres::test_support::integration_broker;
    use crate::protocol::v1::publication::Publication;

    /// Publish continuously *while* a resume is in flight and assert the
    /// replay-to-live handoff loses nothing and repeats nothing.
    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn the_replay_to_live_handoff_delivers_every_sequence_exactly_once() {
        const SEEDED: u64 = 20;
        const DURING: u64 = 30;

        let broker_config = BrokerConfig {
            enabled: true,
            reconcile_interval: Duration::from_millis(50),
            ..BrokerConfig::default()
        };
        let broker = integration_broker("recovery_handoff", broker_config.clone()).await;
        let stream = "chat:handoff";

        let publish = |broker: Arc<crate::broker::postgres::PostgresBroker>, n: u64| async move {
            broker
                .retain(&Publication::new(NAMESPACE, "chat:handoff", format!(r#"{{"n":{n}}}"#)))
                .await
                .expect("retain commits")
        };

        for n in 0..SEEDED {
            publish(Arc::clone(&broker), n).await;
        }

        let (hub, mut frames) = hub_with(&[1], &[stream]).await;
        let interest = Arc::new(RecordingInterest::default());
        let controller = RecoveryController::new(
            NAMESPACE.to_string(),
            SecretBytes::new(SECRET.to_vec()),
            Arc::clone(&broker) as Arc<dyn Broker>,
            broker_config,
            hub,
            Arc::new(StreamWaker::new()),
            interest as Arc<dyn StreamInterest>,
        );

        let token = token_for(&[stream]);
        let identifier = identifier_for(&token);
        subscribe(&controller, 1, &identifier).await;

        // Publish throughout the resume, so live traffic really does race
        // the replay and exercise the buffered handoff.
        let writer = tokio::spawn({
            let broker = Arc::clone(&broker);
            async move {
                for n in SEEDED..SEEDED + DURING {
                    publish(Arc::clone(&broker), n).await;
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        });

        perform(&controller, &identifier, &resume_data(&[(stream, Some(Cursor::new(1, 0)))])).await;

        writer.await.expect("publisher task");

        let total = usize::try_from(SEEDED + DURING).unwrap();
        let mut collected = Vec::new();
        collect_until(&mut frames, &mut collected, "every retained message", |frames| {
            deliveries(frames, stream).len() >= total
        })
        .await;

        let delivered = deliveries(&collected, stream);
        let sequences: Vec<u64> = delivered.iter().map(|(sequence, _)| *sequence).collect();
        let expected: Vec<u64> = (1..=SEEDED + DURING).collect();
        assert_eq!(sequences, expected, "the handoff is contiguous and in order");

        let ids: HashSet<&String> = delivered.iter().map(|(_, id)| id).collect();
        assert_eq!(ids.len(), delivered.len(), "no publication is delivered twice");
        assert!(gap_reasons(&collected).is_empty(), "a clean handoff reports no gaps");
        assert_eq!(events(&collected), vec!["replay_start", "replay_done"]);
        assert_eq!(broker.stats().replayed, SEEDED, "the seeded rows were replayed");
    }
}
