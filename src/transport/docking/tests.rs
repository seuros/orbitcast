use std::assert_matches;
use std::collections::HashMap;
use std::os::unix::fs::FileTypeExt as _;
use std::time::Duration;

use mothership_docking_protocol::{encode_boarding, encode_header, encode_moored};
use tokio::io::duplex;

use super::*;
use crate::transport::test_support::{backend_unready, config};

/// Both outbound message kinds encode to frames Mothership decodes back.
#[test]
fn outgoing_messages_round_trip_through_the_codec() {
    let frame = TextFrame { conn_id: 42, data: Bytes::from_static(b"test payload") };
    let encoded = encode_outgoing(&Outgoing::Cargo(frame.clone()));
    // The link writer takes the same path without copying the frame data.
    let mut written = Vec::new();
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime")
        .block_on(write_outgoing(&mut written, &Outgoing::Cargo(frame)))
        .expect("written");
    assert_eq!(written, encoded, "header + shared data equals the copying encoder's frame");
    let (msg_type, len) = decode_header(&encoded).unwrap();
    assert_eq!(msg_type, MessageType::Cargo);
    let decoded = decode_cargo(&encoded[HEADER_LEN..HEADER_LEN + len]).unwrap();
    assert_eq!(decoded.conn_id, 42);
    assert_eq!(decoded.kind, CargoKind::Text);
    assert_eq!(decoded.data, b"test payload");

    let disembark = Disembark { conn_id: 7, code: CLOSE_GOING_AWAY, reason: "going away".into() };
    let encoded = encode_outgoing(&Outgoing::Disembark(disembark));
    let (msg_type, len) = decode_header(&encoded).unwrap();
    assert_eq!(msg_type, MessageType::Disembark);
    let decoded: Disembark =
        serde_json::from_slice(&encoded[HEADER_LEN..HEADER_LEN + len]).expect("disembark");
    assert_eq!((decoded.conn_id, decoded.code, decoded.reason.as_str()), (7, 1001, "going away"));
}

#[tokio::test]
async fn serve_refuses_a_configuration_without_docking_settings() {
    let error = serve(config(), backend_unready(), Shutdown::new())
        .await
        .expect_err("nothing to dock with");
    assert!(error.to_string().contains("MS_*"), "{error}");
}

/// The ship socket replaces whatever a previous process left at its path, and
/// a shutdown before Mothership docks stops the listener cleanly.
#[tokio::test]
async fn serve_replaces_a_stale_socket_and_stops_on_shutdown() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let docking = DockingConfig {
        mothership_pid: 1,
        ship_name: "cable".to_string(),
        socket_dir: dir.path().join("ships"),
        routes: Vec::new(),
    };
    let socket = docking.socket_path();
    std::fs::create_dir_all(dir.path().join("ships")).expect("socket dir");
    std::fs::write(&socket, b"stale").expect("stale file");

    let mut config = config();
    config.docking = Some(docking);
    let shutdown = Shutdown::new();
    let serving = tokio::spawn(serve(config, backend_unready(), shutdown.clone()));

    tokio::time::timeout(Duration::from_secs(5), async {
        while !std::fs::metadata(&socket).is_ok_and(|meta| meta.file_type().is_socket()) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the ship socket is bound over the stale file");

    shutdown.trigger();
    tokio::time::timeout(Duration::from_secs(5), serving)
        .await
        .expect("serve stops on shutdown")
        .expect("serve task")
        .expect("a clean stop");
}

/// Every frame a single read buffered comes out, in order, as a slice of the
/// read buffer.
#[test]
fn take_frame_splits_every_buffered_frame_without_copying() {
    let messages: [&[u8]; 3] = [b"one", b"two", b"three"];
    let wire: Vec<u8> = messages
        .iter()
        .flat_map(|data| {
            encode_cargo(&Cargo { conn_id: 7, kind: CargoKind::Text, data: data.to_vec() })
        })
        .collect();
    let mut pending = BytesMut::from(&wire[..]);
    let buffer = pending.as_ptr_range();

    for expected in messages {
        let (msg_type, payload) = take_frame(&mut pending).expect("valid frame").expect("buffered");
        assert_eq!(msg_type, MessageType::Cargo);
        assert!(buffer.contains(&payload.as_ptr()), "payload shares the read buffer");
        let cargo = decode_cargo(&payload).expect("cargo");
        assert_eq!((cargo.conn_id, cargo.data), (7, expected));
        assert!(buffer.contains(&cargo.data.as_ptr()), "cargo data borrows the read buffer");
    }
    assert_matches!(take_frame(&mut pending), Ok(None));
    assert!(pending.is_empty());
}

/// A frame split across reads waits for the rest, with room reserved for it.
#[test]
fn take_frame_waits_for_the_rest_of_a_frame() {
    let wire = encode_cargo(&Cargo { conn_id: 1, kind: CargoKind::Binary, data: vec![9; 100] });
    let mut pending = BytesMut::from(&wire[..40]);

    assert_matches!(take_frame(&mut pending), Ok(None));
    assert!(pending.capacity() >= wire.len(), "room reserved for the whole frame");

    pending.extend_from_slice(&wire[40..]);
    let (_, payload) = take_frame(&mut pending).expect("valid frame").expect("complete");
    assert_eq!(decode_cargo(&payload).expect("cargo").data, vec![9; 100]);
}

/// A header announcing more than the protocol allows ends the link instead
/// of reserving (or waiting for) the bytes it announces.
#[test]
fn take_frame_refuses_frames_past_the_payload_limit() {
    let limit = u32::try_from(MAX_FRAME_PAYLOAD).expect("limit fits a header");
    let mut pending = BytesMut::from(&encode_header(MessageType::Cargo, limit + 1)[..]);

    let error = take_frame(&mut pending).expect_err("oversized frame refused");
    assert!(error.to_string().contains("exceeds"), "{error}");
    assert!(pending.capacity() < MAX_FRAME_PAYLOAD, "nothing reserved for it");
}

/// Frames Mothership sends right behind MOORED are handed to the link
/// reader, not dropped with the handshake buffer.
#[tokio::test]
async fn dock_keeps_frames_that_arrive_with_moored() {
    let (mut ship, mut mothership) = duplex(64 * 1024);
    let boarding = Boarding {
        conn_id: 3,
        path: "/cable".into(),
        remote_addr: "203.0.113.7:4321".into(),
        headers: HashMap::new(),
    };
    let reply = [
        encode_moored(&Moored { version: VERSION, config: HashMap::new() }),
        encode_boarding(&boarding),
    ]
    .concat();

    let mothership = tokio::spawn(async move {
        let mut header = [0u8; HEADER_LEN];
        mothership.read_exact(&mut header).await.expect("DOCK header");
        let (msg_type, len) = decode_header(&header).expect("header");
        assert_eq!(msg_type, MessageType::Dock);
        let mut dock = vec![0u8; len];
        mothership.read_exact(&mut dock).await.expect("DOCK payload");
        // One write: MOORED and BOARDING reach the ship in the same read.
        mothership.write_all(&reply).await.expect("reply");
        mothership
    });

    let (moored, mut pending) =
        dock(&mut ship, "cable", Duration::from_secs(5)).await.expect("docked");
    assert_eq!(moored.version, VERSION);
    let (msg_type, payload) =
        take_frame(&mut pending).expect("valid frame").expect("BOARDING kept");
    assert_eq!(msg_type, MessageType::Boarding);
    let kept: Boarding = serde_json::from_slice(&payload).expect("boarding");
    assert_eq!((kept.conn_id, kept.path.as_str()), (3, "/cable"));
    drop(mothership.await.expect("mothership side"));
}

/// A peer that connects and never finishes MOORED is dropped at the deadline.
#[tokio::test]
async fn dock_gives_up_on_a_silent_peer() {
    let (mut ship, mothership) = duplex(64 * 1024);
    let error = dock(&mut ship, "cable", Duration::from_millis(50))
        .await
        .expect_err("a silent peer must not hold the handshake open");
    assert!(error.to_string().contains("did not complete"), "{error}");
    drop(mothership);
}

/// A frame whose remaining bytes never arrive ends the link at the deadline
/// instead of waiting on a stalled supervisor forever.
#[tokio::test]
async fn an_incomplete_frame_ends_the_link_at_the_deadline() {
    let (mut ship, mut mothership) = duplex(64 * 1024);
    let backend = backend_unready();
    let mut config = config();
    config.streams.public = true;
    config.streams.public_insecure = true;
    let (outgoing_tx, _outgoing_rx) = mpsc::channel(8);
    let connections = Connections::new(backend, &config, outgoing_tx).expect("driver");
    let shutdown = Shutdown::new();
    let mut shutdown_rx = shutdown.subscribe();

    let wire = encode_cargo(&Cargo { conn_id: 1, kind: CargoKind::Text, data: vec![b'x'; 100] });
    let reader = tokio::spawn(async move {
        read_link(
            &mut ship,
            BytesMut::new(),
            &connections,
            &mut shutdown_rx,
            LinkDeadlines { shutdown: Duration::from_secs(1), frame: Duration::from_millis(100) },
        )
        .await
    });
    mothership.write_all(&wire[..40]).await.expect("half a frame");

    let error = tokio::time::timeout(Duration::from_secs(5), reader)
        .await
        .expect("the link ends at the deadline")
        .expect("reader task")
        .expect_err("an incomplete frame is a link error");
    assert!(error.to_string().contains("incomplete"), "{error}");
}

/// A handshake naming a key this build does not know is fatal, not ignored.
#[test]
fn an_unknown_handshake_key_is_a_startup_error() {
    let mut config = config();
    let moored: HashMap<String, String> =
        HashMap::from([("listen_addr".to_string(), "0.0.0.0:1".to_string())]);
    let error = config.apply_moored_config(&moored).expect_err("binds are not handshake settings");
    assert!(error.to_string().contains("listen_addr"), "{error}");
}
