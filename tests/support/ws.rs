//! WebSocket client helpers shared by the integration test crates.
//!
//! Included with `#[path = "support/ws.rs"]`: a top-level `tests/support.rs`
//! would be built as a test crate of its own. Every including crate uses
//! [`connect`], [`try_connect`], [`next_frame`], and [`send_command`], so none
//! of them is dead code anywhere.

use std::time::Duration;

use rama::extensions::Extensions;
use rama::futures::SinkExt as _;
use rama::http::client::EasyHttpWebClient;
use rama::http::headers::SecWebSocketProtocol;
use rama::http::ws::Message;
use rama::http::ws::handshake::client::{ClientWebSocket, HttpClientWebSocketExt as _};
use rama::utils::collections::non_empty_smallvec;
use rama::utils::str::non_empty_str;
use serde_json::{Value, json};

use orbitcast::transport::http::ACTIONCABLE_SUBPROTOCOL;

/// Deadline for every handshake, frame, and HTTP response in these tests.
pub const TIMEOUT: Duration = Duration::from_secs(5);

/// The subprotocol offer `@rails/actioncable` sends.
pub fn actioncable_offer() -> SecWebSocketProtocol {
    SecWebSocketProtocol(non_empty_smallvec![
        non_empty_str!(ACTIONCABLE_SUBPROTOCOL),
        non_empty_str!("actioncable-unsupported"),
    ])
}

/// Connect to `url` with the standard `ActionCable` offer and no `Origin`.
pub async fn connect(url: String) -> ClientWebSocket {
    try_connect(url, Some(actioncable_offer()), None).await.expect("websocket handshake")
}

/// Attempt one handshake, controlling the subprotocol offer and `Origin`.
pub async fn try_connect(
    url: String,
    protocols: Option<SecWebSocketProtocol>,
    origin: Option<&str>,
) -> Result<ClientWebSocket, rama::error::BoxError> {
    // The client is needed for the handshake only, not for the socket's life.
    let client = EasyHttpWebClient::default();
    let mut builder = client.websocket(url);

    if let Some(protocols) = protocols {
        builder = builder.with_protocols(protocols);
    }
    if let Some(origin) = origin {
        builder = builder.with_header("origin", origin);
    }

    tokio::time::timeout(TIMEOUT, builder.handshake(Extensions::default()))
        .await
        .map_err(|_| -> rama::error::BoxError { "handshake timed out".into() })?
        .map_err(rama::error::BoxError::from)
}

/// Receive the next `ActionCable` frame, skipping the JSON heartbeat.
///
/// A close frame comes back as `{"type": "__closed"}`, so a test can assert
/// on a server-initiated close like on any other frame.
pub async fn next_frame(socket: &mut ClientWebSocket) -> Value {
    loop {
        let message = tokio::time::timeout(TIMEOUT, socket.recv_message())
            .await
            .expect("frame within deadline")
            .expect("socket open");

        match message {
            Message::Text(text) => {
                let frame: Value = serde_json::from_str(&text).expect("frame is JSON");
                if frame["type"] == "ping" {
                    continue;
                }
                return frame;
            }
            Message::Ping(_) | Message::Pong(_) => {}
            Message::Close(_) => return json!({ "type": "__closed" }),
            other => panic!("unexpected websocket message: {other:?}"),
        }
    }
}

/// Write one client command.
pub async fn send_command(socket: &mut ClientWebSocket, command: Value) {
    socket.send(Message::text(command.to_string())).await.expect("command written to socket");
}
