//! OrbitCast - Real-time WebSocket server with ActionCable protocol
//!
//! A high-performance WebSocket server that implements the ActionCable protocol,
//! designed to work with any backend (Rails, Laravel, Phoenix, etc.) via RPC.
//!
//! OrbitCast runs as a ship under Mothership, communicating via Unix socket.
//!
//! ## Architecture
//!
//! ```text
//! Client (WS) → Mothership → Unix Socket → OrbitCast → Backend (RPC)
//! ```
//!
//! ## Environment Variables
//!
//! OrbitCast expects these from Mothership:
//! - `MS_PID` - Mothership's process ID
//! - `MS_SHIP` - This ship's name
//! - `MS_SOCKET_DIR` - Directory for Unix sockets

// Compile-time feature validation: exactly one pub/sub backend required
#[cfg(not(any(feature = "postgres", feature = "memory")))]
compile_error!(
    "OrbitCast requires a pub/sub backend. Enable: --features postgres OR --features memory"
);

// Enforce mutual exclusivity
#[cfg(all(feature = "postgres", feature = "memory"))]
compile_error!("Only one pub/sub backend can be enabled. Choose postgres OR memory, not both.");

pub mod actioncable;
pub mod config;
pub mod hub;
pub mod protocol;
pub mod pubsub;
pub mod session;

pub use actioncable::{ClientCommand, ServerMessage};
pub use config::Config;
pub use hub::Hub;
pub use protocol::{
    Boarding, Cargo, Disembark, Dock, MessageType, Moored, ProtocolError, VERSION,
};
pub use pubsub::PubSub;
pub use session::Session;

#[cfg(feature = "postgres")]
pub use pubsub::PostgresPubSub;

#[cfg(feature = "memory")]
pub use pubsub::MemoryPubSub;
