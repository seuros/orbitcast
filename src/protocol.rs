//! Protocol re-exports from mothership docking protocol
//!
//! OrbitCast uses the standard Mothership docking protocol.

pub use mothership::docking::{
    Boarding, Cargo, Disembark, Dock, MessageType, Moored, ProtocolError, VERSION,
    decode_cargo, decode_header, encode_boarding, encode_cargo, encode_disembark,
    encode_dock, encode_header, encode_moored,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cargo_roundtrip() {
        let cargo = Cargo {
            conn_id: 42,
            data: b"test payload".to_vec(),
        };
        let encoded = encode_cargo(&cargo);
        let (msg_type, len) = decode_header(&encoded).unwrap();
        assert_eq!(msg_type, MessageType::Cargo);
        let decoded = decode_cargo(&encoded[5..5 + len]).unwrap();
        assert_eq!(decoded.conn_id, 42);
        assert_eq!(decoded.data, b"test payload");
    }
}
