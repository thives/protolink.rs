use std::fmt::Debug;

use micropb::{MessageDecode, MessageEncode, PbEncoder};

/// Decodes `golden` into `M`, then checks that the encoding of that message decodes back to an equal message.
///
/// The raw bytes are not compared. micropb encodes proto3 repeated scalars unpacked, where `protoc` packs them, and it
/// writes plain fields before oneof members, where `protoc` uses field-number order. The protobuf specification requires
/// parsers to accept both forms, and this crate does not change the schema, so the wire contract is unchanged.
pub fn round_trip<M: MessageDecode + MessageEncode + Default + Debug + PartialEq>(
    golden: &[u8],
) -> M {
    let mut message = M::default();
    message
        .decode_from_bytes(golden)
        .expect("golden vector decodes");

    let mut encoder = PbEncoder::new(Vec::with_capacity(message.compute_size()));
    message
        .encode(&mut encoder)
        .expect("encoding to a Vec cannot fail");
    let encoded = encoder.into_writer();

    let mut decoded_again = M::default();
    decoded_again
        .decode_from_bytes(&encoded)
        .expect("encoded message decodes");
    assert_eq!(decoded_again, message);

    message
}
