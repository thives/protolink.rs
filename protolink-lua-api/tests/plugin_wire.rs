//! Golden wire checks for `lua.plugin.v1`.
//!
//! Each vector is the protobuf encoding that `protoc` produces from the canonical schema for the text format named in the
//! comment. Decoding a vector must give the expected values, which pins the field numbers, wire types and oneof members.
//! Encoding the decoded message must decode back to an equal message. The raw bytes are not compared, as described in
//! `common::round_trip`.

#![cfg(feature = "plugin")]

mod common;

use common::round_trip;
use protolink_lua_api::plugin::proto::lua_::plugin_::v1_::{
    DescribeResponse, InvokeOutcome, InvokeOutcome_, InvokeRequest, Value, Value_,
};

/// `protoc` text: `integer: -1`. The integer is zigzag encoded, so -1 is sent as 1.
const VALUE_INTEGER: &[u8] = &[0x18, 0x01];
/// `protoc` text: `protocol: 1 sequence: 2 module: "m" major: 1 minor: 0 export: "e" arguments { integer: -1 }
/// arguments { float: 1.5 } arguments { boolean: true } arguments { nil { } }`
const INVOKE_REQUEST: &[u8] = &[
    0x08, 0x01, 0x10, 0x02, 0x1a, 0x01, 0x6d, 0x20, 0x01, 0x32, 0x01, 0x65, 0x3a, 0x02, 0x18, 0x01,
    0x3a, 0x09, 0x21, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x3f, 0x3a, 0x02, 0x10, 0x01, 0x3a,
    0x02, 0x0a, 0x00,
];
/// `protoc` text: `failure { code: 2 detail: 3 }`
const INVOKE_OUTCOME_FAILURE: &[u8] = &[0x12, 0x04, 0x08, 0x02, 0x10, 0x03];
/// `protoc` text: `protocol: 1 limits { max_arguments: 4 max_message_bytes: 300 } modules { name: "m" major: 1 minor: 0
/// exports { name: "e" arguments: VALUE_KIND_INTEGER } }`
const DESCRIBE_RESPONSE: &[u8] = &[
    0x08, 0x01, 0x12, 0x05, 0x08, 0x04, 0x10, 0xac, 0x02, 0x1a, 0x0d, 0x0a, 0x01, 0x6d, 0x10, 0x01,
    0x22, 0x06, 0x0a, 0x01, 0x65, 0x12, 0x01, 0x02,
];

#[test]
fn integer_value_is_zigzag_encoded_in_field_three() {
    let value: Value = round_trip(VALUE_INTEGER);

    assert!(matches!(value.kind, Some(Value_::Kind::Integer(-1))));
}

#[test]
fn invoke_request_keeps_its_export_and_argument_order() {
    let request: InvokeRequest = round_trip(INVOKE_REQUEST);

    assert_eq!((request.protocol, request.sequence), (1, 2));
    assert_eq!(
        (
            request.module.as_str(),
            request.major,
            request.export.as_str()
        ),
        ("m", 1, "e")
    );
    assert_eq!(request.arguments.len(), 4);
    assert!(matches!(
        request.arguments[0].kind,
        Some(Value_::Kind::Integer(-1))
    ));
    assert!(matches!(
        request.arguments[3].kind,
        Some(Value_::Kind::Nil(_))
    ));
}

#[test]
fn failure_outcome_keeps_its_code_and_detail() {
    let outcome: InvokeOutcome = round_trip(INVOKE_OUTCOME_FAILURE);

    assert!(matches!(
        outcome.outcome,
        Some(InvokeOutcome_::Outcome::Failure(ref failure)) if failure.code == 2 && failure.detail == 3
    ));
}

#[test]
fn describe_response_keeps_its_modules_and_export_kinds() {
    let response: DescribeResponse = round_trip(DESCRIBE_RESPONSE);

    assert_eq!(response.protocol, 1);
    assert_eq!(response.modules.len(), 1);
    assert_eq!(response.modules[0].name.as_str(), "m");
    assert_eq!(response.modules[0].exports[0].arguments.len(), 1);
}
