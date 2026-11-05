//! Golden wire checks for `lua.execution.v1`.
//!
//! Each vector is the protobuf encoding that `protoc` produces from the canonical schema for the text format named in the
//! comment. Decoding a vector must give the expected values, which pins the field numbers, wire types and oneof members.
//! Encoding the decoded message must decode back to an equal message. The raw bytes are not compared, as described in
//! `common::round_trip`.

#![cfg(feature = "execution")]

mod common;

use common::round_trip;
use protolink_lua_api::execution::wire::v1::{
    CancelResponse, Context, DescribeResponse, Outcome, Outcome_, QueryOutcomeResponse,
    ReleaseResponse, StartRequest, StatusResponse, Value, Value_,
};

/// `protoc` text: `epoch: 1 session_id: 2 sequence: 3`
const CONTEXT: &[u8] = &[0x08, 0x01, 0x10, 0x02, 0x18, 0x03];
/// `protoc` text: `bytes: "ab"`
const VALUE_BYTES: &[u8] = &[0x2a, 0x02, 0x61, 0x62];
/// `protoc` text: `float: 1.5`
const VALUE_FLOAT: &[u8] = &[0x21, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x3f];
/// `protoc` text: `failed { code: FAILURE_CODE_BUDGET_EXHAUSTED message: "x" } uncertain: true`
const OUTCOME_FAILED: &[u8] = &[0x1a, 0x05, 0x08, 0x05, 0x12, 0x01, 0x78, 0x20, 0x01];
/// `protoc` text: `context { epoch: 9 session_id: 8 sequence: 7 } digest: "\001\002" entry: 1 captures { values { nil { } } }
/// arguments { values { integer: -2 } values { bytes: "z" } } lease_ms: 500`
const START_REQUEST: &[u8] = &[
    0x0a, 0x06, 0x08, 0x09, 0x10, 0x08, 0x18, 0x07, 0x12, 0x02, 0x01, 0x02, 0x18, 0x01, 0x22, 0x04,
    0x0a, 0x02, 0x0a, 0x00, 0x2a, 0x09, 0x0a, 0x02, 0x18, 0x03, 0x0a, 0x03, 0x2a, 0x01, 0x7a, 0x30,
    0xf4, 0x03,
];
/// `protoc` text: `state: STATE_RETURNED available: true outcome { returned { values { boolean: true } } }`
const QUERY_OUTCOME_RESPONSE: &[u8] = &[
    0x10, 0x07, 0x18, 0x01, 0x22, 0x06, 0x12, 0x04, 0x0a, 0x02, 0x10, 0x01,
];
/// `protoc` text: `context { epoch: 1 } contract: "lua.execution.v1" artifact_format: 1 profile: 1
/// max_artifact_bytes: 65536 max_chunk_bytes: 1024 max_tuple_items: 16 max_string_bytes: 256 max_executions: 4 has_child: true`
const DESCRIBE_RESPONSE: &[u8] = &[
    0x0a, 0x02, 0x08, 0x01, 0x12, 0x10, 0x6c, 0x75, 0x61, 0x2e, 0x65, 0x78, 0x65, 0x63, 0x75, 0x74,
    0x69, 0x6f, 0x6e, 0x2e, 0x76, 0x31, 0x18, 0x01, 0x20, 0x01, 0x28, 0x80, 0x80, 0x04, 0x30, 0x80,
    0x08, 0x38, 0x10, 0x40, 0x80, 0x02, 0x48, 0x04, 0x50, 0x01,
];
/// `protoc` text: `session_floor: 18446744073709551615`
const DESCRIBE_RESERVED_FLOOR: &[u8] = &[
    0x58, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01,
];
/// `protoc` text: `protocol_revision: 2 admission_floor: 5 retired_window: 256 max_request_bytes: 1024`
const DESCRIBE_REVISION_TWO: &[u8] = &[
    0x68, 0x02, 0x70, 0x05, 0x78, 0x80, 0x02, 0x80, 0x01, 0x80, 0x08,
];
/// `protoc` text: `state: STATE_CANCELLING uncertain: true`
const CANCEL_RESPONSE: &[u8] = &[0x10, 0x06, 0x18, 0x01];
/// `protoc` text: `state: STATE_FAILED turn: 2 uncertain: true`
const STATUS_RESPONSE: &[u8] = &[0x10, 0x08, 0x18, 0x02, 0x20, 0x01];
/// `protoc` text: `state: STATE_CANCELLING uncertain: true`
const QUERY_UNAVAILABLE_UNCERTAIN: &[u8] = &[0x10, 0x06, 0x28, 0x01];

// Revision 3 vectors: receipt { sequence: 2 fingerprint: "ab" }, release and idle timeout.
const STATUS_RECEIPT: &[u8] = &[0x2a, 0x06, 0x08, 0x02, 0x12, 0x02, 0x61, 0x62];
const QUERY_RECEIPT: &[u8] = &[0x32, 0x06, 0x08, 0x02, 0x12, 0x02, 0x61, 0x62];
const RELEASE_UNCERTAIN: &[u8] = &[0x10, 0x01, 0x18, 0x01];
const DESCRIBE_REVISION_THREE: &[u8] = &[0x68, 0x03, 0x88, 0x01, 0xb0, 0xea, 0x01];

#[test]
fn revision_three_evidence_fields_round_trip() {
    let status: StatusResponse = round_trip(STATUS_RECEIPT);
    let receipt = status.last_mutation().unwrap();
    assert_eq!(receipt.sequence, 2);
    assert_eq!(receipt.fingerprint, b"ab");
    let query: QueryOutcomeResponse = round_trip(QUERY_RECEIPT);
    assert_eq!(query.last_mutation().unwrap(), receipt);
    let release: ReleaseResponse = round_trip(RELEASE_UNCERTAIN);
    assert!(release.released && release.uncertain);
    let describe: DescribeResponse = round_trip(DESCRIBE_REVISION_THREE);
    assert_eq!(
        (describe.protocol_revision, describe.session_idle_ms),
        (3, 30_000)
    );
}

#[test]
fn context_keeps_its_field_numbers() {
    let context: Context = round_trip(CONTEXT);

    assert_eq!(
        (context.epoch, context.session_id, context.sequence),
        (1, 2, 3)
    );
}

#[test]
fn bytes_value_is_the_fifth_oneof_member() {
    let value: Value = round_trip(VALUE_BYTES);

    assert!(
        matches!(value.kind, Some(Value_::Kind::Bytes(ref bytes)) if bytes.as_slice() == b"ab")
    );
}

#[test]
fn float_value_is_the_fourth_oneof_member() {
    let value: Value = round_trip(VALUE_FLOAT);

    assert!(matches!(value.kind, Some(Value_::Kind::Float(number)) if number == 1.5));
}

#[test]
fn failed_outcome_carries_uncertainty_in_field_four() {
    let outcome: Outcome = round_trip(OUTCOME_FAILED);

    assert!(outcome.uncertain);
    assert!(matches!(
        outcome.kind,
        Some(Outcome_::Kind::Failed(ref failure)) if failure.code.0 == 5 && failure.message.as_slice() == b"x"
    ));
}

#[test]
fn start_request_keeps_scalar_fields_and_nested_tuples() {
    let request: StartRequest = round_trip(START_REQUEST);

    assert_eq!((request.entry, request.lease_ms), (1, 500));
    assert_eq!(request.digest.as_slice(), &[0x01, 0x02]);
}

#[test]
fn query_outcome_response_keeps_its_state_and_availability() {
    let response: QueryOutcomeResponse = round_trip(QUERY_OUTCOME_RESPONSE);

    assert!(response.available);
    assert_eq!(response.state.0, 7);
}

#[test]
fn describe_response_keeps_its_limits_and_contract() {
    let response: DescribeResponse = round_trip(DESCRIBE_RESPONSE);

    assert_eq!(response.contract, "lua.execution.v1");
    assert_eq!((response.max_executions, response.has_child), (4, true));
}

#[test]
fn describe_response_reports_a_reserved_floor_when_no_usable_id_is_left() {
    // u64::MAX is reserved, so a floor of u64::MAX means every usable id has been seen.
    let response: DescribeResponse = round_trip(DESCRIBE_RESERVED_FLOOR);

    assert_eq!(response.session_floor, u64::MAX);
}

#[test]
fn describe_response_carries_the_revision_and_the_admission_window() {
    let response: DescribeResponse = round_trip(DESCRIBE_REVISION_TWO);

    assert_eq!(response.protocol_revision, 2);
    assert_eq!(response.admission_floor, 5);
    assert_eq!(response.retired_window, 256);
    assert_eq!(response.max_request_bytes, 1024);
}

#[test]
fn cancel_and_status_carry_the_disposition() {
    let cancel: CancelResponse = round_trip(CANCEL_RESPONSE);
    assert_eq!(cancel.state.0, 6);
    assert!(cancel.uncertain);

    let status: StatusResponse = round_trip(STATUS_RESPONSE);
    assert_eq!((status.state.0, status.turn), (8, 2));
    assert!(status.uncertain);
}

#[test]
fn a_query_without_an_outcome_still_carries_the_disposition() {
    let response: QueryOutcomeResponse = round_trip(QUERY_UNAVAILABLE_UNCERTAIN);

    assert!(!response.available);
    assert_eq!(response.state.0, 6);
    assert!(response.uncertain);
}
