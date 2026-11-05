//! Bounded inspection of encoded `lua.execution.v1` messages before they are decoded.
//!
//! A frame-length cap alone does not bound what decoding allocates: a small frame can hold many tiny tuple values, and
//! each decoded value is larger than its two encoded bytes. The only repeated field of the schema is `Tuple.values`, so
//! this module walks the encoded bytes, without allocating, and counts the tuple values of each message that can carry
//! tuples. A message is decoded only after its size and its value counts are within bounds. Decoded memory is then at
//! most the frame size plus `max_items` decoded values per tuple, plus a constant per message.

/// Which message the bytes encode, as far as tuple fields are concerned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shape {
    /// A message with no tuple field.
    Plain,
    /// `StartRequest`: `captures` (4) and `arguments` (5) are tuples.
    Start,
    /// `ResumeRequest`: `values` (4) is a tuple.
    Resume,
    /// `QueryOutcomeResponse`: `outcome` (4) holds a `yielded` (1) or `returned` (2) tuple.
    QueryReply,
}

impl Shape {
    /// The request shape of execution method `name`, such as `Start`.
    pub fn of_request(name: &str) -> Self {
        match name {
            "Start" => Self::Start,
            "Resume" => Self::Resume,
            _ => Self::Plain,
        }
    }
}

/// Why a frame was refused before decoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameError {
    /// The frame is longer than the receive bound.
    TooLarge {
        /// The bound.
        limit: usize,
    },
    /// One tuple field holds more values than the bound, counting every occurrence of the field.
    TooManyItems {
        /// The bound.
        limit: usize,
    },
    /// The bytes are not well-formed protobuf.
    Malformed,
}

impl core::fmt::Display for FrameError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::TooLarge { limit } => write!(f, "message is larger than {limit} bytes"),
            Self::TooManyItems { limit } => write!(f, "a tuple has more than {limit} values"),
            Self::Malformed => f.write_str("message is not well-formed protobuf"),
        }
    }
}

/// The smallest request bound that admits every request a node with these tuple limits and this chunk size must accept:
/// a full `Start` with two tuples, a full `Resume`, and a full `PutChunk`. The resource generator applies the same rule
/// to every envelope it accepts.
pub fn min_request_bytes(
    max_items: usize,
    max_total_bytes: usize,
    max_chunk_bytes: usize,
) -> usize {
    let tuple = 6 + max_items * 17 + max_total_bytes;
    let start = 81 + 2 * tuple;
    let resume = 52 + tuple;
    let chunk = 58 + max_chunk_bytes;
    start.max(resume).max(chunk).max(128)
}

/// Conservative reply bound for a full permitted outcome, its context and revision-3 receipt.
/// Describe's contract string is not covered; the receiver still applies the frame cap to every reply.
pub fn min_reply_bytes(max_items: usize, max_total_bytes: usize) -> usize {
    let tuple = max_items.saturating_mul(32).saturating_add(max_total_bytes);
    128usize.saturating_add(tuple.max(crate::lifecycle::MAX_DIAGNOSTIC_BYTES))
}

/// Checks `bytes` against `max_bytes` and, for each tuple field of `shape`, against `max_items` values. Nothing is
/// allocated.
pub fn check(
    shape: Shape,
    bytes: &[u8],
    max_bytes: usize,
    max_items: usize,
) -> Result<(), FrameError> {
    if bytes.len() > max_bytes {
        return Err(FrameError::TooLarge { limit: max_bytes });
    }
    match shape {
        Shape::Plain => walk(bytes, |_, _| Ok(())),
        Shape::Start => tuples_at(bytes, &[4, 5], max_items),
        Shape::Resume => tuples_at(bytes, &[4], max_items),
        Shape::QueryReply => {
            let mut yielded = 0usize;
            let mut returned = 0usize;
            walk(bytes, |field, value| {
                if field == 4 {
                    let Field::Bytes(outcome) = value else {
                        return Err(FrameError::Malformed);
                    };
                    walk(outcome, |inner, value| {
                        let counter = match inner {
                            1 => &mut yielded,
                            2 => &mut returned,
                            _ => return Ok(()),
                        };
                        let Field::Bytes(tuple) = value else {
                            return Err(FrameError::Malformed);
                        };
                        *counter = add_values(*counter, tuple, max_items)?;
                        Ok(())
                    })?;
                }
                Ok(())
            })
        }
    }
}

/// Counts the values of every tuple field in `fields`, separately per field.
fn tuples_at(bytes: &[u8], fields: &[u32], max_items: usize) -> Result<(), FrameError> {
    let mut counts = [0usize; 2];
    walk(bytes, |field, value| {
        let Some(slot) = fields.iter().position(|&f| f == field) else {
            return Ok(());
        };
        let Field::Bytes(tuple) = value else {
            return Err(FrameError::Malformed);
        };
        counts[slot] = add_values(counts[slot], tuple, max_items)?;
        Ok(())
    })
}

/// Adds the number of `values` (field 1) entries in one encoded tuple to `count`.
fn add_values(mut count: usize, tuple: &[u8], max_items: usize) -> Result<usize, FrameError> {
    walk(tuple, |field, value| {
        if field == 1 {
            if !matches!(value, Field::Bytes(_)) {
                return Err(FrameError::Malformed);
            }
            count += 1;
            if count > max_items {
                return Err(FrameError::TooManyItems { limit: max_items });
            }
        }
        Ok(())
    })?;
    Ok(count)
}

enum Field<'a> {
    Scalar,
    Bytes(&'a [u8]),
}

/// Walks the top-level fields of one message and hands each to `visit`. Groups are refused.
fn walk<'a>(
    mut bytes: &'a [u8],
    mut visit: impl FnMut(u32, Field<'a>) -> Result<(), FrameError>,
) -> Result<(), FrameError> {
    while !bytes.is_empty() {
        let key = varint(&mut bytes)?;
        let field = u32::try_from(key >> 3).map_err(|_| FrameError::Malformed)?;
        if field == 0 {
            return Err(FrameError::Malformed);
        }
        let value = match key & 7 {
            0 => {
                varint(&mut bytes)?;
                Field::Scalar
            }
            1 => {
                take(&mut bytes, 8)?;
                Field::Scalar
            }
            2 => {
                let len =
                    usize::try_from(varint(&mut bytes)?).map_err(|_| FrameError::Malformed)?;
                Field::Bytes(take(&mut bytes, len)?)
            }
            5 => {
                take(&mut bytes, 4)?;
                Field::Scalar
            }
            _ => return Err(FrameError::Malformed),
        };
        visit(field, value)?;
    }
    Ok(())
}

fn take<'a>(bytes: &mut &'a [u8], len: usize) -> Result<&'a [u8], FrameError> {
    if len > bytes.len() {
        return Err(FrameError::Malformed);
    }
    let (head, rest) = bytes.split_at(len);
    *bytes = rest;
    Ok(head)
}

fn varint(bytes: &mut &[u8]) -> Result<u64, FrameError> {
    let mut value = 0u64;
    for index in 0..10 {
        let (&byte, rest) = bytes.split_first().ok_or(FrameError::Malformed)?;
        *bytes = rest;
        value |= u64::from(byte & 0x7f) << (7 * index);
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(FrameError::Malformed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec;
    use crate::value::{Limits, Tuple, Value};
    use crate::wire::v1;
    use alloc::vec;
    use alloc::vec::Vec;

    fn start(captures: usize, arguments: usize) -> Vec<u8> {
        let limits = Limits::new(1000, 8, 64);
        let mut request = v1::StartRequest::default();
        request.set_captures(
            Tuple::new(vec![Value::Nil; captures], limits)
                .expect("fits")
                .to_wire(),
        );
        request.set_arguments(
            Tuple::new(vec![Value::Integer(1); arguments], limits)
                .expect("fits")
                .to_wire(),
        );
        codec::encode(&request)
    }

    #[test]
    fn a_start_within_bounds_passes_and_each_tuple_is_counted_on_its_own() {
        let bytes = start(8, 8);
        assert_eq!(check(Shape::Start, &bytes, 1024, 8), Ok(()));
        assert_eq!(
            check(Shape::Start, &start(9, 0), 1024, 8),
            Err(FrameError::TooManyItems { limit: 8 })
        );
    }

    #[test]
    fn a_small_frame_full_of_tiny_values_is_refused_before_decoding() {
        // 400 nil values encode to 1,600 bytes but decode to 400 heap values.
        let bytes = start(400, 0);
        assert!(bytes.len() < 2048);
        assert_eq!(
            check(Shape::Start, &bytes, 2048, 8),
            Err(FrameError::TooManyItems { limit: 8 })
        );
    }

    #[test]
    fn a_repeated_tuple_field_is_counted_across_its_occurrences() {
        // Protobuf merges a repeated message field, so two `captures` of five values decode as ten.
        let mut bytes = start(5, 0);
        let again = start(5, 0);
        bytes.extend_from_slice(&again);
        assert_eq!(
            check(Shape::Start, &bytes, 4096, 8),
            Err(FrameError::TooManyItems { limit: 8 })
        );
    }

    #[test]
    fn an_oversized_frame_is_refused_by_length() {
        let bytes = start(1, 0);
        assert_eq!(
            check(Shape::Start, &bytes, bytes.len() - 1, 8),
            Err(FrameError::TooLarge {
                limit: bytes.len() - 1
            })
        );
    }

    #[test]
    fn a_query_reply_counts_the_returned_tuple() {
        let limits = Limits::new(100, 8, 64);
        let outcome = v1::Outcome {
            r#kind: Some(v1::Outcome_::Kind::Returned(
                Tuple::new(vec![Value::Nil; 20], limits)
                    .expect("fits")
                    .to_wire(),
            )),
            ..v1::Outcome::default()
        };
        let mut reply = v1::QueryOutcomeResponse::default();
        reply.set_outcome(outcome);
        let bytes = codec::encode(&reply);
        assert_eq!(check(Shape::QueryReply, &bytes, 1024, 20), Ok(()));
        assert_eq!(
            check(Shape::QueryReply, &bytes, 1024, 19),
            Err(FrameError::TooManyItems { limit: 19 })
        );
    }

    #[test]
    fn the_request_bound_admits_the_largest_start_resume_and_chunk() {
        let (items, total, chunk) = (8usize, 512usize, 256usize);
        let bound = min_request_bytes(items, total, chunk);
        let limits = Limits::new(items, total, total);
        let full = || {
            let mut values = vec![Value::Integer(i64::MIN); items - 1];
            values.push(Value::Bytes(vec![0xff; total]));
            Tuple::new(values, limits).expect("fits").to_wire()
        };
        let context = v1::Context {
            r#epoch: u64::MAX,
            r#session_id: u64::MAX - 1,
            r#sequence: u64::MAX,
        };
        let mut start = v1::StartRequest {
            r#digest: vec![0xff; 32],
            r#entry: u32::MAX,
            r#lease_ms: u32::MAX,
            ..v1::StartRequest::default()
        };
        start.set_context(context);
        start.set_captures(full());
        start.set_arguments(full());
        assert!(codec::encode(&start).len() <= bound);
        let mut resume = v1::ResumeRequest {
            r#execution_id: u64::MAX,
            r#turn: u32::MAX,
            ..v1::ResumeRequest::default()
        };
        resume.set_context(context);
        resume.set_values(full());
        assert!(codec::encode(&resume).len() <= bound);
        let mut put = v1::PutChunkRequest {
            r#upload_id: u64::MAX,
            r#offset: u32::MAX,
            r#data: vec![0xff; chunk],
            ..v1::PutChunkRequest::default()
        };
        put.set_context(context);
        assert!(codec::encode(&put).len() <= bound);
    }

    #[test]
    fn revision_three_reply_bound_covers_full_outcome_and_receipt() {
        let (items, total) = (8, 512);
        let mut values = vec![Value::Integer(i64::MIN); items - 1];
        values.push(Value::Bytes(vec![0xff; total]));
        let tuple = Tuple::new(values, Limits::new(items, total, total))
            .unwrap()
            .to_wire();
        let mut response = v1::QueryOutcomeResponse {
            r#state: v1::State::Failed,
            r#available: true,
            r#uncertain: true,
            ..Default::default()
        };
        response.set_context(v1::Context {
            r#epoch: u64::MAX,
            r#session_id: u64::MAX,
            r#sequence: u64::MAX,
        });
        response.set_last_mutation(v1::MutationReceipt {
            r#sequence: u64::MAX,
            r#fingerprint: vec![0xff; 32],
        });
        response.set_outcome(v1::Outcome {
            r#kind: Some(v1::Outcome_::Kind::Returned(tuple)),
            r#uncertain: true,
        });
        assert!(codec::encode(&response).len() <= min_reply_bytes(items, total));
        response.set_outcome(v1::Outcome {
            r#kind: Some(v1::Outcome_::Kind::Failed(v1::Failure {
                r#code: v1::FailureCode::BudgetExhausted,
                r#message: vec![0xff; crate::lifecycle::MAX_DIAGNOSTIC_BYTES],
            })),
            r#uncertain: true,
        });
        assert!(codec::encode(&response).len() <= min_reply_bytes(items, total));
    }

    #[test]
    fn malformed_bytes_never_panic() {
        let mut state = 0x2545_f491_4f6c_dd1du64;
        for len in 0..300 {
            let mut bytes = Vec::with_capacity(len);
            for _ in 0..len {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                bytes.push(state as u8);
            }
            for shape in [Shape::Plain, Shape::Start, Shape::Resume, Shape::QueryReply] {
                let _ = check(shape, &bytes, 4096, 8);
            }
        }
        assert_eq!(
            check(Shape::Plain, &[0x80], 16, 8),
            Err(FrameError::Malformed)
        );
        assert_eq!(
            check(Shape::Plain, &[0x0b], 16, 8),
            Err(FrameError::Malformed)
        );
        assert_eq!(
            check(Shape::Plain, &[0x0a, 0x05, 0x00], 16, 8),
            Err(FrameError::Malformed)
        );
    }
}
