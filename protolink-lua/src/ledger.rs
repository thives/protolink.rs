//! Admission of calls on one link, keyed by session.
//!
//! Each concurrent call carries a `session_id`. Within a session, mutating calls carry a `sequence` that increases by
//! one. An identical retransmission returns the recorded answer. A changed payload with the same sequence is a
//! conflict. Any other sequence is out of order. Status and query calls use sequence 0 and never reach the ledger.
//! A session ends in one of two ways. Its final call finishes it, and its answer moves to the final-answer table for
//! replay. Or it goes idle: no call touches it for an idle timeout, and `tick` releases it, without keeping an answer. A
//! session that holds a reservation is never idle: see "Reservations and idle expiry" below.
//! Either way the session's slot is freed and its id is retired. A later call on a retired id is refused and never runs
//! again. A final call's identical retransmission is still answered while its entry is in the table.
//!
//! # Refusals after a session ended
//!
//! A call on an id that is not live is refused in one of three ways, and which one depends on how much the ledger still
//! remembers. None of them runs the call, and none of them says whether an earlier attempt ran.
//!
//! | The id is | Answer | Why |
//! |---|---|---|
//! | in the final-answer table, same sequence and payload | replay of the recorded answer | the entry is still held |
//! | in the final-answer table, same sequence, other payload | [`Admission::Conflict`] | the entry proves the payload differs |
//! | in the final-answer table, other sequence | [`Admission::Retired`] | the session ended |
//! | retired inside the window | [`Admission::Retired`] | its bit is set |
//! | below the admission floor, not live | [`Admission::BelowWindow`] | the window moved past it, and its bit is gone |
//!
//! A final call whose entry was evicted is refused as `Retired` while its id is still inside the window, and as
//! `BelowWindow` once the window moved past it. Both refusals are terminal, and neither reruns the call. A caller that
//! needs to know whether the call ran reconciles through a read, not through a retry.
//!
//! # Bounded retirement
//!
//! Retired ids are remembered in a fixed-width window of `W` ids, as one bit each, above a monotonic admission floor.
//! The memory is `ceil(W / 64)` words, whatever the number of sessions that have ended and however sparse their ids are.
//!
//! 1. A live session is checked first. Its replay and sequence rules hold even after the window has moved past its id.
//! 2. A non-live id below the floor is refused as [`Admission::BelowWindow`], whether it ran before or was merely
//!    delayed. It never becomes new work.
//! 3. Inside the window, a retired id is refused as [`Admission::Retired`]. An id that has not been used yet is admitted.
//! 4. A new session whose id is above the window moves the window up so that the id is its top, in at most `W` steps
//!    however far it jumps. Ids that fall below the floor are refused from then on.
//! 5. Releasing a live id that is already below the floor needs no record, because it is refused anyway.
//!
//! `u64::MAX` is reserved. It is never a session: every call that carries it is refused, and it is never recorded. So
//! the highest recorded id is at most `u64::MAX - 1`, and the next-id suggestion never overflows. When the suggestion
//! reaches `u64::MAX`, every usable id has been recorded, and no new session can start.
//!
//! # Replay storage
//!
//! The answer of a mutating call is recorded for replay. The node reserves the session slot and the answer storage
//! with [`Ledger::reserve`] before it changes any state, and then either records the answer with [`Ledger::record`] or
//! gives the reservation back with [`Ledger::abandon`].
//!
//! The answer is an [`Answer`], a fixed-capacity value of at most [`MAX_MUTATION_ANSWER_BYTES`] bytes. It can only be
//! built from a protocol response type whose encoded size is bounded at compile time, or from a commit digest of exactly
//! 32 bytes. So an oversized answer **cannot be constructed**: the check happens when the code is compiled, not after an
//! effect ran. [`Ledger::new`] refuses a storage bound below [`MAX_MUTATION_ANSWER_BYTES`], and the storage of a
//! reservation is allocated at `reserve`, before the effect. Recording therefore never allocates and never fails for
//! lack of room, and a call that changed state always has its answer recorded.
//!
//! A session holds at most one reservation. A second call on it is refused as [`Admission::InFlight`] until the first is
//! recorded or abandoned. Every reservation carries a ticket that the session stores, so a stale reservation, one whose
//! session has since been released, can neither overwrite a newer record nor recreate the session.
//!
//! # Reservations and idle expiry
//!
//! A reservation is activity. [`Ledger::reserve`] stamps the session with the clock, and [`Ledger::record`] and
//! [`Ledger::finish`] stamp it again. **[`Ledger::tick`] never releases a session that holds a reservation**, so a call
//! that is running when the clock moves can still record its answer, and a final call can still file its answer for
//! replay. [`Ledger::abandon`] gives the reservation back and leaves the timestamp where the reservation put it: a
//! refused call is activity, but not more than that, so the session expires from the moment of the refused call.
//!
//! The ledger cannot see a reservation that was dropped without being completed. Such a reservation would pin its slot.
//! [`Ledger::reservations`] counts them, and [`Ledger::release`] ends the session and makes the dropped reservation
//! stale. The node holds each reservation within one call, so it never leaves one behind.
//!
//! # Final answers
//!
//! A call that ends its session (a final call) keeps its answer in a fixed table of [`FINAL_REPLAY_SLOTS`] entries, which
//! [`Ledger::finish`] fills and evicts oldest first. A retransmission of a final call is answered from the table while
//! its entry is there. Once an entry is evicted, the call is refused as retired or, after the window moved on, as below
//! the admission floor, and is never run again. Eviction is by count, not by time, so the bound holds whatever the
//! clock does. The table size is part of the protocol: a node
//! advertises it as `DescribeResponse.final_replay_slots`.

use alloc::collections::{BTreeMap, VecDeque};
use alloc::vec;
use alloc::vec::Vec;
use core::sync::atomic::{AtomicUsize, Ordering};

use micropb::{MessageEncode, PbEncoder, Tag, WIRE_TYPE_LEN};
use sha2::{Digest, Sha256};

use crate::wire::v1::{self, Context};

/// Length of a payload digest.
pub const DIGEST_BYTES: usize = 32;

/// Source of ledger identities. Each ledger takes the next value when it is created, and every reservation names the
/// ledger that issued it, so a reservation can act only on that ledger. The counter is process-wide and wraps after
/// `usize::MAX` ledgers; a node creates one ledger, so the wrap is not reachable in practice on a device.
static NEXT_LEDGER: AtomicUsize = AtomicUsize::new(0);

/// The session id that no session may use. A call that carries it is refused, and the ledger never records it.
pub const RESERVED_SESSION_ID: u64 = u64::MAX;

/// How many final answers the ledger keeps for replay. A final call whose answer has been evicted is refused, and it is
/// never run again. The node advertises this value as `DescribeResponse.final_replay_slots`.
pub const FINAL_REPLAY_SLOTS: usize = 8;

/// The largest encoded answer of any mutating call of `lua.execution.v1`: `CommitArtifactResponse` with a full context
/// and a 32-byte digest. A ledger refuses a storage bound below it, and [`Answer`] cannot exceed it.
pub const MAX_MUTATION_ANSWER_BYTES: usize = 72;

mod sealed {
    pub trait Sealed {}
}

/// A protocol response whose encoded size is bounded at compile time by [`MAX_MUTATION_ANSWER_BYTES`]. Only the
/// response types of the mutating calls implement it, and the trait cannot be implemented elsewhere. The commit response
/// carries a bytes field with no static bound, so it is built by [`Answer::commit`] instead.
pub trait BoundedAnswer: MessageEncode + sealed::Sealed {
    /// The largest encoded size of the type, from its schema.
    const BOUND: usize;
}

macro_rules! bounded_answers {
    ($($response:ty),* $(,)?) => {$(
        impl sealed::Sealed for $response {}
        impl BoundedAnswer for $response {
            const BOUND: usize = match <$response as MessageEncode>::MAX_SIZE {
                Ok(size) => size,
                Err(_) => panic!("a bounded answer must have a static size"),
            };
        }
    )*};
}

bounded_answers!(
    v1::BeginUploadResponse,
    v1::PutChunkResponse,
    v1::StartResponse,
    v1::ResumeResponse,
    v1::CancelResponse,
    v1::ReleaseResponse,
);

/// The encoded answer of one mutating call. It holds at most [`MAX_MUTATION_ANSWER_BYTES`] bytes in the value itself, so
/// nothing allocates when it is built or recorded, and it cannot be too large for the ledger's storage.
#[derive(Clone, PartialEq, Eq)]
pub struct Answer {
    len: u8,
    bytes: [u8; MAX_MUTATION_ANSWER_BYTES],
}

impl core::fmt::Debug for Answer {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("Answer").field(&self.as_slice()).finish()
    }
}

impl Answer {
    /// Encodes a bounded response. A response type larger than the bound fails to compile.
    pub fn encode<M: BoundedAnswer>(message: &M) -> Self {
        const { assert!(MAX_MUTATION_ANSWER_BYTES <= u8::MAX as usize) };
        let () = const {
            assert!(
                M::BOUND <= MAX_MUTATION_ANSWER_BYTES,
                "a bounded answer exceeds the protocol bound"
            )
        };
        Self::write(message)
    }

    /// Encodes the answer of a commit: the context and the 32-byte digest of the stored artifact. The digest has a fixed
    /// length, so the size is bounded even though the schema field is not.
    ///
    /// Nothing allocates. The bytes are those of `CommitArtifactResponse { Context context = 1; bytes digest = 2; }`, written
    /// field by field from the borrowed digest. The generated message would need an owned `Vec` for the digest, and building
    /// one after the artifact is stored would be an allocation after the effect. A test compares these bytes with the
    /// generated encoding.
    pub fn commit(context: Context, digest: &[u8; DIGEST_BYTES]) -> Self {
        let mut bytes = [0u8; MAX_MUTATION_ANSWER_BYTES];
        let mut free: &mut [u8] = &mut bytes;
        let mut encoder = PbEncoder::new(&mut free);
        let written = encoder
            .encode_tag(Tag::from_parts(1, WIRE_TYPE_LEN))
            .and_then(|()| encoder.encode_varint32(context.compute_size() as u32))
            .and_then(|()| context.encode(&mut encoder))
            .and_then(|()| encoder.encode_tag(Tag::from_parts(2, WIRE_TYPE_LEN)))
            .and_then(|()| encoder.encode_bytes(digest));
        // The bound is fixed: a context is at most 33 bytes and the digest field 34, so 69 fits in 72. A failure here
        // would be a defect in those widths, and `every_mutation_answer_fits_the_protocol_bound` checks them.
        written.expect("a commit answer fits its buffer");
        let len = MAX_MUTATION_ANSWER_BYTES - free.len();
        Self {
            len: len as u8,
            bytes,
        }
    }

    fn write<M: MessageEncode>(message: &M) -> Self {
        let mut bytes = [0u8; MAX_MUTATION_ANSWER_BYTES];
        let mut free: &mut [u8] = &mut bytes;
        let mut encoder = PbEncoder::new(&mut free);
        // The bound is checked at compile time for bounded responses, so this cannot fail.
        message
            .encode(&mut encoder)
            .expect("an answer within the protocol bound fits its buffer");
        let len = MAX_MUTATION_ANSWER_BYTES - free.len();
        Self {
            len: len as u8,
            bytes,
        }
    }

    /// The encoded bytes.
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[..usize::from(self.len)]
    }

    /// Builds an answer from raw bytes, for tests of the ledger itself. Production answers come from [`Answer::encode`]
    /// and [`Answer::commit`], which cannot fail.
    #[cfg(test)]
    pub(crate) fn raw(bytes: &[u8]) -> Self {
        assert!(
            bytes.len() <= MAX_MUTATION_ANSWER_BYTES,
            "test answer too large"
        );
        let mut stored = [0u8; MAX_MUTATION_ANSWER_BYTES];
        stored[..bytes.len()].copy_from_slice(bytes);
        Self {
            len: bytes.len() as u8,
            bytes: stored,
        }
    }
}

/// SHA-256 of a request payload. The sender digests its semantic fields, not transport framing.
pub fn payload_digest(payload: &[u8]) -> [u8; DIGEST_BYTES] {
    Sha256::digest(payload).into()
}

/// Revision-3 fingerprint: SHA-256 of `lua.execution.v1/mutation\0`, the method name's
/// length as a big-endian u32, its UTF-8 bytes, then the exact encoded request bytes.
/// Method names are the case-sensitive RPC names from the execution schema.
pub fn mutation_fingerprint(method: &str, request: &[u8]) -> [u8; DIGEST_BYTES] {
    let mut hash = Sha256::new();
    hash.update(b"lua.execution.v1/mutation\0");
    hash.update((method.len() as u32).to_be_bytes());
    hash.update(method.as_bytes());
    hash.update(request);
    hash.finalize().into()
}

/// A live session's last committed mutation, never an uncommitted reservation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Receipt {
    /// Committed sequence.
    pub sequence: u64,
    /// Method-bound fingerprint of the exact request.
    pub fingerprint: [u8; DIGEST_BYTES],
}

impl Receipt {
    /// Decodes only a full SHA-256 fingerprint; malformed evidence cannot reconcile a flow.
    pub fn from_wire(receipt: &crate::wire::v1::MutationReceipt) -> Option<Self> {
        Some(Self {
            sequence: receipt.r#sequence,
            fingerprint: receipt.r#fingerprint.as_slice().try_into().ok()?,
        })
    }
}

/// Result of checking a mutating call against the ledger.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Admission<'a> {
    /// The call is the next in its session and may run.
    New,
    /// An identical retransmission. The recorded answer is returned without running the call again.
    Replay(&'a [u8]),
    /// The sequence was already used with a different payload. It is refused and never runs.
    Conflict,
    /// The sequence skips ahead or the session is unknown and does not start at 1.
    OutOfOrder {
        /// The sequence the session expects next.
        expected: u64,
    },
    /// The call carries a different epoch. It belongs to a previous incarnation.
    StaleEpoch {
        /// The epoch the node currently runs in.
        current: u64,
    },
    /// The ledger has no room for another session. Live sessions are never evicted.
    Capacity,
    /// The session was released, by its final call or by the idle timeout. Its id never runs again.
    Retired,
    /// The id is below the admission floor and is not live. It is refused whether it ran before or was delayed, and it
    /// never runs. The caller may treat the call as definitely not admitted.
    BelowWindow {
        /// The lowest id a new session may use.
        floor: u64,
    },
    /// The session has used its last sequence number, so no later call can follow the one it recorded. Only the
    /// recorded call is answered, by replay or conflict. Reached only after 2^64 calls on one session.
    SequenceExhausted,
    /// The call carries `RESERVED_SESSION_ID`, which no session may use.
    Reserved,
    /// Another call on the same session is admitted and not yet recorded or abandoned. The call is refused and never
    /// runs. The caller retries once the first call has completed.
    InFlight,
}

/// Why a ledger cannot be built with the requested bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LedgerConfigError {
    /// The answer bound is smaller than the largest answer the protocol defines.
    AnswerBoundTooSmall {
        /// The smallest acceptable bound.
        required: usize,
    },
    /// The retirement window has no width.
    EmptyWindow,
    /// The ledger can hold no session.
    NoSessions,
}

#[derive(Debug)]
struct Session {
    /// The last admitted sequence. Zero while the first call is reserved but not recorded.
    sequence: u64,
    digest: [u8; DIGEST_BYTES],
    /// Recorded answer. Its capacity is reserved before the call runs.
    answer: Vec<u8>,
    /// Clock value of the last activity on the session: a reservation, a recorded answer, or a read that touched it.
    last_seen: u64,
    /// Ticket of the outstanding reservation, if one is held. A session holds at most one.
    reserved: Option<u64>,
}

/// The answer of a final call, kept so that its retransmission is replayed.
#[derive(Debug)]
struct FinalAnswer {
    session_id: u64,
    sequence: u64,
    digest: [u8; DIGEST_BYTES],
    answer: Vec<u8>,
}

/// A slot reserved for one admitted mutating call. Hand it back to [`Ledger::record`] or [`Ledger::finish`] after the
/// call ran, or to [`Ledger::abandon`] when the call was refused without changing any state.
#[derive(Debug, PartialEq, Eq)]
#[must_use = "a reservation must be recorded, finished or abandoned"]
pub struct Reservation {
    /// The ledger that issued the reservation. Only that ledger can record, finish or abandon it.
    ledger: usize,
    session_id: u64,
    sequence: u64,
    digest: [u8; DIGEST_BYTES],
    opened: bool,
    ticket: u64,
}

impl Reservation {
    /// The session the reservation is for.
    pub fn session_id(&self) -> u64 {
        self.session_id
    }
}

/// The fixed-width retirement window: one bit for each id in `[floor, floor + width)`, kept in a ring.
#[derive(Debug)]
struct Window {
    floor: u64,
    width: u64,
    bits: Vec<u64>,
}

impl Window {
    fn new(width: u64) -> Self {
        let words = width.div_ceil(64) as usize;
        Self {
            floor: 0,
            width,
            bits: vec![0; words],
        }
    }

    fn position(&self, id: u64) -> (usize, u64) {
        let slot = id % self.width;
        ((slot / 64) as usize, 1u64 << (slot % 64))
    }

    /// True when `id` is inside the window.
    fn holds(&self, id: u64) -> bool {
        id >= self.floor && id - self.floor < self.width
    }

    fn is_retired(&self, id: u64) -> bool {
        if !self.holds(id) {
            return false;
        }
        let (word, mask) = self.position(id);
        self.bits[word] & mask != 0
    }

    fn retire(&mut self, id: u64) {
        if self.holds(id) {
            let (word, mask) = self.position(id);
            self.bits[word] |= mask;
        }
    }

    /// Moves the window up so that `id` is inside it. Positions that leave the window are cleared, which takes at most
    /// `width` steps however far the window moves.
    fn cover(&mut self, id: u64) {
        if id < self.floor || id - self.floor < self.width {
            return;
        }
        // `id` is at most `u64::MAX - 1`, so `id + 1` cannot overflow.
        let floor = id + 1 - self.width;
        let leaving = (floor - self.floor).min(self.width);
        for offset in 0..leaving {
            let (word, mask) = self.position(self.floor + offset);
            self.bits[word] &= !mask;
        }
        self.floor = floor;
    }
}

/// Session ledger for one node incarnation.
#[derive(Debug)]
pub struct Ledger {
    /// This ledger's identity, from `NEXT_LEDGER`.
    id: usize,
    epoch: u64,
    max_sessions: usize,
    max_answer_bytes: usize,
    sessions: BTreeMap<u64, Session>,
    window: Window,
    /// The highest session id the ledger has recorded, live or retired.
    highest: u64,
    /// The clock, as last set by `tick`.
    now: u64,
    /// Whether `tick` has run. Until then the clock has no meaning, so the first tick starts it.
    ticked: bool,
    /// Ticket of the next reservation. Tickets are never reused within one ledger.
    tickets: u64,
    /// Answers of final calls, oldest first, at most `FINAL_REPLAY_SLOTS`.
    finals: VecDeque<FinalAnswer>,
}

impl Ledger {
    /// Creates an empty ledger for `epoch` that holds at most `max_sessions` live sessions, keeps answers of at most
    /// `max_answer_bytes`, and remembers retired ids in a window of `retired_window` ids.
    pub fn new(
        epoch: u64,
        max_sessions: usize,
        max_answer_bytes: usize,
        retired_window: u64,
    ) -> Result<Self, LedgerConfigError> {
        if max_answer_bytes < MAX_MUTATION_ANSWER_BYTES {
            return Err(LedgerConfigError::AnswerBoundTooSmall {
                required: MAX_MUTATION_ANSWER_BYTES,
            });
        }
        if retired_window == 0 {
            return Err(LedgerConfigError::EmptyWindow);
        }
        if max_sessions == 0 {
            return Err(LedgerConfigError::NoSessions);
        }
        Ok(Self {
            id: NEXT_LEDGER.fetch_add(1, Ordering::Relaxed),
            epoch,
            max_sessions,
            max_answer_bytes,
            sessions: BTreeMap::new(),
            window: Window::new(retired_window),
            highest: 0,
            now: 0,
            ticked: false,
            tickets: 0,
            finals: VecDeque::with_capacity(FINAL_REPLAY_SLOTS),
        })
    }

    /// The epoch this ledger serves.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Checks a mutating call without changing the ledger.
    pub fn admit(&self, context: &Context, digest: &[u8; DIGEST_BYTES]) -> Admission<'_> {
        if context.epoch != self.epoch {
            return Admission::StaleEpoch {
                current: self.epoch,
            };
        }
        if context.session_id == RESERVED_SESSION_ID {
            return Admission::Reserved;
        }
        match self.sessions.get(&context.session_id) {
            None => {
                // A final call is answered from its table entry, whatever the window has done since.
                if let Some(last) = self
                    .finals
                    .iter()
                    .find(|last| last.session_id == context.session_id)
                {
                    return if context.sequence != last.sequence {
                        Admission::Retired
                    } else if &last.digest == digest {
                        Admission::Replay(&last.answer)
                    } else {
                        Admission::Conflict
                    };
                }
                if context.session_id < self.window.floor {
                    Admission::BelowWindow {
                        floor: self.window.floor,
                    }
                } else if self.window.is_retired(context.session_id) {
                    Admission::Retired
                } else if context.sequence != 1 {
                    Admission::OutOfOrder { expected: 1 }
                } else if self.sessions.len() >= self.max_sessions {
                    Admission::Capacity
                } else {
                    Admission::New
                }
            }
            Some(session) => {
                if session.reserved.is_some() {
                    return Admission::InFlight;
                }
                // Replay and conflict come first. The successor of the recorded sequence is computed only after them, so
                // the last admitted call is still answered when no successor exists.
                if context.sequence == session.sequence && &session.digest == digest {
                    Admission::Replay(&session.answer)
                } else if context.sequence == session.sequence {
                    Admission::Conflict
                } else {
                    match session.sequence.checked_add(1) {
                        None => Admission::SequenceExhausted,
                        Some(expected) if context.sequence == expected => Admission::New,
                        Some(expected) => Admission::OutOfOrder { expected },
                    }
                }
            }
        }
    }

    /// Reserves the slot and the answer storage of an admitted call, before the call changes any state. It returns
    /// the refusal instead when the call is not [`Admission::New`]; a replay is answered by [`Ledger::admit`].
    pub fn reserve(
        &mut self,
        context: &Context,
        digest: [u8; DIGEST_BYTES],
    ) -> Result<Reservation, Admission<'static>> {
        match self.admit(context, &digest) {
            Admission::New => {}
            Admission::Replay(_) => return Err(Admission::Conflict),
            other => return Err(detach(other)),
        }
        self.tickets += 1;
        let ticket = self.tickets;
        let opened = !self.sessions.contains_key(&context.session_id);
        let capacity = self.max_answer_bytes;
        let now = self.now;
        let session = self
            .sessions
            .entry(context.session_id)
            .or_insert_with(|| Session {
                sequence: 0,
                digest: [0; DIGEST_BYTES],
                answer: Vec::with_capacity(capacity),
                last_seen: now,
                reserved: None,
            });
        // A reservation is activity. `tick` also skips the session while the reservation is held.
        session.last_seen = now;
        session.reserved = Some(ticket);
        Ok(Reservation {
            ledger: self.id,
            session_id: context.session_id,
            sequence: context.sequence,
            digest,
            opened,
            ticket,
        })
    }

    /// Records the answer of a reserved call and keeps the session live. Returns `false`, and changes nothing, when the
    /// reservation is stale: its session was released after it was taken. A stale reservation never recreates a
    /// session. A reservation issued by another ledger is refused the same way, even when its session and ticket match
    /// one held here. The node holds each reservation within one call, so this does not happen there.
    #[must_use]
    pub fn record(&mut self, reservation: Reservation, answer: &Answer) -> bool {
        if reservation.ledger != self.id {
            return false;
        }
        let now = self.now;
        let Some(session) = self.sessions.get_mut(&reservation.session_id) else {
            return false;
        };
        if session.reserved != Some(reservation.ticket) {
            return false;
        }
        session.reserved = None;
        session.sequence = reservation.sequence;
        session.digest = reservation.digest;
        // The storage was allocated by `reserve`, and the answer cannot exceed it, so this never allocates.
        session.answer.clear();
        session.answer.extend_from_slice(answer.as_slice());
        session.last_seen = now;
        if reservation.opened {
            // Only a session that really started moves the window, so a refused first call changes nothing.
            self.window.cover(reservation.session_id);
            self.highest = self.highest.max(reservation.session_id);
        }
        true
    }

    /// Records the answer of a final call and ends its session. The answer moves into the final-answer table, where it
    /// replays its retransmission. Returns `false` when the reservation is stale, and then nothing changes.
    #[must_use]
    pub fn finish(&mut self, reservation: Reservation, answer: &Answer) -> bool {
        let session_id = reservation.session_id;
        if !self.record(reservation, answer) {
            return false;
        }
        if let Some(session) = self.sessions.remove(&session_id) {
            if self.finals.len() == FINAL_REPLAY_SLOTS {
                self.finals.pop_front();
            }
            self.finals.push_back(FinalAnswer {
                session_id,
                sequence: session.sequence,
                digest: session.digest,
                answer: session.answer,
            });
        }
        self.release(session_id);
        true
    }

    /// Gives back a reservation whose call was refused before it changed anything. A session opened by the reservation
    /// is removed again; its id stays unused, so the caller may retry it. A stale reservation changes nothing.
    pub fn abandon(&mut self, reservation: Reservation) {
        if reservation.ledger != self.id {
            return;
        }
        let Some(session) = self.sessions.get_mut(&reservation.session_id) else {
            return;
        };
        if session.reserved != Some(reservation.ticket) {
            return;
        }
        session.reserved = None;
        if reservation.opened && session.sequence == 0 {
            self.sessions.remove(&reservation.session_id);
        }
    }

    /// Returns existing committed metadata without creating or touching a session.
    pub fn receipt(&self, session_id: u64) -> Option<Receipt> {
        self.sessions.get(&session_id).and_then(|session| {
            (session.sequence != 0).then_some(Receipt {
                sequence: session.sequence,
                fingerprint: session.digest,
            })
        })
    }

    /// Records activity on session `session_id` at the current clock. A read calls this, so a caller that keeps polling
    /// keeps its session alive.
    pub fn touch(&mut self, session_id: u64) {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            session.last_seen = self.now;
        }
    }

    /// Moves the clock to `now` and releases every session with no call for at least `idle`. Returns the released ids,
    /// so the caller can discard what they held. A clock that does not advance expires nothing early. A session that
    /// holds a reservation is never released: its call is running, and it can still record or finish.
    ///
    /// The first tick starts the clock. Calls made before it count as made at that tick, because the node cannot know
    /// when they were made.
    pub fn tick(&mut self, now: u64, idle: u64) -> Vec<u64> {
        if !self.ticked {
            self.ticked = true;
            for session in self.sessions.values_mut() {
                session.last_seen = now;
            }
        }
        self.now = self.now.max(now);
        let now = self.now;
        let idle_ids: Vec<u64> = self
            .sessions
            .iter()
            .filter(|(_, session)| {
                session.reserved.is_none() && now.saturating_sub(session.last_seen) >= idle
            })
            .map(|(&id, _)| id)
            .collect();
        for &id in &idle_ids {
            self.release(id);
        }
        idle_ids
    }

    /// Frees the slot of session `session_id` and retires its id, so no call on it runs again. The reserved id is never a
    /// session, so releasing it does nothing. An id below the floor needs no record, since it is refused anyway.
    pub fn release(&mut self, session_id: u64) {
        if session_id == RESERVED_SESSION_ID {
            return;
        }
        self.sessions.remove(&session_id);
        self.window.cover(session_id);
        self.window.retire(session_id);
        self.highest = self.highest.max(session_id);
    }

    /// Number of reservations held and not yet recorded, finished or abandoned. At most the number of live sessions.
    pub fn reservations(&self) -> usize {
        self.sessions
            .values()
            .filter(|session| session.reserved.is_some())
            .count()
    }

    /// Number of sessions currently recorded.
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// True when session `session_id` is live.
    pub fn is_live(&self, session_id: u64) -> bool {
        self.sessions.contains_key(&session_id)
    }

    /// The first session id this ledger has never recorded: the suggested start for a caller's ids. A caller that draws
    /// its ids at or above it never reuses an id the node has recorded.
    ///
    /// `RESERVED_SESSION_ID` is never a session. When the suggestion equals it, every usable id has been recorded, and no
    /// new session can start. Live sessions are still served.
    pub fn session_floor(&self) -> u64 {
        // `highest` is never `RESERVED_SESSION_ID`, so this sum cannot overflow.
        self.highest + 1
    }

    /// The lowest id a new session may use. A non-live id below it is refused as [`Admission::BelowWindow`].
    pub fn admission_floor(&self) -> u64 {
        self.window.floor
    }

    /// The width of the retirement window, in ids.
    pub fn retired_window(&self) -> u64 {
        self.window.width
    }

    /// The number of final answers the ledger keeps for replay.
    pub fn final_replay_slots(&self) -> usize {
        FINAL_REPLAY_SLOTS
    }

    /// Bytes the retirement window keeps, whatever the number of retired sessions.
    pub fn retirement_bytes(&self) -> usize {
        self.window.bits.len() * core::mem::size_of::<u64>()
    }
}

fn detach(admission: Admission<'_>) -> Admission<'static> {
    match admission {
        Admission::New => Admission::New,
        Admission::Replay(_) | Admission::Conflict => Admission::Conflict,
        Admission::OutOfOrder { expected } => Admission::OutOfOrder { expected },
        Admission::StaleEpoch { current } => Admission::StaleEpoch { current },
        Admission::Capacity => Admission::Capacity,
        Admission::Retired => Admission::Retired,
        Admission::BelowWindow { floor } => Admission::BelowWindow { floor },
        Admission::SequenceExhausted => Admission::SequenceExhausted,
        Admission::Reserved => Admission::Reserved,
        Admission::InFlight => Admission::InFlight,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    const WINDOW: u64 = 64;

    fn ctx(epoch: u64, session: u64, sequence: u64) -> Context {
        Context {
            epoch,
            session_id: session,
            sequence,
        }
    }

    fn ledger(epoch: u64, sessions: usize) -> Ledger {
        Ledger::new(epoch, sessions, 128, WINDOW).expect("valid bounds")
    }

    /// Reserves and records one call, as the node does.
    fn commit(ledger: &mut Ledger, context: &Context, digest: [u8; 32], answer: &[u8]) {
        let reservation = ledger.reserve(context, digest).expect("admitted");
        assert!(ledger.record(reservation, &Answer::raw(answer)));
    }

    #[test]
    fn bounds_below_the_protocol_answer_or_without_a_window_are_refused() {
        assert_eq!(
            Ledger::new(1, 4, MAX_MUTATION_ANSWER_BYTES - 1, 8).err(),
            Some(LedgerConfigError::AnswerBoundTooSmall {
                required: MAX_MUTATION_ANSWER_BYTES
            })
        );
        assert_eq!(
            Ledger::new(1, 4, MAX_MUTATION_ANSWER_BYTES, 0).err(),
            Some(LedgerConfigError::EmptyWindow)
        );
        assert_eq!(
            Ledger::new(1, 0, MAX_MUTATION_ANSWER_BYTES, 8).err(),
            Some(LedgerConfigError::NoSessions)
        );
    }

    #[test]
    fn first_call_of_a_session_must_be_sequence_one() {
        let ledger = ledger(9, 4);
        let digest = payload_digest(b"start");
        assert_eq!(
            ledger.admit(&ctx(9, 5, 2), &digest),
            Admission::OutOfOrder { expected: 1 }
        );
        assert_eq!(ledger.admit(&ctx(9, 5, 1), &digest), Admission::New);
    }

    #[test]
    fn revision_three_fingerprint_binds_method_and_exact_bytes() {
        let digest = mutation_fingerprint("Resume", b"request");
        assert_eq!(
            digest,
            [
                0xe0, 0x3e, 0x1e, 0xb8, 0xf5, 0x4c, 0x75, 0xeb, 0xd5, 0x73, 0x41, 0xc4, 0x36, 0xf9,
                0x68, 0x90, 0xae, 0xa0, 0x2a, 0x1e, 0xe9, 0x14, 0x7f, 0x69, 0xf9, 0xf9, 0x94, 0xd8,
                0x45, 0x49, 0x3b, 0x64
            ]
        );
        assert_ne!(digest, mutation_fingerprint("Cancel", b"request"));
        assert_ne!(digest, mutation_fingerprint("Resume", b"request!"));
    }

    #[test]
    fn identical_retransmission_replays_the_recorded_answer() {
        let mut ledger = ledger(9, 4);
        let digest = payload_digest(b"start");
        commit(&mut ledger, &ctx(9, 5, 1), digest, &[1, 2, 3]);
        assert_eq!(
            ledger.admit(&ctx(9, 5, 1), &digest),
            Admission::Replay(&[1, 2, 3])
        );
    }

    #[test]
    fn same_sequence_with_different_payload_conflicts() {
        let mut ledger = ledger(9, 4);
        commit(&mut ledger, &ctx(9, 5, 1), payload_digest(b"a"), &[]);
        assert_eq!(
            ledger.admit(&ctx(9, 5, 1), &payload_digest(b"b")),
            Admission::Conflict
        );
    }

    #[test]
    fn sequence_gaps_and_stale_epochs_are_refused() {
        let mut ledger = ledger(9, 4);
        let digest = payload_digest(b"a");
        commit(&mut ledger, &ctx(9, 5, 1), digest, &[]);
        assert_eq!(
            ledger.admit(&ctx(9, 5, 3), &digest),
            Admission::OutOfOrder { expected: 2 }
        );
        assert_eq!(
            ledger.admit(&ctx(8, 5, 2), &digest),
            Admission::StaleEpoch { current: 9 }
        );
    }

    #[test]
    fn a_reservation_holds_its_slot_and_an_abandoned_one_frees_it() {
        let mut ledger = ledger(1, 1);
        let digest = payload_digest(b"a");
        let held = ledger.reserve(&ctx(1, 1, 1), digest).expect("reserved");
        assert_eq!(
            ledger.admit(&ctx(1, 2, 1), &digest),
            Admission::Capacity,
            "the reserved slot is taken before the call runs"
        );
        ledger.abandon(held);
        assert_eq!(ledger.session_count(), 0);
        assert_eq!(
            ledger.admit(&ctx(1, 1, 1), &digest),
            Admission::New,
            "an abandoned first call can be retried under the same id"
        );
    }

    #[test]
    fn abandoning_a_later_call_keeps_the_recorded_answer() {
        let mut ledger = ledger(1, 2);
        let first = payload_digest(b"first");
        commit(&mut ledger, &ctx(1, 3, 1), first, &[7]);
        let second = payload_digest(b"second");
        let held = ledger.reserve(&ctx(1, 3, 2), second).expect("reserved");
        ledger.abandon(held);
        assert_eq!(ledger.admit(&ctx(1, 3, 1), &first), Admission::Replay(&[7]));
        assert_eq!(ledger.admit(&ctx(1, 3, 2), &second), Admission::New);
    }

    #[test]
    fn reserve_refuses_what_admit_refuses() {
        let mut ledger = ledger(1, 2);
        let digest = payload_digest(b"x");
        assert_eq!(
            ledger.reserve(&ctx(1, 3, 2), digest),
            Err(Admission::OutOfOrder { expected: 1 })
        );
        assert_eq!(
            ledger.reserve(&ctx(2, 3, 1), digest),
            Err(Admission::StaleEpoch { current: 1 })
        );
        commit(&mut ledger, &ctx(1, 3, 1), digest, &[]);
        assert_eq!(
            ledger.reserve(&ctx(1, 3, 1), digest),
            Err(Admission::Conflict),
            "a replay is answered by admit and never reserved"
        );
    }

    #[test]
    fn a_released_session_frees_its_slot_and_its_id_never_runs_again() {
        let mut ledger = ledger(1, 1);
        let digest = payload_digest(b"a");
        commit(&mut ledger, &ctx(1, 1, 1), digest, &[]);
        assert_eq!(ledger.admit(&ctx(1, 2, 1), &digest), Admission::Capacity);

        ledger.release(1);
        assert_eq!(ledger.session_count(), 0);
        assert_eq!(ledger.admit(&ctx(1, 2, 1), &digest), Admission::New);
        assert_eq!(ledger.admit(&ctx(1, 1, 1), &digest), Admission::Retired);
    }

    #[test]
    fn the_session_floor_is_above_every_session_the_ledger_has_recorded() {
        let mut ledger = ledger(1, 4);
        let digest = payload_digest(b"floor");
        assert_eq!(ledger.session_floor(), 1);
        commit(&mut ledger, &ctx(1, 3, 1), digest, &[]);
        assert_eq!(ledger.session_floor(), 4);
        commit(&mut ledger, &ctx(1, 7, 1), digest, &[]);
        ledger.release(7);
        assert_eq!(ledger.session_floor(), 8);
    }

    #[test]
    fn a_final_commit_on_one_session_leaves_another_live() {
        let mut ledger = ledger(1, 4);
        let digest = payload_digest(b"other");
        commit(&mut ledger, &ctx(1, 5, 1), digest, &[]);
        commit(&mut ledger, &ctx(1, 6, 1), digest, &[]);
        ledger.release(6);
        assert_eq!(ledger.session_count(), 1);
        assert_eq!(ledger.admit(&ctx(1, 5, 2), &digest), Admission::New);
        assert_eq!(ledger.admit(&ctx(1, 6, 2), &digest), Admission::Retired);
    }

    #[test]
    fn a_delayed_first_call_inside_the_window_is_still_admitted() {
        // The caller opened session 4 before session 9 ran, and session 4's first call has not arrived yet.
        let mut ledger = ledger(1, 4);
        let digest = payload_digest(b"low");
        commit(&mut ledger, &ctx(1, 9, 1), digest, &[]);
        ledger.release(9);
        assert_eq!(ledger.admit(&ctx(1, 4, 1), &digest), Admission::New);
        commit(&mut ledger, &ctx(1, 4, 1), digest, &[]);
        assert_eq!(ledger.admit(&ctx(1, 9, 1), &digest), Admission::Retired);
    }

    #[test]
    fn a_delayed_first_call_below_the_window_is_refused_and_never_runs() {
        let mut ledger = ledger(1, 4);
        let digest = payload_digest(b"late");
        let high = 4 + WINDOW;
        commit(&mut ledger, &ctx(1, high, 1), digest, &[]);
        assert_eq!(ledger.admission_floor(), high + 1 - WINDOW);
        assert_eq!(
            ledger.admit(&ctx(1, 4, 1), &digest),
            Admission::BelowWindow { floor: 5 },
            "never used, but below the floor"
        );
        assert_eq!(
            ledger.admit(&ctx(1, 5, 1), &digest),
            Admission::New,
            "the floor itself is inside the window"
        );
    }

    #[test]
    fn a_live_session_keeps_working_after_the_window_moves_past_it() {
        let mut ledger = ledger(1, 4);
        let first = payload_digest(b"first");
        commit(&mut ledger, &ctx(1, 2, 1), first, &[1]);
        commit(&mut ledger, &ctx(1, 2 + 10 * WINDOW, 1), first, &[]);
        assert!(ledger.admission_floor() > 2);
        assert_eq!(ledger.admit(&ctx(1, 2, 1), &first), Admission::Replay(&[1]));
        let second = payload_digest(b"second");
        assert_eq!(ledger.admit(&ctx(1, 2, 2), &second), Admission::New);
        commit(&mut ledger, &ctx(1, 2, 2), second, &[2]);
        ledger.release(2);
        assert_eq!(
            ledger.admit(&ctx(1, 2, 3), &second),
            Admission::BelowWindow {
                floor: ledger.admission_floor()
            },
            "released below the floor, so it needs no tombstone and is still refused"
        );
    }

    #[test]
    fn a_jump_larger_than_the_window_clears_every_old_position() {
        let mut ledger = ledger(1, 4);
        let digest = payload_digest(b"jump");
        for id in 1..=10 {
            commit(&mut ledger, &ctx(1, id, 1), digest, &[]);
            ledger.release(id);
        }
        let far = 1_000_000;
        commit(&mut ledger, &ctx(1, far, 1), digest, &[]);
        // Ids that share a ring position with the old retired ids are not mistaken for retired ones.
        for id in (far - WINDOW + 1)..far {
            assert_eq!(
                ledger.admit(&ctx(1, id, 1), &digest),
                Admission::New,
                "id {id}"
            );
        }
        assert_eq!(
            ledger.admit(&ctx(1, 10, 1), &digest),
            Admission::BelowWindow {
                floor: far + 1 - WINDOW
            }
        );
    }

    #[test]
    fn one_hundred_thousand_sparse_releases_use_fixed_retirement_storage() {
        let mut ledger = Ledger::new(1, 8, 128, 256).expect("valid");
        let before = ledger.retirement_bytes();
        let digest = payload_digest(b"sparse");
        let mut id = 1u64;
        for round in 0..100_000u64 {
            // Sparse ids: gaps of 1 to 37 between sessions, so many ids are never used.
            id += 1 + round % 37;
            commit(&mut ledger, &ctx(1, id, 1), digest, &[1]);
            ledger.release(id);
        }
        assert_eq!(
            ledger.retirement_bytes(),
            before,
            "retirement storage never grows"
        );
        assert_eq!(before, 256 / 8);
        assert_eq!(ledger.session_count(), 0);
        assert_eq!(ledger.admit(&ctx(1, id, 1), &digest), Admission::Retired);
        assert_eq!(ledger.admit(&ctx(1, id + 1, 1), &digest), Admission::New);
    }

    #[test]
    fn interleaved_allocations_for_several_nodes_stay_admissible_within_the_window() {
        // One process-wide counter feeds two nodes, so each node sees every other id.
        let mut a = ledger(1, 4);
        let mut b = ledger(2, 4);
        let digest = payload_digest(b"interleaved");
        for id in 1..=1_000u64 {
            let (node, epoch) = if id % 2 == 0 {
                (&mut a, 1)
            } else {
                (&mut b, 2)
            };
            assert_eq!(
                node.admit(&ctx(epoch, id, 1), &digest),
                Admission::New,
                "id {id}"
            );
            commit(node, &ctx(epoch, id, 1), digest, &[]);
            node.release(id);
        }
    }

    #[test]
    fn retired_ids_inside_the_window_stay_refused() {
        let mut ledger = ledger(1, 4);
        let digest = payload_digest(b"run");
        for id in [3, 1, 2, 5, 4] {
            ledger.release(id);
        }
        for id in 1..=5 {
            assert_eq!(
                ledger.admit(&ctx(1, id, 1), &digest),
                Admission::Retired,
                "id {id}"
            );
        }
        assert_eq!(ledger.admit(&ctx(1, 6, 1), &digest), Admission::New);
    }

    #[test]
    fn an_idle_session_is_released_by_tick_and_activity_keeps_one_alive() {
        let mut ledger = ledger(1, 4);
        let digest = payload_digest(b"idle");
        assert!(ledger.tick(0, 10).is_empty());
        commit(&mut ledger, &ctx(1, 1, 1), digest, &[]);
        commit(&mut ledger, &ctx(1, 2, 1), digest, &[]);

        assert!(ledger.tick(5, 10).is_empty(), "nothing is idle yet");
        ledger.tick(8, 10);
        ledger.touch(1);

        assert_eq!(
            ledger.tick(12, 10),
            vec![2],
            "session 2 has been silent since 0"
        );
        assert_eq!(ledger.session_count(), 1);
        assert_eq!(ledger.admit(&ctx(1, 2, 2), &digest), Admission::Retired);
        assert_eq!(ledger.admit(&ctx(1, 1, 2), &digest), Admission::New);
    }

    #[test]
    fn a_clock_that_goes_back_expires_nothing_early() {
        let mut ledger = ledger(1, 4);
        let digest = payload_digest(b"clock");
        ledger.tick(100, 10);
        commit(&mut ledger, &ctx(1, 1, 1), digest, &[]);
        assert!(ledger.tick(50, 10).is_empty());
        assert!(ledger.tick(105, 10).is_empty());
        assert_eq!(ledger.tick(110, 10), vec![1]);
    }

    #[test]
    fn the_first_tick_starts_the_clock_so_earlier_calls_are_not_expired() {
        let mut ledger = ledger(1, 4);
        let digest = payload_digest(b"boot");
        commit(&mut ledger, &ctx(1, 1, 1), digest, &[]);
        assert!(
            ledger.tick(50_000, 10_000).is_empty(),
            "the clock starts here"
        );
        assert!(ledger.tick(55_000, 10_000).is_empty());
        assert_eq!(ledger.tick(60_000, 10_000), vec![1]);
    }

    #[test]
    fn session_capacity_is_enforced_without_eviction() {
        let mut ledger = ledger(1, 1);
        let digest = payload_digest(b"a");
        commit(&mut ledger, &ctx(1, 1, 1), digest, &[]);
        assert_eq!(ledger.admit(&ctx(1, 2, 1), &digest), Admission::Capacity);
        assert_eq!(ledger.session_count(), 1);
    }

    #[test]
    fn the_reserved_id_is_refused_and_never_recorded() {
        let mut ledger = ledger(1, 4);
        let digest = payload_digest(b"reserved");
        assert_eq!(
            ledger.admit(&ctx(1, RESERVED_SESSION_ID, 1), &digest),
            Admission::Reserved
        );
        assert_eq!(
            ledger.reserve(&ctx(1, RESERVED_SESSION_ID, 1), digest),
            Err(Admission::Reserved)
        );
        ledger.release(RESERVED_SESSION_ID);
        assert_eq!(ledger.session_count(), 0);
        assert_eq!(ledger.session_floor(), 1, "nothing was recorded");
        assert_eq!(ledger.admission_floor(), 0);
    }

    #[test]
    fn the_highest_usable_id_is_served_and_then_the_suggestion_is_reserved() {
        let mut ledger = ledger(1, 4);
        let digest = payload_digest(b"top");
        let top = RESERVED_SESSION_ID - 1;
        commit(&mut ledger, &ctx(1, top, 1), digest, &[5]);
        assert_eq!(
            ledger.session_floor(),
            RESERVED_SESSION_ID,
            "no usable id is above the suggestion, and it does not wrap"
        );
        assert_eq!(ledger.admission_floor(), top + 1 - WINDOW);
        assert_eq!(
            ledger.admit(&ctx(1, top, 1), &digest),
            Admission::Replay(&[5])
        );
        assert_eq!(
            ledger.admit(&ctx(1, RESERVED_SESSION_ID, 1), &digest),
            Admission::Reserved
        );
        let next = payload_digest(b"second");
        assert_eq!(ledger.admit(&ctx(1, top, 2), &next), Admission::New);
        commit(&mut ledger, &ctx(1, top, 2), next, &[9]);
        assert_eq!(
            ledger.admit(&ctx(1, top, 2), &next),
            Admission::Replay(&[9])
        );
        ledger.release(top);
        assert_eq!(ledger.admit(&ctx(1, top, 3), &next), Admission::Retired);
        assert_eq!(
            ledger.admit(&ctx(1, top - 1, 1), &next),
            Admission::New,
            "an unused id inside the top window is still admitted"
        );
        assert_eq!(
            ledger.admit(&ctx(1, 4, 1), &next),
            Admission::BelowWindow {
                floor: top + 1 - WINDOW
            }
        );
    }

    #[test]
    fn zero_and_window_edges_are_exact() {
        let mut ledger = ledger(1, 4);
        let digest = payload_digest(b"edge");
        assert_eq!(
            ledger.admit(&ctx(1, 0, 1), &digest),
            Admission::New,
            "zero is a valid id"
        );
        commit(&mut ledger, &ctx(1, WINDOW - 1, 1), digest, &[]);
        assert_eq!(
            ledger.admission_floor(),
            0,
            "the top of the first window does not move it"
        );
        commit(&mut ledger, &ctx(1, WINDOW, 1), digest, &[]);
        assert_eq!(
            ledger.admission_floor(),
            1,
            "one past the top moves it by one"
        );
        assert_eq!(
            ledger.admit(&ctx(1, 0, 1), &digest),
            Admission::BelowWindow { floor: 1 }
        );
    }

    #[test]
    fn the_last_recorded_call_is_answered_at_the_sequence_boundary() {
        // A session cannot reach u64::MAX by admitted calls in practice, so the boundary is recorded directly.
        let mut ledger = ledger(1, 4);
        let last = payload_digest(b"last");
        ledger.sessions.insert(
            3,
            Session {
                sequence: u64::MAX,
                digest: last,
                answer: vec![1],
                last_seen: 0,
                reserved: None,
            },
        );
        assert_eq!(
            ledger.admit(&ctx(1, 3, u64::MAX), &last),
            Admission::Replay(&[1]),
            "a retransmission is answered without computing a successor"
        );
        assert_eq!(
            ledger.admit(&ctx(1, 3, u64::MAX), &payload_digest(b"other")),
            Admission::Conflict
        );
        assert_eq!(
            ledger.admit(&ctx(1, 3, 1), &last),
            Admission::SequenceExhausted
        );
    }

    #[test]
    fn every_mutation_answer_fits_the_protocol_bound() {
        use crate::codec;
        use crate::wire::v1;
        use micropb::MessageEncode;
        for size in [
            v1::BeginUploadResponse::MAX_SIZE,
            v1::PutChunkResponse::MAX_SIZE,
            v1::StartResponse::MAX_SIZE,
            v1::ResumeResponse::MAX_SIZE,
            v1::CancelResponse::MAX_SIZE,
            v1::ReleaseResponse::MAX_SIZE,
        ] {
            let size = size.expect("a fixed-size answer");
            assert!(size <= MAX_MUTATION_ANSWER_BYTES, "{size} bytes");
        }
        // The commit answer has a bytes field, so it has no static bound. Its largest real form is measured instead.
        let mut commit = v1::CommitArtifactResponse {
            r#digest: vec![0xff; 32],
            ..v1::CommitArtifactResponse::default()
        };
        commit.set_context(Context {
            epoch: u64::MAX,
            session_id: u64::MAX,
            sequence: u64::MAX,
        });
        assert!(codec::encode(&commit).len() <= MAX_MUTATION_ANSWER_BYTES);
    }

    /// A small deterministic model check: random admit, record, abandon, replay, release and expiry operations never
    /// let a forgotten or retired id run again, and a recorded answer is replayed exactly.
    #[test]
    fn random_operation_sequences_never_rerun_a_retired_id() {
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let mut ledger = Ledger::new(1, 6, 128, 32).expect("valid");
        let mut retired: alloc::collections::BTreeSet<u64> = alloc::collections::BTreeSet::new();
        let mut live: BTreeMap<u64, (u64, [u8; 32])> = BTreeMap::new();
        let mut clock = 0u64;
        for _ in 0..20_000 {
            let id = next() % 200;
            let digest = payload_digest(&(next() % 3).to_le_bytes());
            match next() % 6 {
                0 | 1 => {
                    let sequence = live.get(&id).map_or(1, |(seq, _)| seq + 1);
                    let context = ctx(1, id, sequence);
                    if let Ok(reservation) = ledger.reserve(&context, digest) {
                        assert!(!retired.contains(&id), "retired id {id} admitted again");
                        if next() % 4 == 0 {
                            ledger.abandon(reservation);
                        } else {
                            assert!(ledger.record(reservation, &Answer::raw(&id.to_le_bytes())));
                            live.insert(id, (sequence, digest));
                        }
                    }
                }
                2 => {
                    if let Some(&(sequence, recorded)) = live.get(&id) {
                        assert_eq!(
                            ledger.admit(&ctx(1, id, sequence), &recorded),
                            Admission::Replay(&id.to_le_bytes())
                        );
                    }
                }
                3 => {
                    if live.remove(&id).is_some() {
                        ledger.release(id);
                        retired.insert(id);
                    }
                }
                4 => {
                    clock += next() % 20;
                    for expired in ledger.tick(clock, 30) {
                        live.remove(&expired);
                        retired.insert(expired);
                    }
                }
                _ => {
                    if retired.contains(&id) && !live.contains_key(&id) {
                        assert!(
                            matches!(
                                ledger.admit(&ctx(1, id, 1), &digest),
                                Admission::Retired | Admission::BelowWindow { .. }
                            ),
                            "retired id {id} is not refused"
                        );
                    }
                }
            }
            assert!(ledger.session_count() <= 6);
        }
    }
}

#[cfg(test)]
mod finals {
    use super::*;

    const WINDOW: u64 = 256;

    fn ctx(epoch: u64, session: u64, sequence: u64) -> Context {
        Context {
            epoch,
            session_id: session,
            sequence,
        }
    }

    fn ledger() -> Ledger {
        Ledger::new(1, 8, 128, WINDOW).expect("valid bounds")
    }

    /// Runs one final call on `session` as the node does: reserve, then finish with `answer`.
    fn finish(ledger: &mut Ledger, session: u64, digest: [u8; DIGEST_BYTES], answer: &[u8]) {
        let reservation = ledger
            .reserve(&ctx(1, session, 1), digest)
            .expect("admitted");
        assert!(ledger.finish(reservation, &Answer::raw(answer)));
    }

    fn digest_of(session: u64) -> [u8; DIGEST_BYTES] {
        payload_digest(&session.to_le_bytes())
    }

    #[test]
    fn an_identical_retransmission_of_a_final_call_replays_its_answer() {
        let mut ledger = ledger();
        let digest = payload_digest(b"release");
        finish(&mut ledger, 5, digest, b"answer");
        assert!(!ledger.is_live(5), "a final call ends its session");
        assert_eq!(
            ledger.admit(&ctx(1, 5, 1), &digest),
            Admission::Replay(&b"answer"[..])
        );
    }

    #[test]
    fn a_final_call_with_another_payload_conflicts_and_another_sequence_is_retired() {
        let mut ledger = ledger();
        let digest = payload_digest(b"release");
        finish(&mut ledger, 5, digest, b"answer");
        assert_eq!(
            ledger.admit(&ctx(1, 5, 1), &payload_digest(b"other")),
            Admission::Conflict
        );
        assert_eq!(ledger.admit(&ctx(1, 5, 2), &digest), Admission::Retired);
    }

    #[test]
    fn the_newest_final_answers_replay_and_the_oldest_are_evicted_without_a_rerun() {
        let mut ledger = ledger();
        let count = FINAL_REPLAY_SLOTS as u64 + 1;
        for session in 1..=count {
            finish(&mut ledger, session, digest_of(session), &[session as u8]);
        }
        assert_eq!(
            ledger.finals.len(),
            FINAL_REPLAY_SLOTS,
            "the table is bounded"
        );
        assert_eq!(
            ledger.admit(&ctx(1, count, 1), &digest_of(count)),
            Admission::Replay(&[count as u8][..]),
            "the newest final answer still replays"
        );
        assert_eq!(
            ledger.admit(&ctx(1, 1, 1), &digest_of(1)),
            Admission::Retired,
            "an evicted final call is refused, not admitted as new work"
        );
        assert_eq!(
            ledger.admit(&ctx(1, 1, 1), &payload_digest(b"anything")),
            Admission::Retired
        );
    }

    #[test]
    fn a_final_answer_keeps_its_replay_after_the_window_moves_past_its_id() {
        let mut ledger = ledger();
        finish(&mut ledger, 4, digest_of(4), b"done");
        // A session far above the window moves the floor past id 4.
        commit_live(&mut ledger, 10_000);
        assert!(ledger.admission_floor() > 4);
        assert_eq!(
            ledger.admit(&ctx(1, 4, 1), &digest_of(4)),
            Admission::Replay(&b"done"[..]),
            "the table answers before the floor refuses"
        );
    }

    fn commit_live(ledger: &mut Ledger, session: u64) {
        let reservation = ledger
            .reserve(&ctx(1, session, 1), digest_of(session))
            .expect("admitted");
        assert!(ledger.record(reservation, &Answer::raw(b"live")));
    }

    #[test]
    fn a_stale_reservation_cannot_overwrite_a_record_or_recreate_a_released_session() {
        let mut ledger = ledger();
        let digest = digest_of(3);
        let reservation = ledger.reserve(&ctx(1, 3, 1), digest).expect("admitted");
        ledger.release(3);
        assert!(
            !ledger.record(reservation, &Answer::raw(b"late")),
            "the stale record is refused"
        );
        assert!(
            !ledger.is_live(3),
            "the record does not recreate the session"
        );
        assert_eq!(ledger.admit(&ctx(1, 3, 1), &digest), Admission::Retired);
    }

    #[test]
    fn a_stale_finish_cannot_store_a_final_answer_for_a_released_session() {
        let mut ledger = ledger();
        let reservation = ledger
            .reserve(&ctx(1, 3, 1), digest_of(3))
            .expect("admitted");
        ledger.release(3);
        assert!(!ledger.finish(reservation, &Answer::raw(b"late")));
        assert!(
            ledger.finals.is_empty(),
            "nothing is stored for a stale finish"
        );
    }

    #[test]
    fn abandoning_after_a_release_changes_nothing() {
        let mut ledger = ledger();
        let reservation = ledger
            .reserve(&ctx(1, 6, 1), digest_of(6))
            .expect("admitted");
        ledger.release(6);
        ledger.abandon(reservation);
        assert!(!ledger.is_live(6));
        assert_eq!(
            ledger.admit(&ctx(1, 6, 1), &digest_of(6)),
            Admission::Retired
        );
    }

    #[test]
    fn overlapping_reservations_on_one_session_are_refused_until_the_first_completes() {
        let mut ledger = ledger();
        let first = ledger
            .reserve(&ctx(1, 4, 1), digest_of(4))
            .expect("admitted");
        assert_eq!(
            ledger
                .reserve(&ctx(1, 4, 1), payload_digest(b"other"))
                .err(),
            Some(Admission::InFlight),
            "a second reservation on the session is refused"
        );
        assert_eq!(
            ledger.admit(&ctx(1, 4, 2), &digest_of(4)),
            Admission::InFlight
        );
        assert!(ledger.record(first, &Answer::raw(b"ok")));
        assert_eq!(ledger.admit(&ctx(1, 4, 2), &digest_of(4)), Admission::New);
    }

    #[test]
    fn a_final_call_on_another_epoch_is_refused_before_its_answer_is_consulted() {
        let mut ledger = ledger();
        finish(&mut ledger, 2, digest_of(2), b"done");
        assert_eq!(
            ledger.admit(&ctx(2, 2, 1), &digest_of(2)),
            Admission::StaleEpoch { current: 1 }
        );
    }

    #[test]
    fn a_session_released_by_idle_leaves_no_replayable_answer() {
        let mut ledger = ledger();
        commit_live(&mut ledger, 9);
        ledger.release(9);
        assert!(
            ledger.finals.is_empty(),
            "an idle release is not a final call"
        );
        assert_eq!(
            ledger.admit(&ctx(1, 9, 1), &digest_of(9)),
            Admission::Retired
        );
    }

    #[test]
    fn a_replayed_live_call_is_still_the_recorded_answer_after_a_touch() {
        let mut ledger = ledger();
        commit_live(&mut ledger, 12);
        ledger.touch(12);
        assert_eq!(
            ledger.admit(&ctx(1, 12, 1), &digest_of(12)),
            Admission::Replay(&b"live"[..])
        );
    }

    // -- Reservations, idle expiry and answers -------------------------------------------------------------------------------

    #[test]
    fn a_reservation_survives_idle_expiry_and_its_answer_is_recorded() {
        let mut ledger = ledger();
        ledger.tick(0, 10);
        commit_live(&mut ledger, 20);
        ledger.tick(5, 10);
        let digest = payload_digest(b"second");
        let reservation = ledger.reserve(&ctx(1, 20, 2), digest).expect("admitted");

        // The clock passes the idle timeout while the call runs.
        assert!(
            ledger.tick(100, 10).is_empty(),
            "an active reservation is never expired"
        );
        assert_eq!(ledger.reservations(), 1);
        assert!(ledger.is_live(20));

        assert!(ledger.record(reservation, &Answer::raw(b"second answer")));
        assert_eq!(ledger.reservations(), 0);
        assert_eq!(
            ledger.admit(&ctx(1, 20, 2), &digest),
            Admission::Replay(&b"second answer"[..]),
            "the answer of the call that ran is replayed"
        );
        assert!(ledger.tick(100, 10).is_empty(), "completion is activity");
        assert_eq!(
            ledger.tick(110, 10),
            vec![20],
            "the session then expires normally"
        );
    }

    #[test]
    fn a_final_call_reserved_before_expiry_files_its_answer_for_replay() {
        let mut ledger = ledger();
        ledger.tick(0, 10);
        let digest = digest_of(7);
        let reservation = ledger.reserve(&ctx(1, 7, 1), digest).expect("admitted");
        assert!(ledger.tick(1_000, 10).is_empty());
        assert!(ledger.finish(reservation, &Answer::raw(b"released")));
        assert!(!ledger.is_live(7));
        assert_eq!(
            ledger.admit(&ctx(1, 7, 1), &digest),
            Admission::Replay(&b"released"[..]),
            "the final answer survived an expiry that fell inside the call"
        );
    }

    #[test]
    fn reserve_and_record_stamp_activity_and_abandon_does_not_extend_it() {
        let mut ledger = ledger();
        ledger.tick(0, 10);
        commit_live(&mut ledger, 30);
        ledger.tick(8, 10);

        // A refused call at 8 is activity: the session now expires from 8, not from 0.
        let reservation = ledger
            .reserve(&ctx(1, 30, 2), digest_of(30))
            .expect("admitted");
        ledger.abandon(reservation);
        assert_eq!(ledger.reservations(), 0);
        assert!(ledger.tick(12, 10).is_empty(), "idle since 8, not since 0");
        assert!(ledger.tick(17, 10).is_empty());
        assert_eq!(ledger.tick(18, 10), vec![30]);
    }

    #[test]
    fn an_abandoned_first_call_leaves_no_session_to_expire() {
        let mut ledger = ledger();
        ledger.tick(0, 10);
        let reservation = ledger
            .reserve(&ctx(1, 31, 1), digest_of(31))
            .expect("admitted");
        ledger.abandon(reservation);
        assert_eq!(ledger.session_count(), 0);
        assert!(ledger.tick(100, 10).is_empty());
        assert_eq!(
            ledger.admit(&ctx(1, 31, 1), &digest_of(31)),
            Admission::New,
            "the id was never used"
        );
    }

    #[test]
    fn a_dropped_reservation_is_counted_and_released_by_ending_the_session() {
        let mut ledger = ledger();
        ledger.tick(0, 10);
        let dropped = ledger
            .reserve(&ctx(1, 32, 1), digest_of(32))
            .expect("admitted");
        assert!(
            ledger.tick(1_000, 10).is_empty(),
            "the ledger cannot tell a dropped reservation"
        );
        assert_eq!(ledger.reservations(), 1, "but it counts it");
        ledger.release(32);
        assert_eq!(ledger.reservations(), 0);
        assert!(
            !ledger.record(dropped, &Answer::raw(b"late")),
            "the reservation is stale"
        );
        assert_eq!(
            ledger.admit(&ctx(1, 32, 1), &digest_of(32)),
            Admission::Retired
        );
    }

    #[test]
    fn recording_never_grows_the_storage_reserved_for_the_answer() {
        let mut ledger = ledger();
        let reservation = ledger
            .reserve(&ctx(1, 40, 1), digest_of(40))
            .expect("admitted");
        let before = ledger.sessions[&40].answer.capacity();
        assert!(before >= MAX_MUTATION_ANSWER_BYTES);
        let largest = Answer::raw(&[0xab; MAX_MUTATION_ANSWER_BYTES]);
        assert!(ledger.record(reservation, &largest));
        assert_eq!(
            ledger.sessions[&40].answer.capacity(),
            before,
            "the largest possible answer fits the storage that was reserved before the effect"
        );
        assert_eq!(ledger.sessions[&40].answer.len(), MAX_MUTATION_ANSWER_BYTES);
    }

    #[test]
    fn an_evicted_final_answer_is_refused_as_retired_and_then_below_the_floor_and_never_reruns() {
        let mut ledger = ledger();
        for session in 1..=(FINAL_REPLAY_SLOTS as u64 + 1) {
            finish(&mut ledger, session, digest_of(session), &[session as u8]);
        }
        assert_eq!(
            ledger.admit(&ctx(1, 1, 1), &digest_of(1)),
            Admission::Retired,
            "evicted, but still inside the window: its bit says it ended"
        );

        // A live session far above moves the admission floor past id 1 and clears its bit.
        commit_live(&mut ledger, 10_000);
        assert!(ledger.admission_floor() > 1);
        assert_eq!(
            ledger.admit(&ctx(1, 1, 1), &digest_of(1)),
            Admission::BelowWindow {
                floor: ledger.admission_floor()
            },
            "the same call is now refused for a different reason, and still does not run"
        );
        assert!(
            matches!(
                ledger.reserve(&ctx(1, 1, 1), digest_of(1)),
                Err(Admission::BelowWindow { .. })
            ),
            "no reservation, so no rerun"
        );
        assert_eq!(ledger.reservations(), 0);
    }

    #[test]
    fn answers_are_the_wire_encoding_of_the_response_and_fit_the_bound() {
        use crate::codec;
        use crate::wire::v1;
        let context = Context {
            epoch: u64::MAX,
            session_id: u64::MAX - 1,
            sequence: u64::MAX,
        };
        macro_rules! check {
            ($response:expr) => {{
                let mut response = $response;
                response.set_context(context);
                let answer = Answer::encode(&response);
                assert_eq!(answer.as_slice(), codec::encode(&response).as_slice());
                assert!(answer.as_slice().len() <= MAX_MUTATION_ANSWER_BYTES);
            }};
        }
        check!(v1::BeginUploadResponse {
            r#upload_id: u64::MAX,
            ..Default::default()
        });
        check!(v1::PutChunkResponse {
            r#received: u32::MAX,
            ..Default::default()
        });
        check!(v1::StartResponse {
            r#execution_id: u64::MAX,
            ..Default::default()
        });
        check!(v1::ResumeResponse::default());
        check!(v1::CancelResponse::default());
        check!(v1::ReleaseResponse::default());

        let digest = [0xff; DIGEST_BYTES];
        let commit = Answer::commit(context, &digest);
        let mut expected = v1::CommitArtifactResponse {
            r#digest: digest.to_vec(),
            ..Default::default()
        };
        expected.set_context(context);
        assert_eq!(commit.as_slice(), codec::encode(&expected).as_slice());
        assert!(commit.as_slice().len() <= MAX_MUTATION_ANSWER_BYTES);
    }

    /// The hand-written commit encoding matches the generated one at every varint width and at zero, where a proto3 field
    /// is still written because the context is present.
    #[test]
    fn commit_answers_match_the_generated_encoding_at_every_varint_width() {
        use crate::codec;
        use crate::wire::v1;
        let mut values = vec![0u64];
        for bits in 0..64 {
            values.push(1u64 << bits);
            values.push((1u64 << bits) - 1);
        }
        values.push(u64::MAX);
        for &epoch in &values {
            for &session_id in &[0, 1, 127, 128, u64::MAX - 1] {
                let context = Context {
                    epoch,
                    session_id,
                    sequence: epoch ^ session_id,
                };
                for digest in [[0u8; DIGEST_BYTES], [0xa5; DIGEST_BYTES]] {
                    let mut expected = v1::CommitArtifactResponse {
                        r#digest: digest.to_vec(),
                        ..Default::default()
                    };
                    expected.set_context(context);
                    assert_eq!(
                        Answer::commit(context, &digest).as_slice(),
                        codec::encode(&expected).as_slice(),
                        "{context:?}"
                    );
                }
            }
        }
    }
}

/// Reservations are bound to the ledger that issued them. Two ledgers hand out the same session and ticket by
/// construction, so a reservation from one must not act on the other.
#[cfg(test)]
mod ownership {
    use super::*;

    const DIGEST: [u8; DIGEST_BYTES] = [7; DIGEST_BYTES];

    fn ctx(session: u64, sequence: u64) -> Context {
        Context {
            epoch: 1,
            session_id: session,
            sequence,
        }
    }

    fn ledger() -> Ledger {
        Ledger::new(1, 8, MAX_MUTATION_ANSWER_BYTES, 64).expect("bounds are valid")
    }

    #[test]
    fn a_foreign_record_or_finish_changes_nothing_and_the_genuine_reservation_still_records() {
        let mut here = ledger();
        let mut other = ledger();
        // Session 1 and ticket 1 in both ledgers.
        let mine = here.reserve(&ctx(1, 1), DIGEST).expect("admitted");
        let foreign = other.reserve(&ctx(1, 1), DIGEST).expect("admitted");
        assert!(!here.record(foreign, &Answer::raw(b"foreign")));
        assert_eq!(
            here.reservations(),
            1,
            "the genuine reservation is still held"
        );
        assert!(here.record(mine, &Answer::raw(b"mine")));
        assert_eq!(
            here.admit(&ctx(1, 1), &DIGEST),
            Admission::Replay(b"mine"),
            "the recorded answer is the genuine one"
        );

        // A foreign finish on a live session of this ledger must not end it.
        let mine = here.reserve(&ctx(2, 1), DIGEST).expect("admitted");
        let foreign = other.reserve(&ctx(2, 1), DIGEST).expect("admitted");
        assert!(!here.finish(foreign, &Answer::raw(b"foreign")));
        assert!(here.is_live(2), "a foreign finish does not end the session");
        assert!(here.finish(mine, &Answer::raw(b"mine")));
        assert!(!here.is_live(2));
    }

    #[test]
    fn a_foreign_abandon_does_not_open_or_remove_a_session_here() {
        let mut here = ledger();
        let mut other = ledger();
        let mine = here.reserve(&ctx(3, 1), DIGEST).expect("admitted");
        let foreign = other.reserve(&ctx(3, 1), DIGEST).expect("admitted");
        here.abandon(foreign);
        assert!(
            here.is_live(3),
            "a foreign abandon does not remove the reserved session"
        );
        assert_eq!(here.reservations(), 1);
        here.abandon(mine);
        assert!(
            !here.is_live(3),
            "the genuine abandon removes the session it opened"
        );
    }

    #[test]
    fn a_consumed_foreign_reservation_leaves_its_own_session_reserved_until_released() {
        // A reservation dropped without recording is the existing state of a dropped reservation: it is counted and
        // released only by ending its session. A refused foreign record consumes the value, so the owner must release.
        let mut here = ledger();
        let mut other = ledger();
        let foreign = other.reserve(&ctx(4, 1), DIGEST).expect("admitted");
        assert!(!here.record(foreign, &Answer::raw(b"x")));
        assert_eq!(
            other.reservations(),
            1,
            "the owner still counts the reservation"
        );
        other.release(4);
        assert_eq!(other.reservations(), 0);
        assert!(!other.is_live(4));
    }

    #[test]
    fn each_ledger_has_its_own_identity_and_reservations_name_their_issuer() {
        let mut a = ledger();
        let mut b = ledger();
        let from_a = a.reserve(&ctx(1, 1), DIGEST).expect("admitted");
        let from_b = b.reserve(&ctx(1, 1), DIGEST).expect("admitted");
        assert_ne!(from_a.ledger, from_b.ledger);
        assert_eq!(from_a.ledger, a.id);
        assert_eq!(from_b.ledger, b.id);
        // Both ledgers issue ticket 1 for their first reservation, which is why the identity is needed.
        assert_eq!(from_a.ticket, from_b.ticket);
        a.abandon(from_a);
        b.abandon(from_b);
        assert_eq!(a.reservations(), 0);
        assert_eq!(b.reservations(), 0);
    }
}
