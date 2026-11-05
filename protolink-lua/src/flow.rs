//! Sessions of the execution protocol.
//!
//! A session is one caller's stream of mutating calls. The node admits the calls of a session in sequence order, starting
//! at one, and records each answer for replay. A session takes one of the node's `max_sessions` for its epoch until its
//! final call releases it: the commit of an upload, or the release of a finished execution. A session that goes idle
//! ends too, so a caller that keeps polling must keep calling. A caller opens one session per upload and one per
//! execution, not one per request, and it ends each one with its final call.

/// The identity a call carries, as the protocol's `Context`. Each transport converts it to its own message type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stamp {
    /// Incarnation of the node the call is for.
    pub epoch: u64,
    /// Session of the call.
    pub session_id: u64,
    /// Position of the call in its session.
    pub sequence: u64,
}

/// One session on a node that reported `epoch`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Flow {
    epoch: u64,
    session: u64,
    admitted: u64,
}

impl Flow {
    /// Starts session `session` on the node that reported `epoch`.
    pub const fn new(epoch: u64, session: u64) -> Self {
        Self {
            epoch,
            session,
            admitted: 0,
        }
    }

    /// The session identifier.
    pub const fn session(&self) -> u64 {
        self.session
    }

    /// The context of the next mutating call. A call the node refused leaves the sequence unchanged, so the next attempt
    /// reuses it and the session has no gap.
    ///
    /// The counter saturates at `u64::MAX` rather than wrapping. A session that reaches it has no successor, so its
    /// next call repeats the last sequence, and the node answers that by replay or conflict.
    pub fn pending(&self) -> Stamp {
        Stamp {
            epoch: self.epoch,
            session_id: self.session,
            sequence: self.admitted.saturating_add(1),
        }
    }

    /// Records that the node admitted the call whose context `pending` returned.
    pub fn advance(&mut self) {
        self.admitted = self.admitted.saturating_add(1);
    }

    /// Reconciles one pending mutation only when both the response identity and committed receipt match.
    /// Returns false after sequence exhaustion or if any evidence is missing or mismatched.
    pub fn reconcile(
        &mut self,
        response: Stamp,
        pending: Stamp,
        fingerprint: &[u8; crate::ledger::DIGEST_BYTES],
        receipt: Option<&crate::ledger::Receipt>,
    ) -> bool {
        if self.admitted == u64::MAX
            || pending != self.pending()
            || response.epoch != pending.epoch
            || response.session_id != pending.session_id
            || !receipt
                .is_some_and(|r| r.sequence == pending.sequence && &r.fingerprint == fingerprint)
        {
            return false;
        }
        self.advance();
        true
    }

    /// The context of a call that does not change state. The node checks ownership and epoch.
    pub fn reading(&self) -> Stamp {
        Stamp {
            epoch: self.epoch,
            session_id: self.session,
            sequence: self.admitted,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Flow;

    #[test]
    fn the_first_mutating_call_is_sequence_one_and_each_admitted_call_advances() {
        let mut flow = Flow::new(7, 3);
        assert_eq!(
            (
                flow.pending().sequence,
                flow.pending().session_id,
                flow.pending().epoch
            ),
            (1, 3, 7)
        );
        flow.advance();
        flow.advance();
        assert_eq!(flow.pending().sequence, 3);
    }

    #[test]
    fn a_refused_call_leaves_the_sequence_for_the_retry() {
        let flow = Flow::new(7, 3);
        assert_eq!(flow.pending(), flow.pending());
        assert_eq!(flow.pending().sequence, 1);
    }

    #[test]
    fn reads_carry_the_session_without_advancing_it() {
        let mut flow = Flow::new(7, 3);
        flow.advance();
        assert_eq!(flow.reading().sequence, 1);
        assert_eq!(flow.pending().sequence, 2);
        assert_eq!(flow.session(), 3);
    }

    #[test]
    fn receipt_reconciles_only_the_exact_pending_identity_once() {
        use crate::ledger::{Receipt, mutation_fingerprint};
        let mut flow = Flow::new(7, 3);
        flow.advance();
        let pending = flow.pending();
        let fingerprint = mutation_fingerprint("Resume", b"request");
        let receipt = Receipt {
            sequence: pending.sequence,
            fingerprint,
        };
        let read = flow.reading();
        assert!(!flow.reconcile(read, pending, &fingerprint, None));
        assert!(!flow.reconcile(read, pending, &[0; 32], Some(&receipt)));
        let mut wrong = read;
        wrong.epoch += 1;
        assert!(!flow.reconcile(wrong, pending, &fingerprint, Some(&receipt)));
        wrong = read;
        wrong.session_id += 1;
        assert!(!flow.reconcile(wrong, pending, &fingerprint, Some(&receipt)));
        let mut wrong_receipt = receipt;
        wrong_receipt.sequence += 1;
        assert!(!flow.reconcile(read, pending, &fingerprint, Some(&wrong_receipt)));
        assert!(flow.reconcile(read, pending, &fingerprint, Some(&receipt)));
        assert!(!flow.reconcile(read, pending, &fingerprint, Some(&receipt)));
        flow.admitted = u64::MAX;
        assert!(!flow.reconcile(read, flow.pending(), &fingerprint, Some(&receipt)));
    }

    #[test]
    fn the_sequence_saturates_at_the_top_instead_of_wrapping() {
        // Reaching the top takes 2^64 admitted calls, so the state is set directly.
        let mut flow = Flow::new(7, 3);
        flow.admitted = u64::MAX - 1;
        assert_eq!(flow.pending().sequence, u64::MAX);
        flow.advance();
        assert_eq!(
            flow.pending().sequence,
            u64::MAX,
            "the last sequence is repeated"
        );
        flow.advance();
        assert_eq!(flow.pending().sequence, u64::MAX);
        assert_eq!(flow.reading().sequence, u64::MAX);
    }
}
