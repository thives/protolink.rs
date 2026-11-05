//! Scripted child client for tests of consumers of this crate.
//!
//! Each `poll` consumes one scripted reply, in order. Submitted operations are recorded so tests can assert that
//! nothing was sent when a check failed. A capacity bound makes `submit` refuse with `Busy`, as a bounded client must.
//!
//! Quarantine follows the contract: a lost child is kept, operations on it are refused with `Quarantined`, and only
//! `recover_lost` or `recover_slot` free it. The client has one route, so every child is on slot 0.

use alloc::collections::VecDeque;
use alloc::vec::Vec;
use core::task::Poll;

use crate::child::{ChildClient, ChildError, ChildId, ChildOp, ChildReply, Ticket};

/// A child client that answers from a script.
#[derive(Debug, Default)]
pub struct ScriptedChild {
    replies: VecDeque<Poll<Result<ChildReply, ChildError>>>,
    submitted: Vec<ChildOp>,
    forgotten: Vec<ChildId>,
    quarantined: Vec<ChildId>,
    last_ticket: u64,
    capacity: Option<usize>,
    open: usize,
}

impl ScriptedChild {
    /// Creates a client that answers with `replies` in order.
    pub fn new(replies: impl IntoIterator<Item = Result<ChildReply, ChildError>>) -> Self {
        Self {
            replies: replies.into_iter().map(Poll::Ready).collect(),
            ..Self::default()
        }
    }

    /// Creates a client whose script may also say that an operation is still pending.
    pub fn with_polls(
        replies: impl IntoIterator<Item = Poll<Result<ChildReply, ChildError>>>,
    ) -> Self {
        Self {
            replies: replies.into_iter().collect(),
            ..Self::default()
        }
    }

    /// Refuses submits with `Busy` once `capacity` operations are open.
    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = Some(capacity);
        self
    }

    /// Operations submitted so far, in order.
    pub fn submitted(&self) -> &[ChildOp] {
        &self.submitted
    }

    /// Children forgotten so far, in order.
    pub fn forgotten(&self) -> &[ChildId] {
        &self.forgotten
    }

    /// Scripted replies not yet consumed.
    pub fn remaining(&self) -> usize {
        self.replies.len()
    }
}

impl ChildClient for ScriptedChild {
    fn submit(&mut self, op: ChildOp, _deadline_ms: u64) -> Result<Ticket, ChildError> {
        if self.quarantined.contains(&op.child()) {
            return Err(ChildError::Quarantined);
        }
        if self.capacity.is_some_and(|capacity| self.open >= capacity) {
            return Err(ChildError::Busy);
        }
        self.open += 1;
        self.submitted.push(op);
        self.last_ticket += 1;
        Ok(Ticket(self.last_ticket))
    }

    fn poll(&mut self, _ticket: Ticket, _now_ms: u64) -> Poll<Result<ChildReply, ChildError>> {
        let reply = self
            .replies
            .pop_front()
            .unwrap_or(Poll::Ready(Err(ChildError::Unavailable)));
        if reply.is_ready() {
            self.open = self.open.saturating_sub(1);
        }
        reply
    }

    fn abandon(&mut self, _ticket: Ticket) {
        self.open = self.open.saturating_sub(1);
    }

    fn liveness(&self, _child: ChildId) -> Option<(u32, u64)> {
        None
    }

    fn forget(&mut self, child: ChildId) {
        if !self.quarantined.contains(&child) {
            self.forgotten.push(child);
        }
    }

    fn lost(&mut self, child: ChildId, _now_ms: u64) {
        if !self.quarantined.contains(&child) {
            self.quarantined.push(child);
        }
    }

    fn lost_children(&self) -> usize {
        self.quarantined.len()
    }

    fn quarantined(&self) -> Vec<(ChildId, u32)> {
        self.quarantined.iter().map(|&child| (child, 0)).collect()
    }

    fn recover_lost(&mut self, child: ChildId) -> bool {
        match self.quarantined.iter().position(|&lost| lost == child) {
            Some(index) => {
                self.quarantined.remove(index);
                true
            }
            None => false,
        }
    }

    fn recover_slot(&mut self, slot: u32) -> usize {
        if slot != 0 {
            return 0;
        }
        let released = self.quarantined.len();
        self.quarantined.clear();
        released
    }

    fn operations(&self) -> usize {
        self.open
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_full_client_refuses_without_recording_the_operation() {
        let mut client = ScriptedChild::new([Ok(ChildReply::Released)]).with_capacity(1);
        let first = client
            .submit(ChildOp::Release { child: ChildId(1) }, 10)
            .expect("room");
        assert_eq!(
            client.submit(ChildOp::Release { child: ChildId(2) }, 10),
            Err(ChildError::Busy)
        );
        assert_eq!(
            client.submitted().len(),
            1,
            "the refused operation was not sent"
        );
        assert_eq!(client.poll(first, 0), Poll::Ready(Ok(ChildReply::Released)));
        assert!(
            client
                .submit(ChildOp::Release { child: ChildId(2) }, 10)
                .is_ok()
        );
    }
}
