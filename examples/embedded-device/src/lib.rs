//! Embedded-style device service with telemetry and control commands.
//!
//! Demonstrates every RPC shape:
//!
//! - `Command` (unary) executes one command,
//! - `EventSubscribe` (server streaming) replays the event log,
//! - `CommandBatch` (client streaming) executes a batch of commands,
//! - `CommandStream` (bidirectional) executes commands as they arrive.

pub mod proto {
    #![allow(
        clippy::all,
        missing_docs,
        non_snake_case,
        non_camel_case_types,
        unused,
        unused_parens
    )]
    include!(concat!(env!("OUT_DIR"), "/device.rs"));
    include!(concat!(env!("OUT_DIR"), "/device_grpc.rs"));
}

pub use proto::protolink_::examples_::embedded_::device_ as device;
pub use proto::service::{
    METHOD_COMMAND, METHOD_COMMAND_BATCH, METHOD_COMMAND_STREAM, METHOD_EVENT_SUBSCRIBE, Service,
    ServiceBlockingClient, ServiceClient, ServiceServer,
};

use std::collections::{BTreeMap, VecDeque};
use std::task::{Context, Poll};

use device::{
    BatchSummary, Command, Command_, DeviceStatus, Event, EventSubscribe, OutputState, Reply,
    Reply_, RestartAck,
};
use protolink::{CallId, Next, Status};

/// Event code recorded when an output changes state.
pub const EVENT_OUTPUT_CHANGED: u32 = 1;
/// Event code recorded when a restart is accepted.
pub const EVENT_RESTART: u32 = 2;

/// Example application implementing the generated `Service` trait.
#[derive(Debug, Default)]
pub struct Device {
    /// Device uptime in milliseconds.
    pub uptime_ms: u64,
    /// Temperature in thousandths of a degree Celsius.
    pub temperature_milli_c: i32,
    /// Supply voltage in millivolts.
    pub supply_millivolts: u32,
    /// Current state of four controllable digital outputs.
    pub output_states: [bool; 4],
    /// Number of accepted restart commands.
    pub restarts: u32,
    /// Event log (`EVENT_*` codes), replayed by `EventSubscribe`.
    pub events: Vec<u32>,
    /// State of in-flight streaming calls.
    pub calls: CallState,
}

/// Per-call state of the streaming RPCs, keyed by [`CallId`].
#[derive(Debug, Default)]
pub struct CallState {
    subscriptions: BTreeMap<CallId, VecDeque<u32>>,
    batches: BTreeMap<CallId, BatchSummary>,
    streams: BTreeMap<CallId, CommandStream>,
}

impl CallState {
    /// Number of streaming calls the device holds state for.
    pub fn active(&self) -> usize {
        self.subscriptions.len() + self.batches.len() + self.streams.len()
    }
}

#[derive(Debug, Default)]
struct CommandStream {
    replies: VecDeque<Reply>,
    /// Set once the client half-closed or a command was rejected.
    end: Option<Result<(), Status>>,
}

impl Device {
    /// Execute one command.
    pub fn execute(&mut self, request: Command) -> Result<Reply, Status> {
        let reply = match request.command {
            Some(Command_::Command::GetStatus(_)) => Reply_::Reply::Status(DeviceStatus {
                uptime_ms: self.uptime_ms,
                temperature_milli_c: self.temperature_milli_c,
                supply_millivolts: self.supply_millivolts,
            }),
            Some(Command_::Command::SetOutput(output)) => {
                let Some(state) = self.output_states.get_mut(output.channel as usize) else {
                    return Err(Status::invalid_argument("output channel out of range"));
                };
                if *state != output.enabled {
                    *state = output.enabled;
                    self.events.push(EVENT_OUTPUT_CHANGED);
                }
                Reply_::Reply::OutputState(OutputState {
                    channel: output.channel,
                    enabled: output.enabled,
                })
            }
            Some(Command_::Command::Restart(_)) => {
                self.restarts += 1;
                self.events.push(EVENT_RESTART);
                Reply_::Reply::RestartAck(RestartAck {})
            }
            None => return Err(Status::invalid_argument("missing command")),
        };
        let mut out = Reply {
            reply: Some(reply),
            ..Reply::default()
        };
        if let Some(id) = request.correlation_id() {
            out.set_correlation_id(*id);
        }
        Ok(out)
    }
}

impl Service for Device {
    fn command(&mut self, request: Command) -> Result<Reply, Status> {
        self.execute(request)
    }

    fn event_subscribe(&mut self, call: CallId, _request: EventSubscribe) -> Result<(), Status> {
        let snapshot = self.events.iter().copied().collect();
        self.calls.subscriptions.insert(call, snapshot);
        Ok(())
    }

    fn poll_event_subscribe(&mut self, call: CallId, _cx: &mut Context<'_>) -> Poll<Next<Event>> {
        let next = self
            .calls
            .subscriptions
            .get_mut(&call)
            .and_then(VecDeque::pop_front);
        Poll::Ready(match next {
            Some(code) => Next::Message(Event { code }),
            None => {
                self.calls.subscriptions.remove(&call);
                Next::Done(Ok(()))
            }
        })
    }

    fn cancel_event_subscribe(&mut self, call: CallId) {
        self.calls.subscriptions.remove(&call);
    }

    fn command_batch(&mut self, call: CallId, request: Command) -> Result<(), Status> {
        let accepted = self.execute(request).is_ok();
        let summary = self.calls.batches.entry(call).or_default();
        if accepted {
            summary.accepted += 1;
        } else {
            summary.rejected += 1;
        }
        Ok(())
    }

    fn poll_command_batch(
        &mut self,
        call: CallId,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<BatchSummary, Status>> {
        // An empty batch has no state yet.
        Poll::Ready(Ok(self.calls.batches.remove(&call).unwrap_or_default()))
    }

    fn cancel_command_batch(&mut self, call: CallId) {
        self.calls.batches.remove(&call);
    }

    fn command_stream(&mut self, call: CallId, request: Command) -> Result<(), Status> {
        if self
            .calls
            .streams
            .get(&call)
            .is_some_and(|s| s.end.is_some())
        {
            // Rejected earlier: the stream is ending, ignore the rest.
            return Ok(());
        }
        let result = self.execute(request);
        let stream = self.calls.streams.entry(call).or_default();
        match result {
            Ok(reply) => stream.replies.push_back(reply),
            // Replies queued before the failure are still delivered.
            Err(status) => stream.end = Some(Err(status)),
        }
        Ok(())
    }

    fn end_command_stream(&mut self, call: CallId) -> Result<(), Status> {
        let stream = self.calls.streams.entry(call).or_default();
        stream.end.get_or_insert(Ok(()));
        Ok(())
    }

    fn poll_command_stream(&mut self, call: CallId, _cx: &mut Context<'_>) -> Poll<Next<Reply>> {
        // Readiness only changes when a command or the half-close arrives,
        // after which the server polls again, so no waker is needed.
        let Some(stream) = self.calls.streams.get_mut(&call) else {
            return Poll::Pending;
        };
        if let Some(reply) = stream.replies.pop_front() {
            return Poll::Ready(Next::Message(reply));
        }
        match stream.end.take() {
            Some(end) => {
                self.calls.streams.remove(&call);
                Poll::Ready(Next::Done(end))
            }
            None => Poll::Pending,
        }
    }

    fn cancel_command_stream(&mut self, call: CallId) {
        self.calls.streams.remove(&call);
    }
}
