//! Embedded-style device service with telemetry and control commands.

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
    METHOD_COMMAND, METHOD_EVENT_SUBSCRIBE, Service, ServiceBlockingClient, ServiceClient,
    ServiceServer,
};

use device::{Command, Command_, DeviceStatus, OutputState, Reply, Reply_, RestartAck};
use protolink::Status;

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
}

impl Service for Device {
    fn command(&mut self, request: Command) -> Result<Reply, Status> {
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
                *state = output.enabled;
                Reply_::Reply::OutputState(OutputState {
                    channel: output.channel,
                    enabled: output.enabled,
                })
            }
            Some(Command_::Command::Restart(_)) => {
                self.restarts += 1;
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
