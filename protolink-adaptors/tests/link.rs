//! Controller and target connected over a simulated bus: raw traffic, the
//! reliable stack and RPC.

mod common;

use std::collections::VecDeque;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll};
use std::time::Duration;

use common::*;
use embedded_io_async::{Read, Write};
use protolink::grpc::{CallContext, Handler, MethodKind, Route, Status};
use protolink::link::StdTimer;
use protolink_adaptors::{
    Binding, ControllerState, PortReader, PortWriter, TargetState, i2c_controller, i2c_target,
};

async fn with_timeout<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(30), f)
        .await
        .expect("test timed out")
}

/// Runs `test` on a thread with a large stack: in debug builds the reliable
/// link, the RPC client and server and both runners are big futures that
/// would overflow the default 2 MiB test-thread stack.
fn on_big_stack<F, Fut>(test: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()>,
{
    std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap()
                .block_on(test());
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn read_exactly<R: Read>(reader: &mut R, len: usize) -> Vec<u8> {
    let mut out = vec![0u8; len];
    let mut got = 0;
    while got < len {
        got += reader.read(&mut out[got..]).await.unwrap();
    }
    out
}

#[test]
fn sustained_bidirectional_traffic_arrives_complete_and_in_order() {
    on_big_stack(|| async {
        let bus = SharedBus::default();
        let mut target_state = TargetState::new();
        let mut controller_state = ControllerState::<1>::new();
        let Binding {
            port: target_port,
            runner: target_runner,
        } = i2c_target(SimTarget(bus.clone()), BOOT)
            .with_storage(&mut target_state)
            .build();
        let Binding {
            port: controller_port,
            runner: controller_runner,
        } = i2c_controller(SimController(bus.clone()), VirtualTimer::default())
            .with_address(ADDRESS)
            .with_storage(&mut controller_state)
            .build()
            .unwrap();

        let to_target = pattern(5000, 3);
        let to_controller = pattern(4999, 77);

        // Reads and writes of one port are used by different futures, so split
        // the ports.
        let (mut cr, mut cw) = controller_port.into_split();
        let (mut tr, mut tw) = target_port.into_split();
        let app = async {
            let send_to_target = async {
                for chunk in to_target.chunks(37) {
                    cw.write_all(chunk).await.unwrap();
                }
                cw.flush().await.unwrap();
            };
            let send_to_controller = async {
                for chunk in to_controller.chunks(53) {
                    tw.write_all(chunk).await.unwrap();
                }
                tw.flush().await.unwrap();
            };
            tokio::join!(
                send_to_target,
                send_to_controller,
                read_exactly(&mut tr, to_target.len()),
                read_exactly(&mut cr, to_controller.len()),
            )
        };
        let (_, _, at_target, at_controller) = tokio::select! {
            r = controller_runner.run() => panic!("controller runner stopped: {r:?}"),
            r = target_runner.run() => panic!("target runner stopped: {r:?}"),
            out = with_timeout(app) => out,
        };
        assert_eq!(at_target, to_target);
        assert_eq!(at_controller, to_controller);
    });
}

#[test]
fn reliable_stack_carries_data_in_both_directions() {
    on_big_stack(|| async {
        let bus = SharedBus::default();
        let mut target_state = TargetState::new();
        let mut controller_state = ControllerState::<1>::new();
        let Binding {
            port: target_port,
            runner: target_runner,
        } = i2c_target(SimTarget(bus.clone()), BOOT)
            .with_storage(&mut target_state)
            .build();
        let Binding {
            port: controller_port,
            runner: controller_runner,
        } = i2c_controller(SimController(bus.clone()), TokioTimer::default())
            .with_address(ADDRESS)
            .with_storage(&mut controller_state)
            .build()
            .unwrap();
        let mut controller = protolink::link::reliable(controller_port)
            .with_retransmission_timer(StdTimer::new())
            .build();
        let mut target = protolink::link::reliable(target_port)
            .with_retransmission_timer(StdTimer::new())
            .build();

        let request = pattern(700, 5);
        let response = pattern(300, 6);
        let app = async {
            let controller_side = async {
                controller.write_all(&request).await.unwrap();
                controller.flush().await.unwrap();
                read_exactly(&mut controller, response.len()).await
            };
            let target_side = async {
                let got = read_exactly(&mut target, request.len()).await;
                target.write_all(&response).await.unwrap();
                target.flush().await.unwrap();
                got
            };
            tokio::join!(controller_side, target_side)
        };
        let (at_controller, at_target) = tokio::select! {
            r = controller_runner.run() => panic!("controller runner stopped: {r:?}"),
            r = target_runner.run() => panic!("target runner stopped: {r:?}"),
            out = with_timeout(app) => out,
        };
        assert_eq!(at_controller, response);
        assert_eq!(at_target, request);
    });
}

/// Echoes unary requests.
struct Echo;

impl Handler for Echo {
    fn route(&self, path: &str) -> Option<Route> {
        (path == "/t.T/Echo").then_some(Route::new(MethodKind::Unary, 0))
    }

    fn call(&mut self, _: &mut CallContext<'_>, request: &[u8]) -> Result<Vec<u8>, Status> {
        Ok(request.to_vec())
    }

    fn poll_response(
        &mut self,
        _: &mut CallContext<'_>,
        _: &mut Context<'_>,
    ) -> Poll<protolink::Next<Vec<u8>>> {
        Poll::Pending
    }
}

/// An I2C controller hosting an RPC server, and the same with the roles
/// swapped: I2C roles and RPC roles are independent.
#[test]
fn rpc_runs_over_i2c_in_both_role_assignments() {
    on_big_stack(|| async {
        for controller_is_client in [true, false] {
            let bus = SharedBus::default();
            let mut target_state = TargetState::new();
            let mut controller_state = ControllerState::<1>::new();
            let Binding {
                port: target_port,
                runner: target_runner,
            } = i2c_target(SimTarget(bus.clone()), BOOT)
                .with_storage(&mut target_state)
                .build();
            let Binding {
                port: controller_port,
                runner: controller_runner,
            } = i2c_controller(SimController(bus.clone()), TokioTimer::default())
                .with_address(ADDRESS)
                .with_storage(&mut controller_state)
                .build()
                .unwrap();
            let controller_io = protolink::link::reliable(controller_port)
                .with_retransmission_timer(StdTimer::new())
                .build();
            let target_io = protolink::link::reliable(target_port)
                .with_retransmission_timer(StdTimer::new())
                .build();

            let mut handler = Echo;
            let rpc = async {
                let payloads: Vec<Vec<u8>> = vec![b"hello".to_vec(), pattern(200, 1), vec![]];
                let mut results = VecDeque::new();
                if controller_is_client {
                    let mut client = protolink::client(controller_io).without_deadlines().build();
                    let server = protolink::server(target_io)
                        .without_deadlines()
                        .build()
                        .serve(&mut handler);
                    let calls = async {
                        for p in &payloads {
                            results.push_back(client.unary("/t.T/Echo", p).await.unwrap());
                        }
                    };
                    tokio::select! { r = server => panic!("server stopped: {r:?}"), () = calls => {} }
                } else {
                    let mut client = protolink::client(target_io).without_deadlines().build();
                    let server = protolink::server(controller_io)
                        .without_deadlines()
                        .build()
                        .serve(&mut handler);
                    let calls = async {
                        for p in &payloads {
                            results.push_back(client.unary("/t.T/Echo", p).await.unwrap());
                        }
                    };
                    tokio::select! { r = server => panic!("server stopped: {r:?}"), () = calls => {} }
                }
                assert_eq!(results, payloads);
            };
            tokio::select! {
                r = controller_runner.run() => panic!("controller runner stopped: {r:?}"),
                r = target_runner.run() => panic!("target runner stopped: {r:?}"),
                () = with_timeout(rpc) => {}
            }
        }
    });
}

/// Unary calls from a client on one pair of port halves to a server on the
/// other. The split factories take the halves as they come from `into_split`.
async fn unary_calls<'a>(
    client_io: (PortReader<'a>, PortWriter<'a>),
    server_io: (PortReader<'a>, PortWriter<'a>),
    payloads: &[Vec<u8>],
) -> Vec<Vec<u8>> {
    let mut client = protolink::client_split(client_io.0, client_io.1)
        .without_deadlines()
        .build();
    let mut handler = Echo;
    let server = protolink::server_split(server_io.0, server_io.1)
        .without_deadlines()
        .build()
        .serve(&mut handler);
    let calls = async {
        let mut results = Vec::new();
        for p in payloads {
            results.push(client.unary("/t.T/Echo", p).await.unwrap());
        }
        results
    };
    tokio::select! {
        r = server => panic!("server stopped: {r:?}"),
        results = calls => results,
    }
}

/// The application's halves of a split I2C port go to `client_split` and
/// `server_split`, and one runner per bus keeps the traffic moving.
#[test]
fn split_port_halves_carry_rpc_in_both_role_assignments() {
    on_big_stack(|| async {
        for controller_is_client in [true, false] {
            let bus = SharedBus::default();
            let mut target_state = TargetState::new();
            let mut controller_state = ControllerState::<1>::new();
            let Binding {
                port: target_port,
                runner: target_runner,
            } = i2c_target(SimTarget(bus.clone()), BOOT)
                .with_storage(&mut target_state)
                .build();
            let Binding {
                port: controller_port,
                runner: controller_runner,
            } = i2c_controller(SimController(bus.clone()), TokioTimer::default())
                .with_address(ADDRESS)
                .with_storage(&mut controller_state)
                .build()
                .unwrap();
            let controller_halves = controller_port.into_split();
            let target_halves = target_port.into_split();
            let payloads: Vec<Vec<u8>> = vec![b"hello".to_vec(), pattern(200, 1), vec![]];

            let (client_io, server_io) = if controller_is_client {
                (controller_halves, target_halves)
            } else {
                (target_halves, controller_halves)
            };
            let results = tokio::select! {
                r = controller_runner.run() => panic!("controller runner stopped: {r:?}"),
                r = target_runner.run() => panic!("target runner stopped: {r:?}"),
                out = with_timeout(unary_calls(client_io, server_io, &payloads)) => out,
            };
            assert_eq!(results, payloads);
        }
    });
}

/// The target does not read, so its receive credit reaches zero and the
/// controller stops writing. Its own output still reaches the controller, and
/// the withheld bytes move once the target reads.
#[test]
fn withheld_credit_holds_writes_while_inbound_traffic_flows() {
    on_big_stack(|| async {
        let bus = SharedBus::default();
        let mut target_state = TargetState::new();
        let mut controller_state = ControllerState::<1>::new();
        let Binding {
            port: target_port,
            runner: target_runner,
        } = i2c_target(SimTarget(bus.clone()), BOOT)
            .with_storage(&mut target_state)
            .build();
        let Binding {
            port: controller_port,
            runner: controller_runner,
        } = i2c_controller(SimController(bus.clone()), TokioTimer::default())
            .with_address(ADDRESS)
            .with_storage(&mut controller_state)
            .build()
            .unwrap();
        let (mut cr, mut cw) = controller_port.into_split();
        let (mut tr, mut tw) = target_port.into_split();

        // More than the target's receive buffer, and more than the controller's
        // send buffer, so the controller's write cannot finish until the target
        // reads.
        let to_target = pattern(200, 3);
        let to_controller = pattern(150, 4);
        let mut outbound = pin!(async {
            cw.write_all(&to_target).await.unwrap();
            cw.flush().await.unwrap();
        });
        // The write can only finish as the controller reads, so the two run
        // together.
        let mut inbound = pin!(async {
            let send = async {
                tw.write_all(&to_controller).await.unwrap();
                tw.flush().await.unwrap();
            };
            let ((), got) = tokio::join!(send, read_exactly(&mut cr, to_controller.len()));
            got
        });

        let (at_controller, at_target) = tokio::select! {
            r = controller_runner.run() => panic!("controller runner stopped: {r:?}"),
            r = target_runner.run() => panic!("target runner stopped: {r:?}"),
            out = with_timeout(async {
                let at_controller = tokio::select! {
                    biased;
                    got = &mut inbound => got,
                    () = &mut outbound => panic!("a write finished while the target withheld credit"),
                };
                // Reading restores the credit, so the withheld bytes now arrive.
                let ((), at_target) = tokio::join!(
                    &mut outbound,
                    read_exactly(&mut tr, to_target.len()),
                );
                (at_controller, at_target)
            }) => out,
        };
        assert_eq!(at_controller, to_controller);
        assert_eq!(at_target, to_target);
    });
}

/// The controller's runner has no delay, so it sleeps until local work arrives.
/// Its receive buffer fills and the runner goes idle. The target's output is
/// all buffered already, so nothing but the controller's own reads can wake
/// it: reading restores receive space, and a whole mailbox's worth of room must
/// wake the runner to fetch the rest.
#[test]
fn reading_restores_receive_space_and_wakes_an_idle_controller() {
    on_big_stack(|| async {
        let bus = SharedBus::default();
        let mut target_state = TargetState::new();
        let mut controller_state = ControllerState::<1>::new();
        let Binding {
            port: target_port,
            runner: target_runner,
        } = i2c_target(SimTarget(bus.clone()), BOOT)
            .with_storage(&mut target_state)
            .build();
        let Binding {
            port: controller_port,
            runner: controller_runner,
        } = i2c_controller(SimController(bus.clone()), NeverTimer)
            .with_open_retry_interval(Duration::ZERO)
            .with_address(ADDRESS)
            .with_storage(&mut controller_state)
            .build()
            .unwrap();
        let (mut cr, _cw) = controller_port.into_split();
        let (_tr, mut tw) = target_port.into_split();

        // Exactly the target's send buffer, buffered before any runner is polled:
        // with no delay, a controller turn that finds nothing never polls again.
        let to_controller = pattern(64, 9);
        tw.write_all(&to_controller).await.unwrap();
        let target_side = async {
            tw.flush().await.unwrap();
        };
        let controller_side = async {
            // Let the receive buffer fill and the runner go idle.
            tokio::time::sleep(Duration::from_millis(50)).await;
            // Sixteen bytes at a time: the first reads free less than a whole
            // mailbox, and the runner stays idle until one fits.
            let mut got = Vec::new();
            let mut buf = [0u8; 16];
            while got.len() < to_controller.len() {
                let n = cr.read(&mut buf).await.unwrap();
                got.extend_from_slice(&buf[..n]);
            }
            got
        };
        let (at_controller, ()) = tokio::select! {
            r = controller_runner.run() => panic!("controller runner stopped: {r:?}"),
            r = target_runner.run() => panic!("target runner stopped: {r:?}"),
            out = with_timeout(async { tokio::join!(controller_side, target_side) }) => out,
        };
        assert_eq!(at_controller, to_controller);
    });
}

/// 64 KiB each way over the simulated I2C bus, through the reliable stack.
/// Each direction is drained while the other side writes, which is more than
/// the link's receive storage, so the window and acknowledgements must keep
/// the transfer moving.
#[test]
fn reliable_i2c_duplex_exceeds_engine_receive_storage() {
    on_big_stack(|| async {
        let bus = SharedBus::default();
        let mut target_state = TargetState::new();
        let mut controller_state = ControllerState::<1>::new();
        let Binding {
            port: target_port,
            runner: target_runner,
        } = i2c_target(SimTarget(bus.clone()), BOOT)
            .with_storage(&mut target_state)
            .build();
        let Binding {
            port: controller_port,
            runner: controller_runner,
        } = i2c_controller(SimController(bus.clone()), VirtualTimer::default())
            .with_address(ADDRESS)
            .with_storage(&mut controller_state)
            .build()
            .unwrap();
        let mut controller = protolink::link::reliable(controller_port)
            .with_retransmission_timer(StdTimer::new())
            .build();
        let mut target = protolink::link::reliable(target_port)
            .with_retransmission_timer(StdTimer::new())
            .build();

        let forward = pattern(64 * 1024, 11);
        let backward = pattern(64 * 1024, 42);
        let app = async {
            let ((), at_target) = tokio::join!(
                async {
                    controller.write_all(&forward).await.unwrap();
                    controller.flush().await.unwrap();
                },
                read_exactly(&mut target, forward.len()),
            );
            assert!(
                at_target == forward,
                "controller -> target: the bytes differ"
            );

            let ((), at_controller) = tokio::join!(
                async {
                    target.write_all(&backward).await.unwrap();
                    target.flush().await.unwrap();
                },
                read_exactly(&mut controller, backward.len()),
            );
            assert!(
                at_controller == backward,
                "target -> controller: the bytes differ"
            );
        };
        tokio::select! {
            r = controller_runner.run() => panic!("controller runner stopped: {r:?}"),
            r = target_runner.run() => panic!("target runner stopped: {r:?}"),
            out = with_timeout(app) => out,
        };
    });
}
