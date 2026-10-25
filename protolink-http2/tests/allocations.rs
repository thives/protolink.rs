//! Count allocations on the endpoint thread, excluding setup and the peer.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use protolink_http2::{Config, Connection, HeaderField};

thread_local! {
    static ALLOCATIONS: Cell<Option<usize>> = const { Cell::new(None) };
}

struct CountingAllocator;

fn record_allocation() {
    // The constant-initialized, destructor-free cell does not allocate. Ignore
    // access failure during thread teardown rather than panicking in an allocator.
    let _ = ALLOCATIONS.try_with(|counter| {
        if let Some(count) = counter.get() {
            counter.set(Some(count + 1));
        }
    });
}

// SAFETY: allocation and deallocation are forwarded unchanged to System. The
// instrumentation only touches a non-allocating thread-local counter.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        // SAFETY: the caller supplies the layout required by GlobalAlloc.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        // SAFETY: the caller supplies the layout required by GlobalAlloc.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: allocations come from System with the unchanged layout.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record_allocation();
        // SAFETY: the caller supplies System's allocation and a valid new size.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn measure<T>(f: impl FnOnce() -> T) -> (T, usize) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            ALLOCATIONS.with(|counter| counter.set(None));
        }
    }

    ALLOCATIONS.with(|counter| {
        assert!(counter.get().is_none(), "nested allocation measurement");
        counter.set(Some(0));
    });
    let reset = Reset;
    let result = f();
    let count = ALLOCATIONS.with(|counter| counter.get().unwrap());
    drop(reset);
    (result, count)
}

fn request_headers() -> Vec<HeaderField> {
    [
        (":method", "POST"),
        (":scheme", "http"),
        (":path", "/test.Service/Upload"),
        (":authority", "localhost"),
    ]
    .into_iter()
    .map(|(name, value)| HeaderField {
        name: name.into(),
        value: value.into(),
    })
    .collect()
}

#[test]
fn data_serialization_reuses_output_without_payload_scratch_allocations() {
    for size in [0, 1, 4096, 16_384, 20_000] {
        let mut client = Connection::client(Config::default());
        let id = client.open_stream(request_headers(), false).unwrap();
        client.consume_output(client.pending_output().len());

        // Warm the serialized output and outbound queue, then keep their capacity.
        client.send_data(id, vec![0x5a; size], false).unwrap();
        let expected = client.pending_output().to_vec();
        client.consume_output(expected.len());
        let payload = vec![0x5a; size];

        let (result, allocations) = measure(|| client.send_data(id, payload, false));
        result.unwrap();
        assert_eq!(client.pending_output(), expected);
        assert_eq!(client.queued_send_bytes(id), Some(0));
        assert_eq!(allocations, 0, "payload size {size}");
    }
}

#[test]
fn blocked_stream_flush_does_not_allocate_a_stream_id_snapshot() {
    let mut client = Connection::client(Config::default());
    let mut peer = Connection::server(Config {
        initial_window_size: 0,
        ..Config::default()
    });
    client.recv(&peer.take_output()).unwrap();
    let id = client.open_stream(request_headers(), false).unwrap();
    client.consume_output(client.pending_output().len());
    client.send_data(id, vec![0x5a; 4096], false).unwrap();
    assert_eq!(client.queued_send_bytes(id), Some(4096));
    assert!(client.pending_output().is_empty());

    for _ in 0..16 {
        let (result, allocations) = measure(|| client.recv(&[]));
        result.unwrap();
        assert_eq!(allocations, 0);
        assert_eq!(client.queued_send_bytes(id), Some(4096));
        assert!(client.pending_output().is_empty());
    }
}
