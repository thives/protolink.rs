//! Linked ARM smoke only; this is not a board example or a hardware test.
#![no_std]
#![no_main]

#[cfg(not(all(target_arch = "arm", target_os = "none")))]
compile_error!("build this binary for thumbv6m-none-eabi");

mod platform;

fn smoke_main() -> ! {
    // Retain concrete generated clients, both server drivers, and ring wakers
    // in the final ELF. The stub transport does not perform actual RPCs.
    embedded_smoke::blocking_smoke();
    let mut future = core::pin::pin!(embedded_smoke::async_smoke());
    let mut cx = core::task::Context::from_waker(core::task::Waker::noop());
    let _ = core::hint::black_box(core::future::Future::poll(future.as_mut(), &mut cx));
    #[cfg(feature = "compression")]
    embedded_smoke::compression_smoke();
    platform::halt()
}

#[panic_handler]
fn panic(_info: &core::panic::PanicInfo<'_>) -> ! {
    platform::halt()
}
