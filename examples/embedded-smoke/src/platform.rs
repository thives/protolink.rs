//! Minimal single-core Cortex-M0 platform; no HAL or runtime dependency.

use core::alloc::{GlobalAlloc, Layout};
use core::cell::{Cell, UnsafeCell};
use core::ptr;
use critical_section::{CriticalSection, Mutex, RawRestoreState};

struct InterruptCriticalSection;
critical_section::set_impl!(InterruptCriticalSection);

// SAFETY: on a single Cortex-M0 core, PRIMASK excludes all configurable
// interrupts in privileged mode. Saving/restoring it supports nesting and
// already-masked callers. NMI and HardFault must never enter a critical section
// or use this allocator; unprivileged thread mode is not supported.
unsafe impl critical_section::Impl for InterruptCriticalSection {
    unsafe fn acquire() -> RawRestoreState {
        let primask: u32;
        // Do not use `nomem`: these assembly blocks are compiler memory barriers.
        unsafe {
            core::arch::asm!(
                "mrs {}, PRIMASK",
                "cpsid i",
                out(reg) primask,
                options(nostack, preserves_flags),
            );
        }
        primask
    }

    unsafe fn release(primask: RawRestoreState) {
        unsafe {
            core::arch::asm!(
                "msr PRIMASK, {}",
                in(reg) primask,
                options(nostack, preserves_flags),
            );
        }
    }
}

const HEAP_SIZE: usize = 32 * 1024;

struct BumpAllocator {
    storage: UnsafeCell<[u8; HEAP_SIZE]>,
    next: Mutex<Cell<usize>>,
}

// SAFETY: the cursor is accessed only with interrupts masked. Allocations are
// disjoint and never reused; clients own their bytes after leaving the section.
unsafe impl Sync for BumpAllocator {}

impl BumpAllocator {
    fn allocate(&self, cs: CriticalSection<'_>, layout: Layout) -> *mut u8 {
        let base = self.storage.get().cast::<u8>();
        let next = self.next.borrow(cs);
        let Some(address) = (base as usize)
            .checked_add(next.get())
            .and_then(|address| address.checked_add(layout.align() - 1))
            .map(|address| address & !(layout.align() - 1))
        else {
            return ptr::null_mut();
        };
        let offset = address - base as usize;
        let Some(end) = offset
            .checked_add(layout.size())
            .filter(|end| *end <= HEAP_SIZE)
        else {
            return ptr::null_mut();
        };
        next.set(end);
        // SAFETY: offset..end lies in the arena; pointer alignment was checked
        // above, and the monotonic cursor gives exclusive allocation ownership.
        unsafe { base.add(offset) }
    }
}

// SAFETY: allocate provides disjoint, aligned ranges, or null on exhaustion.
// dealloc deliberately leaks: this bounded link-smoke arena is not a production
// allocator and must not be used for a long-lived service.
unsafe impl GlobalAlloc for BumpAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        critical_section::with(|cs| self.allocate(cs, layout))
    }

    unsafe fn dealloc(&self, _ptr: *mut u8, _layout: Layout) {}
}

#[global_allocator]
static ALLOCATOR: BumpAllocator = BumpAllocator {
    storage: UnsafeCell::new([0; HEAP_SIZE]),
    next: Mutex::new(Cell::new(0)),
};

// Cortex-M0 vectors: initial MSP, reset, 14 core exceptions and 32 IRQ slots.
// ARM ELF function relocations preserve the Thumb bit in these entries.
core::arch::global_asm!(
    ".section .vector_table, \"a\", %progbits",
    ".balign 256",
    ".word __stack_top",
    ".word Reset",
    ".rept 46",
    ".word DefaultHandler",
    ".endr",
);

unsafe extern "C" {
    static __sidata: u8;
    static mut __sdata: u8;
    static mut __edata: u8;
    static mut __sbss: u8;
    static mut __ebss: u8;
}

#[unsafe(no_mangle)]
unsafe extern "C" fn Reset() -> ! {
    // SAFETY: link.x defines non-overlapping FLASH load and RAM data ranges,
    // and the vector table initializes MSP before reset. No Rust globals have
    // been used yet, and interrupts remain masked throughout initialization.
    unsafe {
        core::arch::asm!("cpsid i", options(nostack, preserves_flags));
        let data = ptr::addr_of_mut!(__sdata);
        let data_len = ptr::addr_of_mut!(__edata) as usize - data as usize;
        ptr::copy_nonoverlapping(ptr::addr_of!(__sidata), data, data_len);
        let bss = ptr::addr_of_mut!(__sbss);
        let bss_len = ptr::addr_of_mut!(__ebss) as usize - bss as usize;
        ptr::write_bytes(bss, 0, bss_len);
        core::arch::asm!("cpsie i", options(nostack, preserves_flags));
    }
    crate::smoke_main()
}

#[unsafe(no_mangle)]
unsafe extern "C" fn DefaultHandler() -> ! {
    halt()
}

pub fn halt() -> ! {
    // No allocation, logging, or critical-section use in fault/panic paths.
    unsafe { core::arch::asm!("cpsid i", options(nostack, preserves_flags)) };
    loop {
        unsafe { core::arch::asm!("wfi", options(nostack, preserves_flags)) };
    }
}
