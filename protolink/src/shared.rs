//! Interior mutability for the async client state.
//!
//! The state is only ever locked for short, synchronous sections and never
//! across an `.await`, so with `std` a plain mutex is enough (and makes the
//! client `Sync` when its transport is `Send`). Without `std` there is no
//! portable lock, so a `RefCell` is used and the client stays `!Sync`, which
//! single-threaded executors don't need.

#[cfg(feature = "std")]
use std::sync::{Mutex, PoisonError};

#[cfg(not(feature = "std"))]
use core::cell::RefCell;

/// A value that is accessed in short, non-reentrant sections.
#[derive(Debug)]
pub(crate) struct Shared<T> {
    #[cfg(feature = "std")]
    cell: Mutex<T>,
    #[cfg(not(feature = "std"))]
    cell: RefCell<T>,
}

impl<T> Shared<T> {
    pub(crate) fn new(value: T) -> Self {
        Self {
            #[cfg(feature = "std")]
            cell: Mutex::new(value),
            #[cfg(not(feature = "std"))]
            cell: RefCell::new(value),
        }
    }

    /// Run `f` with exclusive access.
    ///
    /// `f` must not call back into the owner of this value (that would
    /// deadlock with `std` and panic without it).
    pub(crate) fn with<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        #[cfg(feature = "std")]
        {
            // The state is consistent between sections, so a panic elsewhere
            // doesn't invalidate it.
            f(&mut self.cell.lock().unwrap_or_else(PoisonError::into_inner))
        }
        #[cfg(not(feature = "std"))]
        {
            f(&mut self.cell.borrow_mut())
        }
    }

    pub(crate) fn into_inner(self) -> T {
        #[cfg(feature = "std")]
        {
            self.cell
                .into_inner()
                .unwrap_or_else(PoisonError::into_inner)
        }
        #[cfg(not(feature = "std"))]
        {
            self.cell.into_inner()
        }
    }
}
