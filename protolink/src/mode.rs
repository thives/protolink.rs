//! The transport modes of the async drivers.
//!
//! A driver is built over one of three modes, which say how the transport is
//! served:
//!
//! - [`Serial`]: one object, read and written in turn. A blocked write stops
//!   input, so the transport must buffer what its peer sends meanwhile.
//! - [`Duplex`]: independent read and write halves. Input keeps flowing while
//!   output is blocked on the same connection.
//! - [`CoordinatedReliable`]: a reliable link whose ARQ engine is one object,
//!   polled in both directions by a single worker.
//!
//! The modes are sealed: [`Mode`] is implemented by these three types only.

use core::future::Future;

use embedded_io_async::{Read, Write};

use crate::driver::{self, Port};
use crate::link::{ReliableError, ReliableLink, Timer};
use crate::shared::Shared;

mod private {
    pub trait Sealed {}
}

/// How a transport is served. Implemented by [`Serial`], [`Duplex`] and
/// [`CoordinatedReliable`] only.
pub trait Mode: private::Sealed {
    /// The error of the transport. Both halves of a duplex transport have it.
    type Error;

    /// The worker that serves this transport through `port`. It never completes
    /// on its own, and it is dropped to stop it.
    fn worker<'a>(
        &'a mut self,
        port: &'a Shared<Port<Self::Error>>,
    ) -> impl Future<Output = ()> + 'a;

    /// Write `buf`, as [`Write::write`] does, for the sequential drivers.
    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, Self::Error>> + 'a;

    /// Flush, as [`Write::flush`] does, for the sequential drivers.
    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Self::Error>> + 'a;

    /// Read into `buf`, as [`Read::read`] does, for the sequential drivers.
    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Self::Error>> + 'a;
}

/// A transport served in serial mode: one object, read and written in turn.
#[derive(Debug)]
pub struct Serial<IO>(IO);

impl<IO> Serial<IO> {
    /// Wrap a transport that is read and written in turn.
    pub fn new(io: IO) -> Self {
        Self(io)
    }

    /// Unwrap the transport.
    pub fn into_inner(self) -> IO {
        self.0
    }
}

impl<IO> private::Sealed for Serial<IO> {}

impl<IO: Read + Write> Mode for Serial<IO> {
    type Error = IO::Error;

    fn worker<'a>(
        &'a mut self,
        port: &'a Shared<Port<Self::Error>>,
    ) -> impl Future<Output = ()> + 'a {
        driver::run_serial(&mut self.0, port)
    }

    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, Self::Error>> + 'a {
        Write::write(&mut self.0, buf)
    }

    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Self::Error>> + 'a {
        Write::flush(&mut self.0)
    }

    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Self::Error>> + 'a {
        Read::read(&mut self.0, buf)
    }
}

/// A transport served in duplex mode: a read half and a write half that are
/// polled independently.
///
/// The halves are not required to be `Send`, `'static` or `Unpin`. Their
/// errors must match, so one error type describes the connection.
#[derive(Debug)]
pub struct Duplex<R, W> {
    reader: R,
    writer: W,
}

impl<R, W> Duplex<R, W>
where
    R: Read,
    W: Write<Error = R::Error>,
{
    /// Pair a read half with a write half of the same transport.
    pub fn new(reader: R, writer: W) -> Self {
        Self { reader, writer }
    }

    /// Unwrap the two halves.
    pub fn into_parts(self) -> (R, W) {
        (self.reader, self.writer)
    }
}

impl<R, W> private::Sealed for Duplex<R, W> {}

impl<R, W> Mode for Duplex<R, W>
where
    R: Read,
    W: Write<Error = R::Error>,
{
    type Error = R::Error;

    fn worker<'a>(
        &'a mut self,
        port: &'a Shared<Port<Self::Error>>,
    ) -> impl Future<Output = ()> + 'a {
        driver::run_duplex(&mut self.reader, &mut self.writer, port)
    }

    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, Self::Error>> + 'a {
        Write::write(&mut self.writer, buf)
    }

    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Self::Error>> + 'a {
        Write::flush(&mut self.writer)
    }

    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Self::Error>> + 'a {
        Read::read(&mut self.reader, buf)
    }
}

/// A reliable link served as one coordinated engine.
///
/// The link is ARQ over COBS over the raw transport, see
/// [`link::reliable`](crate::link::reliable). Its reads and writes are polled
/// by one worker, so the link's own state stays in one place.
pub struct CoordinatedReliable<S: Read + Write, L: Timer> {
    link: ReliableLink<S, L>,
}

impl<S: Read + Write, L: Timer> CoordinatedReliable<S, L> {
    /// Use a built reliable link.
    pub fn new(link: ReliableLink<S, L>) -> Self {
        Self { link }
    }

    /// Unwrap the link.
    pub fn into_link(self) -> ReliableLink<S, L> {
        self.link
    }
}

impl<S: Read + Write, L: Timer> private::Sealed for CoordinatedReliable<S, L> {}

impl<S: Read + Write, L: Timer> Mode for CoordinatedReliable<S, L> {
    type Error = ReliableError<S::Error>;

    fn worker<'a>(
        &'a mut self,
        port: &'a Shared<Port<Self::Error>>,
    ) -> impl Future<Output = ()> + 'a {
        driver::run_reliable(&mut self.link, port)
    }

    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, Self::Error>> + 'a {
        Write::write(&mut self.link, buf)
    }

    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Self::Error>> + 'a {
        Write::flush(&mut self.link)
    }

    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Self::Error>> + 'a {
        Read::read(&mut self.link, buf)
    }
}
