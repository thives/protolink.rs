use alloc::vec::Vec;

use crate::Status;

/// Server-side request handler. Generated `*Server` wrappers implement this.
///
/// `call` returns `None` when the path is not handled, which lets handlers be
/// combined: a tuple `(A, B, ...)` of handlers tries each in order. The server
/// answers unhandled paths with `UNIMPLEMENTED`.
pub trait Handler {
    /// Handle the unary call `path` with the encoded `request` message.
    fn call(&mut self, path: &str, request: &[u8]) -> Option<Result<Vec<u8>, Status>>;
}

impl<H: Handler + ?Sized> Handler for &mut H {
    fn call(&mut self, path: &str, request: &[u8]) -> Option<Result<Vec<u8>, Status>> {
        (**self).call(path, request)
    }
}

/// [`Handler`] backed by a closure.
#[derive(Debug, Clone)]
pub struct FnHandler<F>(pub F);

impl<F> Handler for FnHandler<F>
where
    F: FnMut(&str, &[u8]) -> Option<Result<Vec<u8>, Status>>,
{
    fn call(&mut self, path: &str, request: &[u8]) -> Option<Result<Vec<u8>, Status>> {
        (self.0)(path, request)
    }
}

macro_rules! tuple_handler {
    ($($name:ident),+) => {
        impl<$($name: Handler),+> Handler for ($($name,)+) {
            #[allow(non_snake_case)]
            fn call(&mut self, path: &str, request: &[u8]) -> Option<Result<Vec<u8>, Status>> {
                let ($($name,)+) = self;
                $(
                    if let Some(r) = $name.call(path, request) {
                        return Some(r);
                    }
                )+
                None
            }
        }
    };
}

tuple_handler!(A);
tuple_handler!(A, B);
tuple_handler!(A, B, C);
tuple_handler!(A, B, C, D);
tuple_handler!(A, B, C, D, E);
tuple_handler!(A, B, C, D, E, F);
tuple_handler!(A, B, C, D, E, F, G);
tuple_handler!(A, B, C, D, E, F, G, H);
