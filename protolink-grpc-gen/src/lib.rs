//! # protolink-grpc-gen
//!
//! Generates gRPC service bindings for [protolink] from `.proto` service
//! definitions. Message types are produced by [`micropb-gen`]; this crate
//! produces the service glue that `tonic-build` would normally provide:
//!
//! - method path constants,
//! - a service trait with methods for every RPC (unary, server-streaming,
//!   client-streaming and bidirectional streaming),
//! - a `<Service>Server<S>` wrapper implementing `protolink_grpc::Handler`,
//! - an async `<Service>Client<T>`: unary methods for
//!   `T: UnaryTransport`, streaming methods for `T: StreamingTransport`,
//! - a `<Service>BlockingClient<T>`, the same over the blocking transports.
//!
//! ## Streaming methods
//!
//! Servers are sans-IO and executor-agnostic: each streaming call is
//! identified by the `CallId` in its `CallContext`, request messages are delivered to the service
//! as they arrive, and responses are pulled with a `poll_<method>` function
//! taking a [`Context`](core::task::Context). For an RPC `Method`:
//!
//! | Shape | Trait methods |
//! |---|---|
//! | unary | `method(ctx, request) -> Result<Resp, Status>` |
//! | server streaming | `method(ctx, request)`, `poll_method(ctx, cx) -> Poll<Next<Resp>>`, `cancel_method(ctx)` |
//! | client streaming | `method(ctx, request)` per message, `poll_method(ctx, cx) -> Poll<Result<Resp, Status>>` after the client half-closes, `cancel_method(ctx)` |
//! | bidirectional | `method(ctx, request)` per message, `end_method(ctx)` on half-close, `poll_method(ctx, cx) -> Poll<Next<Resp>>`, `cancel_method(ctx)` |
//!
//! Streaming methods have default implementations that answer
//! `UNIMPLEMENTED`, so adding a streaming RPC to a `.proto` does not break
//! existing implementations. See `protolink_grpc::Handler` for the call
//! lifecycle.
//!
//! ## Metadata
//!
//! Handlers read the request's custom metadata and set the response's through the
//! `&mut CallContext` every service method gets. On the client, unary
//! `<method>_with_options` returns a `protolink_grpc::Response<T>` (message, headers, trailers)
//! and takes request metadata in its `CallOptions`; the typed streaming wrappers have
//! `headers()` and `trailers()`, and `ClientStreaming` has `finish_with_metadata`.
//!
//! Clients return typed call wrappers from `protolink_grpc::codec`:
//! `ServerStreaming` (after sending the request), `ClientStreaming` and
//! `BidiStreaming`, or their `Blocking*` counterparts.
//!
//! ## Usage from `build.rs`
//!
//! ```no_run
//! let out = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());
//! let mut grpc = protolink_grpc_gen::Generator::new();
//! // Write the parsed descriptors so micropb-gen can reuse them without protoc.
//! grpc.file_descriptor_set_path(out.join("fdset.bin"));
//! grpc.compile_protos(&["proto/service.proto"], out.join("grpc.rs")).unwrap();
//!
//! // micropb_gen::Generator::new()
//! //     .compile_fdset_file(out.join("fdset.bin"), out.join("messages.rs"))
//! //     .unwrap();
//! ```
//!
//! Include both files in the same module:
//!
//! ```ignore
//! pub mod proto {
//!     include!(concat!(env!("OUT_DIR"), "/messages.rs"));
//!     include!(concat!(env!("OUT_DIR"), "/grpc.rs"));
//! }
//! ```
//!
//! Type names are resolved exactly like micropb-gen does (`package_` modules,
//! `Message_` modules for nested types). Keep [`Generator::suffixed_package_names`]
//! and [`Generator::extern_type_path`] in sync with your micropb-gen settings.
//!
//! [protolink]: https://github.com/thives/protolink.rs
//! [`micropb-gen`]: https://docs.rs/micropb-gen

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use protobuf::Message as _;
use protobuf::descriptor::{
    FileDescriptorProto, FileDescriptorSet, MethodDescriptorProto, ServiceDescriptorProto,
};

/// Generator errors.
#[derive(Debug)]
pub enum Error {
    /// Reading or writing a file failed.
    Io(io::Error),
    /// The `.proto` files or descriptor set could not be parsed.
    Parse(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(e) => write!(f, "io error: {e}"),
            Error::Parse(e) => write!(f, "proto parse error: {e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

/// Code generator configuration.
#[derive(Debug, Clone)]
pub struct Generator {
    includes: Vec<PathBuf>,
    use_protoc: bool,
    fdset_path: Option<PathBuf>,
    messages_path: String,
    runtime_path: String,
    suffixed_package_names: bool,
    extern_paths: HashMap<String, String>,
    server: bool,
    client: bool,
    blocking_client: bool,
}

impl Default for Generator {
    fn default() -> Self {
        Self::new()
    }
}

impl Generator {
    /// New generator with default settings.
    pub fn new() -> Self {
        Self {
            includes: Vec::new(),
            use_protoc: false,
            fdset_path: None,
            messages_path: String::new(),
            runtime_path: "::protolink::grpc".into(),
            suffixed_package_names: true,
            extern_paths: HashMap::new(),
            server: true,
            client: true,
            blocking_client: true,
        }
    }

    /// Add an include directory for import resolution. Defaults to the parent
    /// directory of every input file.
    pub fn include(&mut self, path: impl Into<PathBuf>) -> &mut Self {
        self.includes.push(path.into());
        self
    }

    /// Parse with `protoc` (from `PROTOC` or `PATH`) instead of the built-in
    /// pure-Rust parser.
    pub fn use_protoc(&mut self, flag: bool) -> &mut Self {
        self.use_protoc = flag;
        self
    }

    /// Also write the parsed `FileDescriptorSet` here. Feed it to
    /// `micropb_gen::Generator::compile_fdset_file` to generate the messages
    /// from the exact same parse, without needing `protoc`.
    pub fn file_descriptor_set_path(&mut self, path: impl Into<PathBuf>) -> &mut Self {
        self.fdset_path = Some(path.into());
        self
    }

    /// Rust path of the module containing the micropb-generated code, as seen
    /// from where the generated file is `include!`d. Empty (default) means the
    /// same module. Paths starting with `crate::` or `::` are absolute.
    pub fn messages_path(&mut self, path: impl Into<String>) -> &mut Self {
        self.messages_path = path.into();
        self
    }

    /// Path of the protolink gRPC runtime. Defaults to `::protolink::grpc`;
    /// use `::protolink_grpc` when depending on `protolink-grpc` directly.
    pub fn runtime_path(&mut self, path: impl Into<String>) -> &mut Self {
        self.runtime_path = path.into();
        self
    }

    /// Mirror of `micropb_gen::Generator::suffixed_package_names` (default `true`).
    pub fn suffixed_package_names(&mut self, flag: bool) -> &mut Self {
        self.suffixed_package_names = flag;
        self
    }

    /// Mirror of `micropb_gen::Generator::extern_type_path`: use `rust_path`
    /// for the fully-qualified protobuf type `proto_path` (e.g. `.pkg.Msg`).
    pub fn extern_type_path(
        &mut self,
        proto_path: impl AsRef<str>,
        rust_path: impl Into<String>,
    ) -> &mut Self {
        let p = proto_path.as_ref();
        let p = if p.starts_with('.') {
            p.to_owned()
        } else {
            format!(".{p}")
        };
        self.extern_paths.insert(p, rust_path.into());
        self
    }

    /// Generate service traits and server wrappers (default `true`).
    pub fn server(&mut self, flag: bool) -> &mut Self {
        self.server = flag;
        self
    }

    /// Generate async clients (default `true`).
    pub fn client(&mut self, flag: bool) -> &mut Self {
        self.client = flag;
        self
    }

    /// Generate blocking clients (default `true`).
    pub fn blocking_client(&mut self, flag: bool) -> &mut Self {
        self.blocking_client = flag;
        self
    }

    /// Parse `protos` and write the bindings for all their services to `out_file`.
    pub fn compile_protos(
        &self,
        protos: &[impl AsRef<Path>],
        out_file: impl AsRef<Path>,
    ) -> Result<(), Error> {
        let mut parser = protobuf_parse::Parser::new();
        if self.use_protoc {
            parser.protoc();
        } else {
            parser.pure();
        }
        if self.includes.is_empty() {
            for p in protos {
                let parent = p.as_ref().parent().filter(|p| !p.as_os_str().is_empty());
                parser.include(parent.unwrap_or(Path::new(".")));
            }
        } else {
            parser.includes(&self.includes);
        }
        parser.inputs(protos);
        let mut set = parser
            .file_descriptor_set()
            .map_err(|e| Error::Parse(format!("{e:#}")))?;
        for file in &mut set.file {
            for msg in &mut file.message_type {
                normalize_synthetic_oneofs(msg);
            }
        }
        if let Some(path) = &self.fdset_path {
            let bytes = set
                .write_to_bytes()
                .map_err(|e| Error::Parse(e.to_string()))?;
            fs::write(path, bytes)?;
        }
        fs::write(out_file, self.generate(&set.file)?)?;
        Ok(())
    }

    /// Generate bindings from an encoded `FileDescriptorSet` (e.g. written by
    /// `protoc -o` or micropb-gen's `file_descriptor_set_path`).
    pub fn compile_fdset_file(
        &self,
        fdset: impl AsRef<Path>,
        out_file: impl AsRef<Path>,
    ) -> Result<(), Error> {
        let bytes = fs::read(fdset)?;
        let set =
            FileDescriptorSet::parse_from_bytes(&bytes).map_err(|e| Error::Parse(e.to_string()))?;
        fs::write(out_file, self.generate(&set.file)?)?;
        Ok(())
    }

    /// Generate the Rust source for all services in `files`.
    pub fn generate(&self, files: &[FileDescriptorProto]) -> Result<String, Error> {
        let services: Vec<(&str, &ServiceDescriptorProto)> = files
            .iter()
            .flat_map(|fd| fd.service.iter().map(move |s| (fd.package(), s)))
            .collect();

        // Count Rust identifiers, not protobuf spellings: escaping can also
        // merge names (e.g. `Self` and `Self_`).
        let mut counts: BTreeMap<String, usize> = BTreeMap::new();
        for (_, svc) in &services {
            let module = rust_ident(&snake(svc.name()));
            *counts.entry(ident_key(&module).to_owned()).or_default() += 1;
        }

        let reserved = self.reserved_root_names(files);

        // Ordinary candidates first; a candidate that shadows a message-side
        // module or type gets the `_grpc` fallback.
        let mut chosen = Vec::new();
        for (package, svc) in services {
            let base = rust_ident(&snake(svc.name()));
            let module = if counts[ident_key(&base)] > 1 && !package.is_empty() {
                rust_ident(&format!(
                    "{}_{}",
                    snake(&package.replace('.', "_")),
                    ident_key(&base)
                ))
            } else {
                base
            };
            let module = if reserved.contains_key(ident_key(&module)) {
                format!("{}_grpc", ident_key(&module))
            } else {
                module
            };
            chosen.push((package, svc, module));
        }

        // Validate every final name together, independent of descriptor order.
        // Qualification is not injective (`a.b` versus `a_b`), and a qualified
        // name can collide with another service's short name.
        let mut modules: BTreeMap<&str, Vec<String>> = BTreeMap::new();
        for (package, svc, module) in &chosen {
            modules
                .entry(ident_key(module))
                .or_default()
                .push(service_name(package, svc));
        }
        for (key, mut names) in modules {
            names.sort();
            if let Some(owner) = reserved.get(key) {
                return Err(Error::Parse(format!(
                    "services {} generate a module named `{key}`, which collides with {owner}; \
                     the `_grpc` fallback is also taken",
                    quoted(&names)
                )));
            }
            if names.len() > 1 {
                return Err(Error::Parse(format!(
                    "services {} both generate a module named `{key}`",
                    quoted(&names)
                )));
            }
        }

        let mut out = String::from("// @generated by protolink-grpc-gen. DO NOT EDIT.\n");
        for (package, svc, module) in chosen {
            self.gen_service(&mut out, package, svc, &module)?;
        }
        Ok(out)
    }

    /// Names that micropb-gen defines in the namespace the service modules are
    /// included into, with a description of each. Empty when the messages live
    /// in a different module than the one the bindings are included in.
    fn reserved_root_names(&self, files: &[FileDescriptorProto]) -> BTreeMap<String, String> {
        let mut names = BTreeMap::new();
        let messages_path = self
            .messages_path
            .trim_start_matches("self::")
            .trim_end_matches("::");
        if !messages_path.is_empty() {
            return names;
        }
        let suffixed = self.suffixed_package_names;
        let mut reserve = |name: String, what: String| {
            names.entry(ident_key(&name).to_owned()).or_insert(what);
        };
        for file in files {
            let package = file.package();
            if !package.is_empty() {
                let root = package.split('.').next().unwrap_or(package);
                reserve(
                    resolve_path_elem(root, suffixed),
                    format!("the message module for package `{package}`"),
                );
                continue;
            }
            for msg in &file.message_type {
                reserve(
                    sanitized_ident(msg.name()),
                    format!("message `{}`", msg.name()),
                );
                if !msg.nested_type.is_empty()
                    || !msg.enum_type.is_empty()
                    || !msg.oneof_decl.is_empty()
                {
                    reserve(
                        resolve_path_elem(msg.name(), suffixed),
                        format!("the nested-type module of message `{}`", msg.name()),
                    );
                }
            }
            for en in &file.enum_type {
                reserve(sanitized_ident(en.name()), format!("enum `{}`", en.name()));
            }
        }
        names
    }

    /// Rust type path for a fully-qualified protobuf type, as seen from inside
    /// a generated service module. Mirrors micropb-gen's `resolve_type_name`.
    fn type_path(&self, proto_type: &str) -> Result<String, Error> {
        if !proto_type.starts_with('.') {
            return Err(Error::Parse(format!(
                "type name `{proto_type}` is not fully qualified"
            )));
        }
        if let Some(p) = self.extern_paths.get(proto_type) {
            return Ok(p.clone());
        }
        let segs: Vec<&str> = proto_type[1..].split('.').collect();
        let (last, parents) = segs.split_last().expect("non-empty type name");

        let base = self
            .messages_path
            .trim_start_matches("self::")
            .trim_end_matches("::");
        let mut path = if base == "crate" || base.starts_with("crate::") || base.starts_with("::") {
            base.to_owned()
        } else if base.is_empty() {
            "super".to_owned()
        } else {
            format!("super::{base}")
        };
        for p in parents {
            path.push_str("::");
            path.push_str(&resolve_path_elem(p, self.suffixed_package_names));
        }
        path.push_str("::");
        path.push_str(&sanitized_ident(last));
        Ok(path)
    }

    fn gen_service(
        &self,
        out: &mut String,
        package: &str,
        svc: &ServiceDescriptorProto,
        module: &str,
    ) -> Result<(), Error> {
        let full = service_name(package, svc);
        let name = rust_ident(svc.name());
        let rt = &self.runtime_path;

        let methods = svc
            .method
            .iter()
            .map(|m| {
                let s = snake(m.name());
                Ok(M {
                    name: m.name().to_owned(),
                    kind: Kind::of(m),
                    konst: format!("METHOD_{}", s.to_uppercase()),
                    func: rust_ident(&s),
                    with_options: format!("{s}_with_options"),
                    poll: format!("poll_{s}"),
                    end: format!("end_{s}"),
                    cancel: format!("cancel_{s}"),
                    req: self.type_path(m.input_type())?,
                    resp: self.type_path(m.output_type())?,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;

        let check_names = |names: Vec<(&str, String)>, namespace: &str| -> Result<(), Error> {
            let mut seen = BTreeMap::new();
            for (rpc, ident) in names {
                if let Some(other) = seen.insert(ident_key(&ident).to_owned(), rpc) {
                    return Err(Error::Parse(format!(
                        "service `{full}`: RPCs `{other}` and `{rpc}` both generate a {namespace} named `{ident}`"
                    )));
                }
            }
            Ok(())
        };
        check_names(
            methods
                .iter()
                .map(|m| (m.name.as_str(), m.konst.clone()))
                .collect(),
            "constant",
        )?;
        if self.server {
            check_names(
                methods
                    .iter()
                    .flat_map(|m| m.trait_fns().into_iter().map(|f| (m.name.as_str(), f)))
                    .collect(),
                "server method",
            )?;
        }
        if self.client || self.blocking_client {
            for m in &methods {
                for f in [&m.func, &m.with_options] {
                    if matches!(ident_key(f), "new" | "into_inner" | "transport_mut") {
                        return Err(Error::Parse(format!(
                            "service `{full}`: RPC `{}` generates reserved client method `{f}`",
                            m.name
                        )));
                    }
                }
            }
            check_names(
                methods
                    .iter()
                    .flat_map(|m| {
                        [&m.func, &m.with_options]
                            .into_iter()
                            .map(|f| (m.name.as_str(), f.clone()))
                    })
                    .collect(),
                "client method",
            )?;
        }
        let streaming = methods.iter().any(|m| m.kind != Kind::Unary);
        let unary = methods.iter().any(|m| m.kind == Kind::Unary);

        let w = &mut *out;
        let _ = writeln!(w, "\n/// gRPC bindings for `{full}`.");
        let _ = writeln!(w, "#[allow(dead_code, unused_imports, clippy::all)]");
        let _ = writeln!(w, "pub mod {module} {{");
        let _ = writeln!(w, "    use {rt} as __rt;");
        let _ = writeln!(w);
        let _ = writeln!(w, "    /// Fully-qualified service name.");
        let _ = writeln!(w, "    pub const SERVICE_NAME: &str = \"{full}\";");
        for m in &methods {
            let _ = writeln!(w, "    /// Path of `{}`.", m.name);
            let _ = writeln!(
                w,
                "    pub const {}: &str = \"/{full}/{}\";",
                m.konst, m.name
            );
        }
        let list: Vec<&str> = methods.iter().map(|m| m.konst.as_str()).collect();
        let _ = writeln!(w, "    /// All method paths.");
        let _ = writeln!(
            w,
            "    pub const METHODS: &[&str] = &[{}];",
            list.join(", ")
        );

        if self.server {
            self.gen_server(w, &full, &name, &methods, streaming);
        }
        if self.client {
            gen_client(
                w,
                &full,
                &name,
                &methods,
                ClientFlavor::ASYNC,
                unary,
                streaming,
            );
        }
        if self.blocking_client {
            gen_client(
                w,
                &full,
                &name,
                &methods,
                ClientFlavor::BLOCKING,
                unary,
                streaming,
            );
        }
        let _ = writeln!(w, "}}");
        Ok(())
    }

    fn gen_server(&self, w: &mut String, full: &str, name: &str, methods: &[M], streaming: bool) {
        let _ = writeln!(w, "\n    /// Server-side implementation of `{full}`.");
        if streaming {
            let _ = writeln!(
                w,
                "    ///\n    \
                 /// Every method gets a `CallContext` (path, `CallId` unique per connection, deadline,\n    \
                 /// request metadata, and where to set the response metadata).\n    \
                 /// Request messages are delivered as they arrive and responses are\n    \
                 /// pulled with the `poll_*` methods; on `Poll::Pending`, wake `cx` once a\n    \
                 /// response may be ready. Unless overridden, streaming methods answer\n    \
                 /// `UNIMPLEMENTED`. See `Handler` in the protolink gRPC runtime for the\n    \
                 /// full call lifecycle."
            );
        }
        let _ = writeln!(w, "    pub trait {name} {{");
        let mut first = true;
        for m in methods {
            if !first {
                w.push('\n');
            }
            first = false;
            let (n, func, req, resp) = (&m.name, &m.func, &m.req, &m.resp);
            let (poll, end, cancel) = (&m.poll, &m.end, &m.cancel);
            let unimpl = format!("__rt::Status::unimplemented(\"`{n}` is not implemented\")");
            let start = |w: &mut String, doc: &str| {
                let _ = writeln!(
                    w,
                    "        /// {doc}\n        \
                     fn {func}(&mut self, ctx: &mut __rt::CallContext<'_>, request: {req}) -> ::core::result::Result<(), __rt::Status> {{\n            \
                     let _ = (ctx, request);\n            \
                     Err({unimpl})\n        }}\n"
                );
            };
            let poll_stream = |w: &mut String, doc: &str| {
                let _ = writeln!(
                    w,
                    "        /// {doc}\n        \
                     /// Return `Next::Message` for each response, then `Next::Done` with\n        \
                     /// the final status.\n        \
                     fn {poll}(&mut self, ctx: &mut __rt::CallContext<'_>, cx: &mut __rt::__private::Context<'_>) -> __rt::__private::Poll<__rt::Next<{resp}>> {{\n            \
                     let _ = (ctx, cx);\n            \
                     __rt::__private::Poll::Ready(__rt::Next::Done(Err({unimpl})))\n        }}\n"
                );
            };
            let cancel_fn = |w: &mut String| {
                let _ = writeln!(
                    w,
                    "        /// A `{n}` call ended without the service finishing it (cancelled,\n        \
                     /// reset, malformed or the connection closed): release its state.\n        \
                     fn {cancel}(&mut self, ctx: &mut __rt::CallContext<'_>) {{\n            \
                     let _ = ctx;\n        }}"
                );
            };
            match m.kind {
                Kind::Unary => {
                    let _ = writeln!(
                        w,
                        "        /// Handle `{n}`.\n        \
                         fn {func}(&mut self, ctx: &mut __rt::CallContext<'_>, request: {req}) -> ::core::result::Result<{resp}, __rt::Status>;"
                    );
                }
                Kind::Server => {
                    start(
                        w,
                        &format!(
                            "Start a `{n}` call (server streaming) with its request.\n        \
                             /// Responses are then pulled with `{poll}`."
                        ),
                    );
                    poll_stream(w, &format!("Next response of a `{n}` call."));
                    cancel_fn(w);
                }
                Kind::Client => {
                    start(
                        w,
                        &format!(
                            "One request message of a `{n}` call (client streaming), in order."
                        ),
                    );
                    let _ = writeln!(
                        w,
                        "        /// The response of a `{n}` call, polled once the client has sent\n        \
                         /// all of its requests.\n        \
                         fn {poll}(&mut self, ctx: &mut __rt::CallContext<'_>, cx: &mut __rt::__private::Context<'_>) -> __rt::__private::Poll<::core::result::Result<{resp}, __rt::Status>> {{\n            \
                         let _ = (ctx, cx);\n            \
                         __rt::__private::Poll::Ready(Err({unimpl}))\n        }}\n"
                    );
                    cancel_fn(w);
                }
                Kind::Bidi => {
                    start(
                        w,
                        &format!(
                            "One request message of a `{n}` call (bidirectional streaming),\n        \
                             /// in order."
                        ),
                    );
                    let _ = writeln!(
                        w,
                        "        /// The client finished sending requests on a `{n}` call.\n        \
                         fn {end}(&mut self, ctx: &mut __rt::CallContext<'_>) -> ::core::result::Result<(), __rt::Status> {{\n            \
                         let _ = ctx;\n            \
                         Ok(())\n        }}\n"
                    );
                    poll_stream(
                        w,
                        &format!(
                            "Next response of a `{n}` call, polled from the start of the call."
                        ),
                    );
                    cancel_fn(w);
                }
            }
        }
        let _ = writeln!(w, "    }}\n");
        let _ = writeln!(
            w,
            "    /// Routes `{full}` requests to a [`{name}`] implementation.\n    \
             #[derive(Debug, Clone, Default)]\n    \
             pub struct {name}Server<S>(pub S);\n\n    \
             impl<S> {name}Server<S> {{\n        \
             /// Wrap a service implementation.\n        \
             pub fn new(service: S) -> Self {{\n            Self(service)\n        }}\n\n        \
             /// Unwrap the service implementation.\n        \
             pub fn into_inner(self) -> S {{\n            self.0\n        }}\n    }}\n"
        );
        let _ = writeln!(
            w,
            "    impl<S: self::{name}> __rt::Handler for {name}Server<S> {{"
        );

        // Unary dispatch.
        let _ = writeln!(
            w,
            "        fn call(&mut self, ctx: &mut __rt::CallContext<'_>, request: &[u8]) -> ::core::option::Option<::core::result::Result<__rt::__private::Vec<u8>, __rt::Status>> {{\n            \
             match ctx.path {{"
        );
        for m in methods {
            if m.kind == Kind::Unary {
                let _ = writeln!(
                    w,
                    "                {} => Some(__rt::codec::unary(request, |req| <S as self::{name}>::{}(&mut self.0, ctx, req))),",
                    m.konst, m.func
                );
            } else {
                let _ = writeln!(
                    w,
                    "                {} => Some(Err(__rt::Status::unimplemented(\"`{}` is a streaming method\"))),",
                    m.konst, m.name
                );
            }
        }
        let _ = writeln!(
            w,
            "                _ => None,\n            }}\n        }}\n"
        );

        // Method kinds.
        let _ = writeln!(
            w,
            "        fn method_kind(&self, path: &str) -> ::core::option::Option<__rt::MethodKind> {{\n            \
             match path {{"
        );
        for m in methods {
            let _ = writeln!(
                w,
                "                {} => Some(__rt::MethodKind::{}),",
                m.konst,
                m.kind.runtime_name()
            );
        }
        let _ = writeln!(w, "                _ => None,\n            }}\n        }}");

        // Paths this service does not serve at all.
        let _ = writeln!(
            w,
            "\n        fn is_unknown_method(&self, path: &str) -> bool {{\n            \
             self.method_kind(path).is_none()\n        }}"
        );

        if streaming {
            let streams = || methods.iter().filter(|m| m.kind != Kind::Unary);
            let _ = writeln!(
                w,
                "\n        fn on_message(&mut self, ctx: &mut __rt::CallContext<'_>, message: &[u8]) -> ::core::result::Result<(), __rt::Status> {{\n            \
                 match ctx.path {{"
            );
            for m in streams() {
                let _ = writeln!(
                    w,
                    "                {} => __rt::codec::message(message, |req| <S as self::{name}>::{}(&mut self.0, ctx, req)),",
                    m.konst, m.func
                );
            }
            let _ = writeln!(
                w,
                "                _ => Err(__rt::Status::unimplemented(\"unknown method\")),\n            }}\n        }}"
            );

            let _ = writeln!(
                w,
                "\n        fn on_half_close(&mut self, ctx: &mut __rt::CallContext<'_>) -> ::core::result::Result<(), __rt::Status> {{\n            \
                 match ctx.path {{"
            );
            for m in streams().filter(|m| m.kind == Kind::Bidi) {
                let _ = writeln!(
                    w,
                    "                {} => <S as self::{name}>::{}(&mut self.0, ctx),",
                    m.konst, m.end
                );
            }
            let _ = writeln!(
                w,
                "                _ => Ok(()),\n            }}\n        }}"
            );

            let _ = writeln!(
                w,
                "\n        fn poll_response(&mut self, ctx: &mut __rt::CallContext<'_>, cx: &mut __rt::__private::Context<'_>) -> __rt::__private::Poll<__rt::Next<__rt::__private::Vec<u8>>> {{\n            \
                 match ctx.path {{"
            );
            for m in streams() {
                let helper = if m.kind == Kind::Client {
                    "poll_single"
                } else {
                    "poll_stream"
                };
                let _ = writeln!(
                    w,
                    "                {} => __rt::codec::{helper}(<S as self::{name}>::{}(&mut self.0, ctx, cx)),",
                    m.konst, m.poll
                );
            }
            let _ = writeln!(
                w,
                "                _ => __rt::__private::Poll::Ready(__rt::Next::Done(Err(__rt::Status::unimplemented(\"unknown method\")))),\n            }}\n        }}"
            );

            let _ = writeln!(
                w,
                "\n        fn on_cancel(&mut self, ctx: &mut __rt::CallContext<'_>) {{\n            \
                 match ctx.path {{"
            );
            for m in streams() {
                let _ = writeln!(
                    w,
                    "                {} => <S as self::{name}>::{}(&mut self.0, ctx),",
                    m.konst, m.cancel
                );
            }
            let _ = writeln!(w, "                _ => {{}}\n            }}\n        }}");
        }
        let _ = writeln!(w, "    }}");
    }
}

/// One RPC of a service, with its generated names.
struct M {
    name: String,
    kind: Kind,
    konst: String,
    func: String,
    with_options: String,
    poll: String,
    end: String,
    cancel: String,
    req: String,
    resp: String,
}

impl M {
    /// Names of the trait methods generated for this RPC.
    fn trait_fns(&self) -> Vec<String> {
        match self.kind {
            Kind::Unary => vec![self.func.clone()],
            Kind::Server | Kind::Client => {
                vec![self.func.clone(), self.poll.clone(), self.cancel.clone()]
            }
            Kind::Bidi => vec![
                self.func.clone(),
                self.end.clone(),
                self.poll.clone(),
                self.cancel.clone(),
            ],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Unary,
    Server,
    Client,
    Bidi,
}

impl Kind {
    fn of(m: &MethodDescriptorProto) -> Self {
        match (m.client_streaming(), m.server_streaming()) {
            (false, false) => Self::Unary,
            (false, true) => Self::Server,
            (true, false) => Self::Client,
            (true, true) => Self::Bidi,
        }
    }

    fn runtime_name(self) -> &'static str {
        match self {
            Self::Unary => "Unary",
            Self::Server => "ServerStreaming",
            Self::Client => "ClientStreaming",
            Self::Bidi => "BidiStreaming",
        }
    }
}

/// Async or blocking client.
struct ClientFlavor {
    ty: &'static str,
    doc: &'static str,
    unary_bound: &'static str,
    stream_bound: &'static str,
    call_trait: &'static str,
    wrapper_prefix: &'static str,
    asyncness: &'static str,
    dot_await: &'static str,
}

impl ClientFlavor {
    const ASYNC: Self = Self {
        ty: "Client",
        doc: "Async",
        unary_bound: "UnaryTransport",
        stream_bound: "StreamingTransport",
        call_trait: "StreamingCall",
        wrapper_prefix: "",
        asyncness: "async ",
        dot_await: ".await",
    };
    const BLOCKING: Self = Self {
        ty: "BlockingClient",
        doc: "Blocking",
        unary_bound: "BlockingUnaryTransport",
        stream_bound: "BlockingStreamingTransport",
        call_trait: "BlockingStreamingCall",
        wrapper_prefix: "Blocking",
        asyncness: "",
        dot_await: "",
    };
}

fn gen_client(
    w: &mut String,
    full: &str,
    name: &str,
    methods: &[M],
    f: ClientFlavor,
    unary: bool,
    streaming: bool,
) {
    let ClientFlavor {
        ty,
        doc,
        unary_bound,
        stream_bound,
        call_trait,
        wrapper_prefix: p,
        asyncness,
        dot_await,
    } = f;
    let mut transports = Vec::new();
    if unary {
        transports.push(format!("`{unary_bound}` for unary methods"));
    }
    if streaming {
        transports.push(format!("`{stream_bound}` for streaming methods"));
    }
    let _ = writeln!(
        w,
        "\n    /// {doc} client for `{full}`.\n    \
         ///\n    \
         /// Requires {}.\n    \
         #[derive(Debug, Clone)]\n    \
         pub struct {name}{ty}<T> {{\n        transport: T,\n    }}\n\n    \
         impl<T> {name}{ty}<T> {{\n        \
         /// Create a client on top of `transport`.\n        \
         pub fn new(transport: T) -> Self {{\n            Self {{ transport }}\n        }}\n\n        \
         /// Mutable access to the transport.\n        \
         pub fn transport_mut(&mut self) -> &mut T {{\n            &mut self.transport\n        }}\n\n        \
         /// Unwrap the transport.\n        \
         pub fn into_inner(self) -> T {{\n            self.transport\n        }}\n    }}",
        if transports.is_empty() {
            "nothing".to_owned()
        } else {
            transports.join(" and ")
        }
    );
    if unary {
        let _ = writeln!(w, "\n    impl<T: __rt::{unary_bound}> {name}{ty}<T> {{");
        for m in methods.iter().filter(|m| m.kind == Kind::Unary) {
            let _ = writeln!(
                w,
                "        /// Call `{n}`.\n        \
                 pub {asyncness}fn {func}(&mut self, request: &{req}) -> ::core::result::Result<{resp}, __rt::Status> {{\n            \
                 self.{with}(request, __rt::CallOptions::default()){dot_await}.map(__rt::Response::into_message)\n        }}\n\n        \
                 /// Call `{n}` with per-call `options`, for example a timeout or request\n        \
                 /// metadata. The response carries the metadata that came with it.\n        \
                 pub {asyncness}fn {with}(&mut self, request: &{req}, options: __rt::CallOptions) -> ::core::result::Result<__rt::Response<{resp}>, __rt::Status> {{\n            \
                 let request = __rt::codec::encode(request)?;\n            \
                 let reply = __rt::{unary_bound}::unary(&mut self.transport, {konst}, &request, options){dot_await}?;\n            \
                 let message = __rt::codec::decode_response(&reply.message)?;\n            \
                 Ok(__rt::Response {{ message, headers: reply.headers, trailers: reply.trailers }})\n        }}",
                n = m.name,
                func = m.func,
                with = m.with_options,
                req = m.req,
                resp = m.resp,
                konst = m.konst,
            );
        }
        let _ = writeln!(w, "    }}");
    }
    if streaming {
        let call = format!("<T as __rt::{stream_bound}>::Call<'_>");
        let _ = writeln!(w, "\n    impl<T: __rt::{stream_bound}> {name}{ty}<T> {{");
        for m in methods.iter().filter(|m| m.kind != Kind::Unary) {
            let (n, func, req, resp, konst) = (&m.name, &m.func, &m.req, &m.resp, &m.konst);
            let with = &m.with_options;
            let start = format!(
                "__rt::{stream_bound}::start(&self.transport, {konst}, options){dot_await}?"
            );
            match m.kind {
                Kind::Server => {
                    let _ = writeln!(
                        w,
                        "        /// Call `{n}` (server streaming): send `request`, then read the\n        \
                         /// responses from the returned stream.\n        \
                         pub {asyncness}fn {func}(&self, request: &{req}) -> ::core::result::Result<__rt::codec::{p}ServerStreaming<{call}, {resp}>, __rt::Status> {{\n            \
                         self.{with}(request, __rt::CallOptions::default()){dot_await}\n        }}\n\n        \
                         /// Call `{n}` with per-call `options`, for example a timeout.\n        \
                         pub {asyncness}fn {with}(&self, request: &{req}, options: __rt::CallOptions) -> ::core::result::Result<__rt::codec::{p}ServerStreaming<{call}, {resp}>, __rt::Status> {{\n            \
                         let request = __rt::codec::encode(request)?;\n            \
                         let mut call = {start};\n            \
                         __rt::{call_trait}::send(&mut call, &request){dot_await}?;\n            \
                         __rt::{call_trait}::close_send(&mut call){dot_await}?;\n            \
                         Ok(__rt::codec::{p}ServerStreaming::new(call))\n        }}"
                    );
                }
                Kind::Client | Kind::Bidi => {
                    let (wrapper, doc) = if m.kind == Kind::Client {
                        (
                            "ClientStreaming",
                            "(client streaming): send requests on the returned\n        \
                             /// stream, then `finish` it (or `finish_with_metadata`) to get the\n        \
                             /// response.",
                        )
                    } else {
                        (
                            "BidiStreaming",
                            "(bidirectional streaming): send requests and read\n        \
                             /// responses on the returned stream.",
                        )
                    };
                    let _ = writeln!(
                        w,
                        "        /// Call `{n}` {doc}\n        \
                         pub {asyncness}fn {func}(&self) -> ::core::result::Result<__rt::codec::{p}{wrapper}<{call}, {req}, {resp}>, __rt::Status> {{\n            \
                         self.{with}(__rt::CallOptions::default()){dot_await}\n        }}\n\n        \
                         /// Call `{n}` with per-call `options`, for example a timeout.\n        \
                         pub {asyncness}fn {with}(&self, options: __rt::CallOptions) -> ::core::result::Result<__rt::codec::{p}{wrapper}<{call}, {req}, {resp}>, __rt::Status> {{\n            \
                         Ok(__rt::codec::{p}{wrapper}::new({start}))\n        }}"
                    );
                }
                Kind::Unary => {}
            }
        }
        let _ = writeln!(w, "    }}");
    }
}

/// Move proto3 `optional` synthetic oneofs after all real oneofs, as `protoc`
/// does and `descriptor.proto` requires. The pure-Rust parser declares them in
/// field order, which breaks consumers (like micropb-gen) that rely on the
/// documented layout.
fn normalize_synthetic_oneofs(msg: &mut protobuf::descriptor::DescriptorProto) {
    for nested in &mut msg.nested_type {
        normalize_synthetic_oneofs(nested);
    }
    let n = msg.oneof_decl.len();
    let synthetic: Vec<bool> = (0..n as i32)
        .map(|i| {
            let mut fields = msg.field.iter().filter(|f| f.oneof_index == Some(i));
            fields.next().is_some_and(|f| f.proto3_optional()) && fields.next().is_none()
        })
        .collect();
    let order: Vec<usize> = (0..n)
        .filter(|&i| !synthetic[i])
        .chain((0..n).filter(|&i| synthetic[i]))
        .collect();
    if order.iter().enumerate().all(|(new, &old)| new == old) {
        return;
    }
    let mut remap = vec![0i32; n];
    for (new, &old) in order.iter().enumerate() {
        remap[old] = new as i32;
    }
    let decls = std::mem::take(&mut msg.oneof_decl);
    let mut decls: Vec<Option<_>> = decls.into_iter().map(Some).collect();
    msg.oneof_decl = order
        .iter()
        .map(|&old| decls[old].take().expect("each oneof moved once"))
        .collect();
    for f in &mut msg.field {
        if let Some(i) = f.oneof_index {
            f.oneof_index = Some(remap[i as usize]);
        }
    }
}

/// micropb-gen's `resolve_path_elem`: package/message module name.
fn resolve_path_elem(elem: &str, suffixed: bool) -> String {
    let suffixed = suffixed || elem.starts_with(|c: char| c.is_uppercase());
    if suffixed || matches!(elem, "super" | "crate" | "self" | "Self" | "extern") {
        format!("{elem}_")
    } else {
        format!("r#{elem}")
    }
}

/// micropb-gen's `sanitized_ident`: type name.
fn sanitized_ident(name: &str) -> String {
    match name {
        "_" | "super" | "crate" | "self" | "Self" | "extern" => format!("_{name}"),
        n if n.starts_with(|c: char| c.is_numeric()) => format!("_{n}"),
        n if n.starts_with(|c: char| c.is_lowercase()) => format!("r#{n}"),
        n => n.to_owned(),
    }
}

fn quoted(names: &[String]) -> String {
    let mut out = String::new();
    for (i, name) in names.iter().enumerate() {
        if i > 0 {
            out.push_str(if i + 1 == names.len() { " and " } else { ", " });
        }
        let _ = write!(out, "`{name}`");
    }
    out
}

fn service_name(package: &str, svc: &ServiceDescriptorProto) -> String {
    if package.is_empty() {
        svc.name().to_owned()
    } else {
        format!("{package}.{}", svc.name())
    }
}

// Raw identifiers and their ordinary spelling occupy the same Rust namespace.
fn ident_key(ident: &str) -> &str {
    ident.strip_prefix("r#").unwrap_or(ident)
}

fn rust_ident(s: &str) -> String {
    const KW: &[&str] = &[
        "as", "async", "await", "break", "const", "continue", "dyn", "else", "enum", "fn", "for",
        "gen", "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub", "ref",
        "return", "static", "struct", "trait", "true", "false", "type", "unsafe", "use", "where",
        "while", "abstract", "become", "box", "do", "final", "macro", "override", "priv", "try",
        "typeof", "unsized", "virtual", "yield",
    ];
    if matches!(
        s,
        "self" | "super" | "crate" | "Self" | "extern" | "_" | "__rt"
    ) {
        format!("{s}_")
    } else if KW.contains(&s) {
        format!("r#{s}")
    } else {
        s.to_owned()
    }
}

fn snake(s: &str) -> String {
    let mut out = String::new();
    let chars: Vec<char> = s.chars().collect();
    for (i, &c) in chars.iter().enumerate() {
        if c.is_uppercase() {
            let prev_lower =
                i > 0 && (chars[i - 1].is_lowercase() || chars[i - 1].is_ascii_digit());
            let next_lower = chars.get(i + 1).is_some_and(|n| n.is_lowercase());
            if i > 0 && (prev_lower || (chars[i - 1].is_uppercase() && next_lower)) {
                out.push('_');
            }
            out.extend(c.to_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROTO: &str = r#"syntax = "proto3";
package a.b;
message Command { message Inner {} }
message Reply {}
message EventSubscribe {}
message Event {}
service Service {
  rpc Command(Command) returns (Reply);
  rpc Nested(Command.Inner) returns (Reply);
  rpc EventSubscribe(EventSubscribe) returns (stream Event);
  rpc Upload(stream Command) returns (Reply);
  rpc Chat(stream Command) returns (stream Reply);
}"#;

    fn generate(g: &Generator) -> String {
        static N: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("protolink_grpc_gen_{}_{n}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let proto = dir.join("t.proto");
        fs::write(&proto, PROTO).unwrap();
        let out = dir.join("grpc.rs");
        g.compile_protos(&[&proto], &out).unwrap();
        fs::read_to_string(out).unwrap()
    }

    /// Parse a single proto3 file given as source.
    fn parse(body: &str) -> FileDescriptorProto {
        static N: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "protolink_grpc_gen_parse_{}_{n}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        let proto = dir.join("p.proto");
        fs::write(&proto, format!("syntax = \"proto3\";\n{body}\n")).unwrap();
        let mut parser = protobuf_parse::Parser::new();
        parser.pure().include(&dir).input(&proto);
        let mut set = parser.file_descriptor_set().unwrap();
        set.file.pop().unwrap()
    }

    #[test]
    fn snake_case() {
        assert_eq!(snake("EventSubscribe"), "event_subscribe");
        assert_eq!(snake("HTTPServer"), "http_server");
        assert_eq!(snake("GetV2"), "get_v2");
    }

    #[test]
    fn micropb_naming() {
        let g = Generator::new();
        assert_eq!(
            g.type_path(".a.b.Command").unwrap(),
            "super::a_::b_::Command"
        );
        assert_eq!(
            g.type_path(".a.b.Command.Inner").unwrap(),
            "super::a_::b_::Command_::Inner"
        );
        assert_eq!(g.type_path(".Top").unwrap(), "super::Top");
        let mut g = Generator::new();
        g.suffixed_package_names(false)
            .messages_path("crate::proto");
        assert_eq!(
            g.type_path(".a.b.Msg").unwrap(),
            "crate::proto::r#a::r#b::Msg"
        );
        g.extern_type_path(".a.b.Msg", "crate::Custom");
        assert_eq!(g.type_path(".a.b.Msg").unwrap(), "crate::Custom");
        g.messages_path("pb");
        assert_eq!(g.type_path(".x.lower").unwrap(), "super::pb::r#x::r#lower");
    }

    #[test]
    fn service_named_like_the_runtime_alias_is_renamed() {
        assert_eq!(rust_ident("__rt"), "__rt_");
        let files = vec![parse(
            "package p; message M {} service __rt { rpc Get(M) returns (M); }",
        )];
        let src = Generator::new().generate(&files).unwrap();
        assert!(src.contains("pub trait __rt_ {"), "{src}");
        assert!(src.contains("use ::protolink::grpc as __rt;") || src.contains(" as __rt;"));
    }

    #[test]
    fn generates_service() {
        let src = generate(&Generator::new());
        assert!(src.contains("pub const METHOD_COMMAND: &str = \"/a.b.Service/Command\";"));
        assert!(src.contains("pub const METHODS: &[&str] = &[METHOD_COMMAND, METHOD_NESTED, METHOD_EVENT_SUBSCRIBE, METHOD_UPLOAD, METHOD_CHAT];"));
        // The call context, with the deadline, reaches the service method.
        assert!(src.contains(
            "__rt::codec::unary(request, |req| <S as self::Service>::command(&mut self.0, ctx, req))"
        ));
    }

    #[test]
    fn generates_streaming_service_methods() {
        let src = generate(&Generator::new());
        // Streaming methods default to UNIMPLEMENTED; unary ones are required.
        assert!(src.contains("Err(__rt::Status::unimplemented(\"`Upload` is not implemented\"))"));
        assert!(!src.contains("fn command(&mut self, ctx: &mut __rt::CallContext<'_>, request: super::a_::b_::Command) -> ::core::result::Result<super::a_::b_::Reply, __rt::Status> {"));
        // Only bidirectional calls have an end-of-requests hook.
        assert!(src.contains("fn end_chat("));
        assert!(!src.contains("fn end_upload("));
        assert!(!src.contains("fn end_event_subscribe("));
    }

    #[test]
    fn generates_streaming_routing() {
        let src = generate(&Generator::new());
        for line in [
            "METHOD_COMMAND => Some(__rt::MethodKind::Unary),",
            "METHOD_EVENT_SUBSCRIBE => Some(__rt::MethodKind::ServerStreaming),",
            "METHOD_UPLOAD => Some(__rt::MethodKind::ClientStreaming),",
            "METHOD_CHAT => Some(__rt::MethodKind::BidiStreaming),",
            "METHOD_EVENT_SUBSCRIBE => Some(Err(__rt::Status::unimplemented(\"`EventSubscribe` is a streaming method\"))),",
            "METHOD_UPLOAD => __rt::codec::message(message, |req| <S as self::Service>::upload(&mut self.0, ctx, req)),",
            "METHOD_CHAT => <S as self::Service>::end_chat(&mut self.0, ctx),",
            "METHOD_EVENT_SUBSCRIBE => __rt::codec::poll_stream(<S as self::Service>::poll_event_subscribe(&mut self.0, ctx, cx)),",
            "METHOD_UPLOAD => __rt::codec::poll_single(<S as self::Service>::poll_upload(&mut self.0, ctx, cx)),",
            "METHOD_CHAT => __rt::codec::poll_stream(<S as self::Service>::poll_chat(&mut self.0, ctx, cx)),",
            "METHOD_UPLOAD => <S as self::Service>::cancel_upload(&mut self.0, ctx),",
            "fn is_unknown_method(&self, path: &str) -> bool {\n            self.method_kind(path).is_none()",
        ] {
            assert!(src.contains(line), "missing `{line}`");
        }
        // Only bidi calls observe the half-close.
        assert!(!src.contains("::end_upload(&mut self.0"));
        // Unary methods are not routed to the streaming entry points.
        assert!(!src.contains("METHOD_COMMAND => __rt::codec::message"));
    }

    #[test]
    fn generates_streaming_clients() {
        let src = generate(&Generator::new());
        for line in [
            // Each shape returns its own wrapper type.
            "pub async fn event_subscribe(&self, request: &super::a_::b_::EventSubscribe) -> ::core::result::Result<__rt::codec::ServerStreaming<",
            "pub async fn upload(&self) -> ::core::result::Result<__rt::codec::ClientStreaming<",
            "pub async fn chat(&self) -> ::core::result::Result<__rt::codec::BidiStreaming<",
            "pub fn event_subscribe(&self, request: &super::a_::b_::EventSubscribe) -> ::core::result::Result<__rt::codec::BlockingServerStreaming<",
            "pub fn upload(&self) -> ::core::result::Result<__rt::codec::BlockingClientStreaming<",
            "pub fn chat(&self) -> ::core::result::Result<__rt::codec::BlockingBidiStreaming<",
            // A server-streaming request is sent and half-closed at once.
            "__rt::BlockingStreamingCall::close_send(&mut call)?;",
            "__rt::StreamingTransport::start(&self.transport, METHOD_CHAT, options).await?",
            "__rt::BlockingStreamingTransport::start(&self.transport, METHOD_CHAT, options)?",
            "self.chat_with_options(__rt::CallOptions::default()).await",
        ] {
            assert!(src.contains(line), "missing `{line}`");
        }
    }

    #[test]
    fn generates_unary_clients_with_options() {
        let src = generate(&Generator::new());
        for line in [
            "self.command_with_options(request, __rt::CallOptions::default()).await",
            "__rt::UnaryTransport::unary(&mut self.transport, METHOD_COMMAND, &request, options).await?",
            "__rt::BlockingUnaryTransport::unary(&mut self.transport, METHOD_COMMAND, &request, options)?",
            ".await.map(__rt::Response::into_message)",
            "Ok(__rt::Response { message, headers: reply.headers, trailers: reply.trailers })",
        ] {
            assert!(src.contains(line), "missing `{line}`");
        }
    }

    #[test]
    fn unary_only_service_has_no_streaming_items() {
        let mut g = Generator::new();
        g.client(false).blocking_client(false);
        let src = g
            .generate(&[parse(
                "package p; message M {} service S { rpc Get(M) returns (M); }",
            )])
            .unwrap();
        assert!(
            src.contains(
                "fn get(&mut self, ctx: &mut __rt::CallContext<'_>, request: super::p_::M) -> ::core::result::Result<super::p_::M, __rt::Status>;"
            )
        );
        assert!(src.contains("METHOD_GET => Some(__rt::MethodKind::Unary),"));
        assert!(src.contains("self.method_kind(path).is_none()"));
        assert!(!src.contains("fn on_message"));
        assert!(!src.contains("fn poll_response"));
    }

    #[test]
    fn rejects_generated_name_collisions() {
        let proto = "package p; message M {}\n\
                     service S {\n\
                       rpc Feed(stream M) returns (stream M);\n\
                       rpc PollFeed(M) returns (M);\n\
                     }";
        let err = Generator::new().generate(&[parse(proto)]).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("`Feed` and `PollFeed`") || msg.contains("`PollFeed` and `Feed`"),
            "{msg}"
        );
        assert!(msg.contains("`poll_feed`"), "{msg}");
    }

    #[test]
    fn rejects_with_options_collisions() {
        let proto = "package p; message M {}\n\
                     service S {\n\
                       rpc Get(M) returns (M);\n\
                       rpc GetWithOptions(M) returns (M);\n\
                     }";
        let err = Generator::new().generate(&[parse(proto)]).unwrap_err();
        assert!(err.to_string().contains("`get_with_options`"), "{err}");
    }

    #[test]
    fn rejects_client_builtins_only_when_clients_are_enabled() {
        for rpc in ["New", "IntoInner", "TransportMut"] {
            for shape in [(false, false), (false, true), (true, false), (true, true)] {
                let req = if shape.0 { "stream M" } else { "M" };
                let resp = if shape.1 { "stream M" } else { "M" };
                let file = parse(&format!(
                    "package p; message M {{}} service S {{ rpc {rpc}({req}) returns ({resp}); }}"
                ));
                for (asynchronous, blocking, server) in [
                    (true, false, true),
                    (false, true, true),
                    (true, true, true),
                    (true, false, false),
                    (false, true, false),
                ] {
                    let mut g = Generator::new();
                    g.client(asynchronous)
                        .blocking_client(blocking)
                        .server(server);
                    let err = g.generate(std::slice::from_ref(&file)).unwrap_err();
                    let msg = err.to_string();
                    assert!(msg.contains("service `p.S`"), "{msg}");
                    assert!(msg.contains(&format!("RPC `{rpc}`")), "{msg}");
                    assert!(msg.contains("reserved client method"), "{msg}");
                }
                let mut g = Generator::new();
                g.client(false).blocking_client(false);
                g.generate(&[file]).unwrap();
            }
        }
    }

    #[test]
    fn collisions_follow_enabled_rust_namespaces() {
        let file = parse(
            "message M {} service S { rpc Get(M) returns (M); rpc GetWithOptions(M) returns (M); }",
        );
        let mut server_only = Generator::new();
        server_only.client(false).blocking_client(false);
        server_only.generate(&[file]).unwrap();

        let file = parse(
            "message M {} service S { rpc Feed(stream M) returns (stream M); rpc PollFeed(M) returns (M); }",
        );
        let mut clients_only = Generator::new();
        clients_only.server(false);
        clients_only.generate(&[file]).unwrap();
    }

    #[test]
    fn rejects_escaped_rpc_collisions() {
        let file =
            parse("message M {} service S { rpc Self(M) returns (M); rpc Self_(M) returns (M); }");
        let err = Generator::new().generate(&[file]).unwrap_err();
        assert!(err.to_string().contains("`self_`"), "{err}");
    }

    #[test]
    fn rejects_module_collisions_after_normalization_and_qualification() {
        for (bodies, module) in [
            (
                vec!["package p; service Self {} service Self_ {}"],
                "p_self_",
            ),
            (
                vec!["package p; service HTTPServer {} service HttpServer {}"],
                "p_http_server",
            ),
            (
                vec!["package a.b; service S {}", "package a_b; service S {}"],
                "a_b_s",
            ),
            (
                vec!["package Foo; service S {}", "package foo; service S {}"],
                "foo_s",
            ),
            (
                vec![
                    "package a; service S {}",
                    "package b; service S {}",
                    "service A_S {}",
                ],
                "a_s",
            ),
            (vec!["service Self {} service Self_ {}"], "self_"),
        ] {
            let files: Vec<_> = bodies.into_iter().map(parse).collect();
            let err = Generator::new().generate(&files).unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("both generate a module"), "{msg}");
            assert!(msg.contains(&format!("`{module}`")), "{msg}");
        }
    }

    fn modules(g: &Generator, bodies: &[&str]) -> Result<Vec<String>, Error> {
        let files: Vec<_> = bodies.iter().map(|b| parse(b)).collect();
        let src = g.generate(&files)?;
        Ok(src
            .lines()
            .filter_map(|l| l.strip_prefix("pub mod "))
            .map(|l| l.trim_end_matches(" {").to_owned())
            .collect())
    }

    #[test]
    fn message_module_collisions_use_the_grpc_fallback() {
        // Default suffixing: `Foo_` is `foo_`, which is the package module.
        let g = Generator::new();
        assert_eq!(
            modules(&g, &["package foo; service Foo_ {}"]).unwrap(),
            ["foo__grpc"]
        );
        // Without suffixing, `Foo` is `foo`, which is `r#foo`.
        let mut g = Generator::new();
        g.suffixed_package_names(false);
        assert_eq!(
            modules(&g, &["package foo; service Foo {}"]).unwrap(),
            ["foo_grpc"]
        );
        // Noncolliding names are unchanged.
        assert_eq!(
            modules(&Generator::new(), &["package foo; service Foo {}"]).unwrap(),
            ["foo"]
        );
        // Root-level messages, enums and nested-type modules are reserved too.
        assert_eq!(
            modules(&g, &["message m {} service M {}"]).unwrap(),
            ["m_grpc"]
        );
        assert_eq!(
            modules(&g, &["enum e { A = 0; } service E {}"]).unwrap(),
            ["e_grpc"]
        );
        // Order of descriptors does not matter.
        let a = "package foo; service Foo {}";
        let b = "package bar; service Bar {}";
        let mut forward = modules(&g, &[a, b]).unwrap();
        let mut reverse = modules(&g, &[b, a]).unwrap();
        forward.sort();
        reverse.sort();
        assert_eq!(forward, reverse);
    }

    #[test]
    fn message_module_fallback_collisions_fail_explicitly() {
        let mut g = Generator::new();
        g.suffixed_package_names(false);
        // The fallback is itself a message module.
        let err = modules(
            &g,
            &[
                "package foo; service Foo {}",
                "package foo_grpc; message M {}",
            ],
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("`Foo`") || err.contains("`foo.Foo`"), "{err}");
        assert!(err.contains("`foo_grpc`"), "{err}");
        // The fallback is another service's module, in either order.
        for bodies in [
            ["package foo; service Foo {}", "service FooGrpc {}"],
            ["service FooGrpc {}", "package foo; service Foo {}"],
        ] {
            let err = modules(&g, &bodies).unwrap_err().to_string();
            assert!(err.contains("`FooGrpc` and `foo.Foo`"), "{err}");
            assert!(err.contains("`foo_grpc`"), "{err}");
        }
    }

    #[test]
    fn separate_message_module_does_not_rename_services() {
        let mut g = Generator::new();
        g.suffixed_package_names(false).messages_path("pb");
        assert_eq!(
            modules(&g, &["package foo; service Foo {}"]).unwrap(),
            ["foo"]
        );
        g.messages_path("self::pb");
        assert_eq!(
            modules(&g, &["package foo; service Foo {}"]).unwrap(),
            ["foo"]
        );
        g.messages_path("self::");
        assert_eq!(
            modules(&g, &["package foo; service Foo {}"]).unwrap(),
            ["foo_grpc"]
        );
    }

    #[test]
    fn cross_package_service_collisions_still_qualify_first() {
        let g = Generator::new();
        assert_eq!(
            modules(&g, &["package a; service S {}", "package b; service S {}"]).unwrap(),
            ["a_s", "b_s"]
        );
    }

    #[test]
    fn absolute_message_paths_are_exact() {
        let t = |path: &str| {
            let mut g = Generator::new();
            g.messages_path(path);
            g.type_path(".p.M").unwrap()
        };
        assert_eq!(t(""), "super::p_::M");
        assert_eq!(t("self::"), "super::p_::M");
        assert_eq!(t("crate"), "crate::p_::M");
        assert_eq!(t("crate::"), "crate::p_::M");
        assert_eq!(t("crate::proto"), "crate::proto::p_::M");
        assert_eq!(t("::ext::proto"), "::ext::proto::p_::M");
        assert_eq!(t("crate_messages"), "super::crate_messages::p_::M");
        assert_eq!(t("crates"), "super::crates::p_::M");
        assert_eq!(t("self::crate_messages"), "super::crate_messages::p_::M");
    }

    #[test]
    fn synthetic_oneofs_are_moved_last() {
        let dir =
            std::env::temp_dir().join(format!("protolink_grpc_gen_oneof_{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let proto = dir.join("o.proto");
        fs::write(
            &proto,
            "syntax = \"proto3\";\nmessage M { optional uint32 a = 1; oneof x { uint32 b = 2; uint32 c = 3; } }\n",
        )
        .unwrap();
        let mut g = Generator::new();
        g.file_descriptor_set_path(dir.join("o.bin"));
        g.compile_protos(&[&proto], dir.join("o.rs")).unwrap();
        let set =
            FileDescriptorSet::parse_from_bytes(&fs::read(dir.join("o.bin")).unwrap()).unwrap();
        let m = &set.file[0].message_type[0];
        let names: Vec<&str> = m.oneof_decl.iter().map(|o| o.name()).collect();
        assert_eq!(names, ["x", "_a"]);
        let idx: Vec<Option<i32>> = m.field.iter().map(|f| f.oneof_index).collect();
        assert_eq!(idx, [Some(1), Some(0), Some(0)]);
    }

    #[test]
    fn writes_fdset() {
        let dir =
            std::env::temp_dir().join(format!("protolink_grpc_gen_fd_{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let proto = dir.join("t.proto");
        fs::write(&proto, PROTO).unwrap();
        let mut g = Generator::new();
        g.file_descriptor_set_path(dir.join("fd.bin"));
        g.compile_protos(&[&proto], dir.join("a.rs")).unwrap();
        g.compile_fdset_file(dir.join("fd.bin"), dir.join("b.rs"))
            .unwrap();
        assert_eq!(
            fs::read_to_string(dir.join("a.rs")).unwrap(),
            fs::read_to_string(dir.join("b.rs")).unwrap()
        );
    }
}
