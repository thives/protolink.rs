//! # protolink-grpc-gen
//!
//! Generates gRPC service bindings for [protolink] from `.proto` service
//! definitions. Message types are produced by [`micropb-gen`]; this crate
//! produces the service glue that `tonic-build` would normally provide:
//!
//! - method path constants,
//! - a service trait with one method per unary RPC,
//! - a `<Service>Server<S>` wrapper implementing `protolink_grpc::Handler`,
//! - an async `<Service>Client<T: UnaryTransport>`,
//! - a `<Service>BlockingClient<T: BlockingUnaryTransport>`.
//!
//! Streaming RPCs are not supported: generated servers answer them with
//! `UNIMPLEMENTED` and generated clients do not expose them.
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

        // Module names: snake_case service name, package-qualified on clashes.
        let mut counts: BTreeMap<String, usize> = BTreeMap::new();
        for (_, svc) in &services {
            *counts.entry(snake(svc.name())).or_default() += 1;
        }

        let mut out = String::from("// @generated by protolink-grpc-gen. DO NOT EDIT.\n");
        for (package, svc) in services {
            let mut module = snake(svc.name());
            if counts[&module] > 1 && !package.is_empty() {
                module = format!("{}_{module}", package.replace('.', "_"));
            }
            self.gen_service(&mut out, package, svc, &module)?;
        }
        Ok(out)
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
        let mut path = if base.starts_with("crate") || base.starts_with("::") {
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
        let full = if package.is_empty() {
            svc.name().to_owned()
        } else {
            format!("{package}.{}", svc.name())
        };
        let name = svc.name();
        let rt = &self.runtime_path;

        struct M<'a> {
            desc: &'a MethodDescriptorProto,
            konst: String,
            func: String,
            req: String,
            resp: String,
        }
        let methods = svc
            .method
            .iter()
            .map(|m| {
                Ok(M {
                    desc: m,
                    konst: format!("METHOD_{}", snake(m.name()).to_uppercase()),
                    func: rust_fn_ident(&snake(m.name())),
                    req: self.type_path(m.input_type())?,
                    resp: self.type_path(m.output_type())?,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let unary = || {
            methods
                .iter()
                .filter(|m| !m.desc.client_streaming() && !m.desc.server_streaming())
        };

        let w = &mut *out;
        let _ = writeln!(w, "\n/// gRPC bindings for `{full}`.");
        let _ = writeln!(w, "#[allow(dead_code, unused_imports, clippy::all)]");
        let _ = writeln!(w, "pub mod {module} {{");
        let _ = writeln!(w, "    use {rt} as __rt;");
        let _ = writeln!(w, "    use __rt::Status;");
        let _ = writeln!(w, "    use __rt::__private::Vec;\n");
        let _ = writeln!(w, "    /// Fully-qualified service name.");
        let _ = writeln!(w, "    pub const SERVICE_NAME: &str = \"{full}\";");
        for m in &methods {
            let _ = writeln!(w, "    /// Path of `{}`.", m.desc.name());
            let _ = writeln!(
                w,
                "    pub const {}: &str = \"/{full}/{}\";",
                m.konst,
                m.desc.name()
            );
        }
        let list: Vec<&str> = methods.iter().map(|m| m.konst.as_str()).collect();
        let _ = writeln!(
            w,
            "    /// All method paths, including unsupported streaming methods."
        );
        let _ = writeln!(
            w,
            "    pub const METHODS: &[&str] = &[{}];",
            list.join(", ")
        );

        if self.server {
            let _ = writeln!(
                w,
                "\n    /// Server-side implementation of `{full}` (unary methods only)."
            );
            let _ = writeln!(w, "    pub trait {name} {{");
            for m in unary() {
                let _ = writeln!(w, "        /// Handle `{}`.", m.desc.name());
                let _ = writeln!(
                    w,
                    "        fn {}(&mut self, request: {}) -> Result<{}, Status>;",
                    m.func, m.req, m.resp
                );
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
                "    impl<S: {name}> __rt::Handler for {name}Server<S> {{"
            );
            let _ = writeln!(
                w,
                "        fn call(&mut self, path: &str, request: &[u8]) -> Option<Result<Vec<u8>, Status>> {{"
            );
            let _ = writeln!(w, "            match path {{");
            for m in &methods {
                let (cs, ss) = (m.desc.client_streaming(), m.desc.server_streaming());
                if !cs && !ss {
                    let _ = writeln!(
                        w,
                        "                {} => Some(__rt::codec::unary(request, |req| {name}::{}(&mut self.0, req))),",
                        m.konst, m.func
                    );
                } else {
                    let kind = match (cs, ss) {
                        (true, true) => "bidirectional",
                        (true, false) => "client",
                        _ => "server",
                    };
                    let _ = writeln!(
                        w,
                        "                {} => Some(Err(Status::unimplemented(\"{kind} streaming is not supported\"))),",
                        m.konst
                    );
                }
            }
            let _ = writeln!(
                w,
                "                _ => None,\n            }}\n        }}\n    }}"
            );
        }

        let client = |w: &mut String,
                      ty: &str,
                      bound: &str,
                      asyncness: &str,
                      dot_await: &str,
                      doc: &str| {
            let _ = writeln!(
                w,
                "\n    /// {doc} client for `{full}` (unary methods only).\n    \
                 #[derive(Debug, Clone)]\n    \
                 pub struct {name}{ty}<T> {{\n        transport: T,\n    }}\n\n    \
                 impl<T> {name}{ty}<T> {{\n        \
                 /// Create a client on top of `transport`.\n        \
                 pub fn new(transport: T) -> Self {{\n            Self {{ transport }}\n        }}\n\n        \
                 /// Mutable access to the transport.\n        \
                 pub fn transport_mut(&mut self) -> &mut T {{\n            &mut self.transport\n        }}\n\n        \
                 /// Unwrap the transport.\n        \
                 pub fn into_inner(self) -> T {{\n            self.transport\n        }}\n    }}\n"
            );
            let _ = writeln!(w, "    impl<T: __rt::{bound}> {name}{ty}<T> {{");
            for m in unary() {
                let _ = writeln!(
                    w,
                    "        /// Call `{}`.\n        \
                     pub {asyncness}fn {}(&mut self, request: &{}) -> Result<{}, Status> {{\n            \
                     let request = __rt::codec::encode(request)?;\n            \
                     let reply = __rt::{bound}::unary(&mut self.transport, {}, &request){dot_await}?;\n            \
                     __rt::codec::decode_response(&reply)\n        }}",
                    m.desc.name(),
                    m.func,
                    m.req,
                    m.resp,
                    m.konst
                );
            }
            let _ = writeln!(w, "    }}");
        };
        if self.client {
            client(w, "Client", "UnaryTransport", "async ", ".await", "Async");
        }
        if self.blocking_client {
            client(
                w,
                "BlockingClient",
                "BlockingUnaryTransport",
                "",
                "",
                "Blocking",
            );
        }
        let _ = writeln!(w, "}}");
        Ok(())
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

fn rust_fn_ident(s: &str) -> String {
    const KW: &[&str] = &[
        "as", "async", "await", "break", "const", "continue", "dyn", "else", "enum", "fn", "for",
        "gen", "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub", "ref",
        "return", "static", "struct", "trait", "true", "false", "type", "unsafe", "use", "where",
        "while", "abstract", "become", "box", "do", "final", "macro", "override", "priv", "try",
        "typeof", "unsized", "virtual", "yield",
    ];
    if matches!(s, "self" | "super" | "crate" | "Self" | "extern" | "_") {
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
}"#;

    fn generate(g: &Generator) -> String {
        let dir = std::env::temp_dir().join(format!("protolink_grpc_gen_{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let proto = dir.join("t.proto");
        fs::write(&proto, PROTO).unwrap();
        let out = dir.join("grpc.rs");
        g.compile_protos(&[&proto], &out).unwrap();
        fs::read_to_string(out).unwrap()
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
    fn generates_service() {
        let src = generate(&Generator::new());
        assert!(src.contains("pub const METHOD_COMMAND: &str = \"/a.b.Service/Command\";"));
        assert!(src.contains("fn command(&mut self, request: super::a_::b_::Command) -> Result<super::a_::b_::Reply, Status>;"));
        assert!(src.contains("request: super::a_::b_::Command_::Inner"));
        assert!(!src.contains("fn event_subscribe"));
        assert!(src.contains("METHOD_EVENT_SUBSCRIBE => Some(Err(Status::unimplemented(\"server streaming is not supported\")))"));
        assert!(src.contains("pub async fn command(&mut self, request: &super::a_::b_::Command)"));
        assert!(src.contains("pub struct ServiceBlockingClient<T>"));
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
