//! `prost-build` frontend: adapts [`prost_build::ServiceGenerator`] callbacks
//! to the shared service emitter.
//!
//! prost resolves the request and response Rust types itself (package paths,
//! nested messages, identifier escaping, `extern_path`, well-known types), so
//! this frontend never reimplements those rules. It only re-roots the paths:
//! prost hands them out as seen from the package module, while the bindings are
//! emitted into a `<service>_grpc` submodule of it.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use crate::{Codec, Emitter, Error, Kind, M, Service, ident_key, rust_ident, snake};

/// Errors raised while generating, which prost's callback cannot return.
///
/// Each one is also emitted as a `compile_error!` into the generated module,
/// so using the generator directly (outside of
/// [`Generator::compile_protos_with_prost`](crate::Generator::compile_protos_with_prost))
/// never silently produces incomplete bindings.
#[derive(Debug, Clone, Default)]
pub struct ErrorLog(Arc<Mutex<Vec<String>>>);

impl ErrorLog {
    fn push(&self, message: String) {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(message);
    }

    /// Take the errors recorded so far.
    pub fn take(&self) -> Vec<String> {
        std::mem::take(&mut *self.0.lock().unwrap_or_else(|e| e.into_inner()))
    }
}

/// A [`prost_build::ServiceGenerator`] that emits protolink gRPC bindings using
/// the `Prost` codec.
///
/// The bindings of service `pkg.Name` go into module `name_grpc` of the
/// package module prost generates; rename it with
/// [`service_module`](Self::service_module). Create one with
/// [`Generator::prost_service_generator`](crate::Generator::prost_service_generator)
/// or [`new`](Self::new), register it with
/// `prost_build::Config::service_generator`.
#[derive(Debug, Clone)]
pub struct ProstServiceGenerator {
    emitter: Emitter,
    modules: HashMap<String, String>,
    /// Service modules taken so far: package, module -> service.
    used: BTreeMap<(String, String), String>,
    errors: ErrorLog,
}

impl Default for ProstServiceGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl ProstServiceGenerator {
    /// New generator with default settings (server and both clients enabled,
    /// runtime at `::protolink::grpc`).
    pub fn new() -> Self {
        Self::with_emitter(Emitter {
            runtime_path: "::protolink::grpc".into(),
            server: true,
            client: true,
            blocking_client: true,
            codec: Codec::Prost,
        })
    }

    pub(crate) fn with_emitter(emitter: Emitter) -> Self {
        Self {
            emitter,
            modules: HashMap::new(),
            used: BTreeMap::new(),
            errors: ErrorLog::default(),
        }
    }

    /// Path of the protolink gRPC runtime (default `::protolink::grpc`).
    pub fn runtime_path(&mut self, path: impl Into<String>) -> &mut Self {
        self.emitter.runtime_path = path.into();
        self
    }

    /// Generate service traits and server wrappers (default `true`).
    pub fn server(&mut self, flag: bool) -> &mut Self {
        self.emitter.server = flag;
        self
    }

    /// Generate async clients (default `true`).
    pub fn client(&mut self, flag: bool) -> &mut Self {
        self.emitter.client = flag;
        self
    }

    /// Generate blocking clients (default `true`).
    pub fn blocking_client(&mut self, flag: bool) -> &mut Self {
        self.emitter.blocking_client = flag;
        self
    }

    /// Use `module` instead of `<service>_grpc` for the fully-qualified
    /// protobuf service `service` (e.g. `pkg.Name`). Use it when the default
    /// collides with a message module of the same package.
    ///
    /// `module` must be one Rust module identifier, not a path: letters,
    /// digits and `_` (Unicode XID rules), not starting with a digit, and not
    /// `_` alone. Its spelling and case are kept as given. A Rust keyword such
    /// as `type` is emitted as `r#type`; `r#type` is accepted as is, and both
    /// spellings name the same module for collision detection. The names that
    /// cannot be raw identifiers (`self`, `super`, `crate`, `Self`, `extern`,
    /// `__rt`) gain a trailing underscore, while `r#self`, `r#super`,
    /// `r#crate` and `r#Self` are rejected.
    ///
    /// The override is validated when the service is generated. An invalid one
    /// is recorded in the [`error_log`](Self::error_log), emits only a
    /// `compile_error!`, and reserves no module name, so it can be corrected
    /// and generation retried. Overrides and other settings survive reuse of
    /// the generator across several `prost_build::Config` compilations.
    pub fn service_module(
        &mut self,
        service: impl AsRef<str>,
        module: impl Into<String>,
    ) -> &mut Self {
        self.modules.insert(
            service.as_ref().trim_start_matches('.').to_owned(),
            module.into(),
        );
        self
    }

    /// Handle to the errors this generator records; see [`ErrorLog`].
    pub fn error_log(&self) -> ErrorLog {
        self.errors.clone()
    }

    fn resolve(&mut self, service: &prost_build::Service) -> Result<Service, Error> {
        let full = if service.package.is_empty() {
            service.proto_name.clone()
        } else {
            format!("{}.{}", service.package, service.proto_name)
        };
        let module = match self.modules.get(&full) {
            Some(module) => override_ident(&full, module)?,
            None => rust_ident(&format!("{}_grpc", snake(&service.proto_name))),
        };
        // Services of one package share a module scope across files.
        let scope = (service.package.clone(), ident_key(&module).to_owned());
        if let Some(other) = self.used.insert(scope, full.clone()) {
            return Err(Error::Parse(format!(
                "services `{other}` and `{full}` both generate a module named `{}`; \
                 rename one with `service_module`",
                ident_key(&module)
            )));
        }
        let methods = service
            .methods
            .iter()
            .map(|m| {
                M::new(
                    &m.proto_name,
                    match (m.client_streaming, m.server_streaming) {
                        (false, false) => Kind::Unary,
                        (false, true) => Kind::Server,
                        (true, false) => Kind::Client,
                        (true, true) => Kind::Bidi,
                    },
                    reroot(&m.input_type),
                    reroot(&m.output_type),
                )
            })
            .collect();
        Ok(Service {
            full,
            name: rust_ident(&service.name),
            module,
            methods,
        })
    }
}

impl prost_build::ServiceGenerator for ProstServiceGenerator {
    fn generate(&mut self, service: prost_build::Service, buf: &mut String) {
        let result = self.resolve(&service).and_then(|resolved| {
            // Emit into a scratch buffer: a failure must not leave half a module.
            let mut out = String::new();
            self.emitter.gen_service(&mut out, &resolved)?;
            buf.push_str(&out);
            Ok(())
        });
        if let Err(e) = result {
            let message = format!("protolink-grpc-gen: {e}");
            buf.push_str(&format!("\ncompile_error!({message:?});\n"));
            self.errors.push(message);
        }
    }

    // Module names are scoped by package, and a package can span several
    // files, so `finalize` (called per file) would hide cross-file collisions.
    // Forget only this package's names so a reused generator starts the next
    // compilation clean; overrides, settings and recorded errors stay.
    fn finalize_package(&mut self, package: &str, _buf: &mut String) {
        self.used.retain(|(p, _), _| p != package);
    }
}

/// Validate a `service_module` override and spell it as a module identifier.
fn override_ident(service: &str, module: &str) -> Result<String, Error> {
    let invalid = |why: &str| {
        Error::Parse(format!(
            "invalid module override `{module}` for service `{service}`: {why}"
        ))
    };
    let (name, raw) = match module.strip_prefix("r#") {
        Some(name) => (name, true),
        None => (module, false),
    };
    let mut chars = name.chars();
    let valid = chars
        .next()
        .is_some_and(|c| c == '_' || unicode_ident::is_xid_start(c))
        && chars.all(unicode_ident::is_xid_continue);
    if !valid {
        return Err(invalid("expected a single Rust identifier"));
    }
    if name == "_" {
        return Err(invalid("`_` is not a module name"));
    }
    if !raw {
        return Ok(rust_ident(name));
    }
    if matches!(name, "self" | "super" | "crate" | "Self") {
        return Err(invalid("not a valid raw identifier"));
    }
    Ok(module.to_owned())
}

/// Re-root a type path from the package module (where prost resolved it) to a
/// submodule of it. Absolute paths and types that are not plain paths (such as
/// an `extern_path` to `Vec<u8>`) stay as they are.
fn reroot(ty: &str) -> String {
    let is_path = ty.starts_with(|c: char| c.is_alphabetic() || c == '_')
        && !ty.contains(['<', '(', '&', '[', ' ']);
    if !is_path || ty.starts_with("crate::") {
        ty.to_owned()
    } else {
        format!("super::{ty}")
    }
}

#[cfg(test)]
mod tests {
    use prost_build::{Comments, Method, Service, ServiceGenerator};

    use super::{ProstServiceGenerator, reroot};

    fn method(name: &str, input: &str, output: &str, client: bool, server: bool) -> Method {
        Method {
            name: name.to_lowercase(),
            proto_name: name.to_owned(),
            comments: Comments::default(),
            input_type: input.to_owned(),
            output_type: output.to_owned(),
            input_proto_type: String::new(),
            output_proto_type: String::new(),
            options: Default::default(),
            client_streaming: client,
            server_streaming: server,
        }
    }

    fn service(package: &str, name: &str) -> Service {
        Service {
            name: name.to_owned(),
            proto_name: name.to_owned(),
            package: package.to_owned(),
            comments: Comments::default(),
            methods: vec![
                method("GetThing", "Request", "inner::Reply", false, false),
                method(
                    "Watch",
                    "super::other::Request",
                    "::prost_types::Timestamp",
                    false,
                    true,
                ),
                method("Upload", "crate::ext::Request", "Vec<u8>", true, false),
                method("Chat", "Request", "Request", true, true),
            ],
            options: Default::default(),
        }
    }

    fn generate(g: &mut ProstServiceGenerator, service: Service) -> String {
        let mut buf = String::new();
        g.generate(service, &mut buf);
        buf
    }

    #[test]
    fn uses_prost_types_the_prost_codec_and_wire_names() {
        let src = generate(&mut ProstServiceGenerator::new(), service("a.b", "Thing"));
        for expected in [
            "pub mod thing_grpc {",
            "pub trait Thing {",
            "pub const SERVICE_NAME: &::core::primitive::str = \"a.b.Thing\";",
            "\"/a.b.Thing/GetThing\"",
            "request: super::Request) -> ::core::result::Result<super::inner::Reply,",
            "super::super::other::Request",
            "::prost_types::Timestamp",
            "crate::ext::Request",
            "Vec<u8>",
            "__rt::codec::unary::<__rt::codec::Prost, _, _>(",
            "__rt::codec::BidiStreaming<<T as __rt::StreamingTransport>::Call<'_>, super::Request, super::Request, __rt::codec::Prost>",
        ] {
            assert!(src.contains(expected), "missing `{expected}`:\n{src}");
        }
        assert!(!src.contains("Micropb"), "{src}");
        assert!(!src.contains("super::Vec"), "{src}");
    }

    #[test]
    fn generation_modes_apply() {
        let mut g = ProstServiceGenerator::new();
        g.server(false)
            .client(false)
            .runtime_path("::protolink_grpc");
        let src = generate(&mut g, service("", "Thing"));
        assert!(src.contains("use ::protolink_grpc as __rt;"), "{src}");
        assert!(src.contains("\"/Thing/GetThing\""), "{src}");
        assert!(!src.contains("pub trait Thing"), "{src}");
        assert!(!src.contains("ThingClient"), "{src}");
        assert!(src.contains("ThingBlockingClient"), "{src}");
    }

    #[test]
    fn service_module_collisions_are_reported_per_package() {
        let mut g = ProstServiceGenerator::new();
        let errors = g.error_log();
        generate(&mut g, service("a", "Thing"));
        // Another package has its own module scope.
        let other = generate(&mut g, service("b", "Thing"));
        assert!(!other.contains("compile_error"), "{other}");
        assert!(errors.take().is_empty());

        // `Thing` and `THING` both want `thing_grpc` in package `a`.
        let clash = generate(&mut g, service("a", "THING"));
        let errors = errors.take();
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(errors[0].contains("a.Thing") && errors[0].contains("a.THING"));
        assert!(clash.contains("compile_error!"), "{clash}");
        assert!(
            !clash.contains("pub mod"),
            "half a module was emitted:\n{clash}"
        );
    }

    #[test]
    fn module_override_resolves_collisions() {
        let mut g = ProstServiceGenerator::new();
        g.service_module(".a.THING", "shouting");
        let errors = g.error_log();
        generate(&mut g, service("a", "Thing"));
        let src = generate(&mut g, service("a", "THING"));
        assert!(src.contains("pub mod shouting {"), "{src}");
        assert!(errors.take().is_empty());
    }

    #[test]
    fn rpc_name_collisions_are_reported() {
        let mut svc = service("a", "Thing");
        svc.methods
            .push(method("get_thing", "Request", "Request", false, false));
        let mut g = ProstServiceGenerator::new();
        let errors = g.error_log();
        let src = generate(&mut g, svc);
        assert!(src.contains("compile_error!"), "{src}");
        assert!(errors.take()[0].contains("GetThing"));
    }

    #[test]
    fn reroots_relative_paths_only() {
        assert_eq!(reroot("Foo"), "super::Foo");
        assert_eq!(reroot("foo::Bar"), "super::foo::Bar");
        assert_eq!(reroot("super::other::X"), "super::super::other::X");
        assert_eq!(reroot("r#type::Msg"), "super::r#type::Msg");
        assert_eq!(
            reroot("::prost_types::Timestamp"),
            "::prost_types::Timestamp"
        );
        assert_eq!(reroot("crate::x::Y"), "crate::x::Y");
        assert_eq!(reroot("Vec<u8>"), "Vec<u8>");
    }

    #[test]
    fn finalize_package_allows_regenerating() {
        for package in ["a", ""] {
            let mut g = ProstServiceGenerator::new();
            let errors = g.error_log();
            g.service_module(format!("{package}.Other"), "custom");
            generate(&mut g, service(package, "Thing"));
            g.finalize_package(package, &mut String::new());
            let again = generate(&mut g, service(package, "Thing"));
            assert!(again.contains("pub mod thing_grpc {"), "{again}");
            assert!(errors.take().is_empty());
        }
    }

    #[test]
    fn finalize_does_not_hide_collisions() {
        let mut g = ProstServiceGenerator::new();
        let errors = g.error_log();
        generate(&mut g, service("a", "Thing"));
        g.finalize(&mut String::new());
        generate(&mut g, service("a", "THING"));
        assert_eq!(errors.take().len(), 1);
    }

    #[test]
    fn finalize_package_is_scoped_to_its_package() {
        let mut g = ProstServiceGenerator::new();
        let errors = g.error_log();
        generate(&mut g, service("a", "Thing"));
        generate(&mut g, service("b", "Thing"));
        g.finalize_package("a", &mut String::new());
        generate(&mut g, service("b", "THING"));
        let errors = errors.take();
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(errors[0].contains("b.Thing") && errors[0].contains("b.THING"));
        generate(&mut g, service("a", "THING"));
        assert!(errors_empty(&g));
    }

    fn errors_empty(g: &ProstServiceGenerator) -> bool {
        g.error_log().take().is_empty()
    }

    #[test]
    fn finalize_package_keeps_errors_and_overrides() {
        let mut g = ProstServiceGenerator::new();
        g.service_module("a.Thing", "custom").server(false);
        let errors = g.error_log();
        generate(&mut g, service("a", "Thing"));
        generate(&mut g, service("a", "Other"));
        generate(&mut g, service("a", "OTHER"));
        g.finalize_package("a", &mut String::new());
        assert_eq!(errors.take().len(), 1);
        generate(&mut g, service("a", "OTHER"));
        let src = generate(&mut g, service("a", "Thing"));
        assert!(src.contains("pub mod custom {"), "{src}");
        assert!(!src.contains("pub trait Thing"), "{src}");
    }

    fn override_result(module: &str) -> (String, Vec<String>) {
        let mut g = ProstServiceGenerator::new();
        g.service_module("a.Thing", module);
        let src = generate(&mut g, service("a", "Thing"));
        (src, g.error_log().take())
    }

    #[test]
    fn valid_overrides_are_normalized() {
        for (input, expected) in [
            ("custom_name", "custom_name"),
            ("type", "r#type"),
            ("r#type", "r#type"),
            ("r#foo", "r#foo"),
            ("MyModule", "MyModule"),
            ("_private", "_private"),
            ("名前", "名前"),
            ("self", "self_"),
            ("crate", "crate_"),
            ("Self", "Self_"),
        ] {
            let (src, errors) = override_result(input);
            assert!(errors.is_empty(), "{input}: {errors:?}");
            assert!(
                src.contains(&format!("pub mod {expected} {{")),
                "{input}:\n{src}"
            );
        }
    }

    #[test]
    fn invalid_overrides_are_rejected_once_without_a_module() {
        for input in [
            "", "_", "r#", "r#_", "1name", "foo-bar", "foo::bar", " foo", "foo ", "r#1a", "r#r#a",
            "r#self", "r#super", "r#crate", "r#Self",
        ] {
            let (src, errors) = override_result(input);
            assert_eq!(errors.len(), 1, "{input:?}: {errors:?}");
            assert!(
                errors[0].contains("a.Thing") && errors[0].contains(&format!("`{input}`")),
                "{input:?}: {errors:?}"
            );
            assert!(src.contains("compile_error!"), "{input:?}: {src}");
            assert!(!src.contains("pub mod"), "{input:?}: {src}");
        }
    }

    #[test]
    fn raw_and_plain_overrides_share_a_namespace() {
        let mut g = ProstServiceGenerator::new();
        g.service_module("a.One", "type")
            .service_module("a.Two", "r#type");
        let errors = g.error_log();
        generate(&mut g, service("a", "One"));
        generate(&mut g, service("a", "Two"));
        let errors = errors.take();
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(errors[0].contains("a.One") && errors[0].contains("a.Two"));
    }

    #[test]
    fn override_collides_with_default_module_of_another_service() {
        let mut g = ProstServiceGenerator::new();
        g.service_module("a.One", "two_grpc");
        let errors = g.error_log();
        generate(&mut g, service("a", "One"));
        generate(&mut g, service("a", "Two"));
        assert_eq!(errors.take().len(), 1);
    }

    #[test]
    fn corrected_override_does_not_leave_a_stale_reservation() {
        let mut g = ProstServiceGenerator::new();
        let errors = g.error_log();
        g.service_module("a.Thing", "foo-bar");
        let bad = generate(&mut g, service("a", "Thing"));
        assert!(bad.contains("compile_error!"));
        assert_eq!(errors.take().len(), 1);
        // The default name is still free for another service...
        let other = generate(&mut g, service("a", "Thing2"));
        assert!(!other.contains("compile_error!"), "{other}");
        // ...and the corrected override takes the original service.
        g.service_module("a.Thing", "thing_ok");
        let ok = generate(&mut g, service("a", "Thing"));
        assert!(ok.contains("pub mod thing_ok {"), "{ok}");
        assert!(errors.take().is_empty());
    }

    mod prost_lifecycle {
        use std::path::{Path, PathBuf};
        use std::sync::atomic::{AtomicUsize, Ordering};

        use prost_types::{
            DescriptorProto, FileDescriptorProto, FileDescriptorSet, MethodDescriptorProto,
            ServiceDescriptorProto,
        };

        use super::ProstServiceGenerator;

        struct TempDir(PathBuf);

        impl TempDir {
            fn new() -> Self {
                static N: AtomicUsize = AtomicUsize::new(0);
                let dir = std::env::temp_dir().join(format!(
                    "protolink-grpc-gen-{}-{}",
                    std::process::id(),
                    N.fetch_add(1, Ordering::Relaxed)
                ));
                std::fs::create_dir_all(&dir).unwrap();
                Self(dir)
            }
            fn path(&self) -> &Path {
                &self.0
            }
        }

        impl Drop for TempDir {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }

        fn file(name: &str, package: &str, services: &[&str]) -> FileDescriptorProto {
            FileDescriptorProto {
                name: Some(name.into()),
                package: Some(package.into()),
                syntax: Some("proto3".into()),
                message_type: vec![DescriptorProto {
                    name: Some("Msg".into()),
                    ..Default::default()
                }],
                service: services
                    .iter()
                    .map(|s| ServiceDescriptorProto {
                        name: Some((*s).into()),
                        method: vec![MethodDescriptorProto {
                            name: Some("Call".into()),
                            input_type: Some(format!(".{package}.Msg")),
                            output_type: Some(format!(".{package}.Msg")),
                            ..Default::default()
                        }],
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }
        }

        fn compile(
            config: &mut prost_build::Config,
            dir: &TempDir,
            files: Vec<FileDescriptorProto>,
        ) -> String {
            config.out_dir(dir.path());
            config
                .compile_fds(FileDescriptorSet { file: files })
                .unwrap();
            std::fs::read_dir(dir.path())
                .unwrap()
                .map(|e| std::fs::read_to_string(e.unwrap().path()).unwrap())
                .collect()
        }

        #[test]
        fn config_and_generator_can_be_reused() {
            let generator = ProstServiceGenerator::new();
            let errors = generator.error_log();
            let mut config = prost_build::Config::new();
            config.service_generator(Box::new(generator));
            for _ in 0..2 {
                let dir = TempDir::new();
                let out = compile(&mut config, &dir, vec![file("a.proto", "a", &["Thing"])]);
                assert!(out.contains("pub mod thing_grpc {"), "{out}");
                assert!(!out.contains("compile_error!"), "{out}");
                assert!(errors.take().is_empty());
            }
        }

        #[test]
        fn collisions_across_files_of_one_package_are_reported() {
            let generator = ProstServiceGenerator::new();
            let errors = generator.error_log();
            let mut config = prost_build::Config::new();
            config.service_generator(Box::new(generator));
            let dir = TempDir::new();
            let out = compile(
                &mut config,
                &dir,
                vec![
                    file("one.proto", "a", &["Thing"]),
                    file("two.proto", "a", &["THING"]),
                ],
            );
            assert!(out.contains("compile_error!"), "{out}");
            assert_eq!(errors.take().len(), 1);
        }
    }
}
