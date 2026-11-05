//! Command-line front end of `protolink-lua-plugin-gen`.
//!
//! ```text
//! protolink-lua-plugin-gen resolve  --root DIR --profiles DIR [--lock FILE [--write-lock]] MANIFEST...
//! protolink-lua-plugin-gen generate --root DIR --profiles DIR [--lock FILE] --out DIR MANIFEST...
//! ```
//!
//! Each MANIFEST is a root. The lock covers every contract that any root reaches. `resolve` reports diagnostics, and
//! writes the lock file when asked. `generate` writes one `.rs` and one `.lua` per contract, named by name and id. With `--lock`, a stale
//! lock is an error and nothing is written.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use protolink_lua_plugin_gen::{Diagnostic, Lock, Resolver, lua_definitions, rust_bindings};

#[derive(Default)]
struct Args {
    command: String,
    root: Option<PathBuf>,
    profiles: Option<PathBuf>,
    lock: Option<PathBuf>,
    write_lock: bool,
    out: Option<PathBuf>,
    manifests: Vec<PathBuf>,
}

fn parse(mut argv: Vec<String>) -> Result<Args, String> {
    if argv.is_empty() {
        return Err(
            "usage: protolink-lua-plugin-gen <resolve|generate> [options] MANIFEST".to_string(),
        );
    }
    let mut args = Args {
        command: argv.remove(0),
        ..Args::default()
    };
    let mut iter = argv.into_iter();
    while let Some(flag) = iter.next() {
        let mut value = |name: &str| iter.next().ok_or_else(|| format!("{name} needs a value"));
        match flag.as_str() {
            "--root" => args.root = Some(PathBuf::from(value("--root")?)),
            "--profiles" => args.profiles = Some(PathBuf::from(value("--profiles")?)),
            "--lock" => args.lock = Some(PathBuf::from(value("--lock")?)),
            "--out" => args.out = Some(PathBuf::from(value("--out")?)),
            "--write-lock" => args.write_lock = true,
            other if other.starts_with("--") => return Err(format!("unknown option {other}")),
            _ => args.manifests.push(PathBuf::from(flag)),
        }
    }
    Ok(args)
}

fn report(diagnostics: &[Diagnostic]) -> ExitCode {
    for diagnostic in diagnostics {
        eprintln!("{diagnostic}");
    }
    ExitCode::FAILURE
}

fn run(args: Args) -> Result<(), ExitCode> {
    let (Some(root), Some(profiles)) = (args.root.as_ref(), args.profiles.as_ref()) else {
        eprintln!("--root and --profiles are required");
        return Err(ExitCode::FAILURE);
    };
    if args.manifests.is_empty() {
        eprintln!("at least one MANIFEST is required");
        return Err(ExitCode::FAILURE);
    }
    let resolver = Resolver::new(root, profiles).map_err(|d| report(&[d]))?;
    let graphs = args
        .manifests
        .iter()
        .map(|manifest| resolver.resolve(manifest).map_err(|d| report(&d)))
        .collect::<Result<Vec<_>, _>>()?;

    if let Some(lock_path) = args.lock.as_deref().filter(|_| !args.write_lock) {
        let text = fs::read_to_string(lock_path).map_err(|e| {
            eprintln!("cannot read lock {}: {e}", lock_path.display());
            ExitCode::FAILURE
        })?;
        let lock = Lock::parse(&text, lock_path).map_err(|d| report(&[d]))?;
        lock.verify(&graphs, lock_path).map_err(|d| report(&d))?;
    }
    if args.write_lock {
        let Some(lock_path) = args.lock.as_deref() else {
            eprintln!("--write-lock needs --lock FILE");
            return Err(ExitCode::FAILURE);
        };
        let lock = Lock::from_graphs(&graphs, lock_path).map_err(|d| report(&d))?;
        fs::write(lock_path, lock.to_toml()).map_err(|e| {
            eprintln!("cannot write lock {}: {e}", lock_path.display());
            ExitCode::FAILURE
        })?;
    }
    if args.command == "generate" {
        let Some(out) = args.out.as_deref() else {
            eprintln!("generate needs --out DIR");
            return Err(ExitCode::FAILURE);
        };
        write_outputs(&graphs, out)?;
    } else if args.command != "resolve" {
        eprintln!("unknown command {}", args.command);
        return Err(ExitCode::FAILURE);
    }
    Ok(())
}

fn write_outputs(graphs: &[protolink_lua_plugin_gen::Graph], out: &Path) -> Result<(), ExitCode> {
    fs::create_dir_all(out).map_err(|e| {
        eprintln!("cannot create {}: {e}", out.display());
        ExitCode::FAILURE
    })?;
    for contract in graphs.iter().flat_map(|graph| graph.contracts()) {
        let slug = |text: &str| -> String {
            text.chars()
                .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
                .collect()
        };
        let stem = format!("{}_{}", slug(&contract.name), slug(&contract.id));
        let rust_path = out.join(format!("{stem}.rs"));
        let lua_path = out.join(format!("{stem}.lua"));
        fs::write(&rust_path, rust_bindings(contract)).map_err(|e| {
            eprintln!("cannot write {}: {e}", rust_path.display());
            ExitCode::FAILURE
        })?;
        fs::write(&lua_path, lua_definitions(contract)).map_err(|e| {
            eprintln!("cannot write {}: {e}", lua_path.display());
            ExitCode::FAILURE
        })?;
    }
    Ok(())
}

fn main() -> ExitCode {
    let args = match parse(env::args().skip(1).collect()) {
        Ok(args) => args,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::FAILURE;
        }
    };
    match run(args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(code) => code,
    }
}
