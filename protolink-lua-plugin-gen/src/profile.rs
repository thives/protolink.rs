//! Language profiles: the consumer's versioned, machine-readable list of supported remote features and its native ABI.

use std::collections::BTreeSet;
use std::path::Path;

use sha2::{Digest, Sha256};

use crate::diag::{Code, Diagnostic};
use crate::schema::{RawProfile, SCHEMA_VERSION};

/// Domain separator of a profile's semantic fingerprint.
const FINGERPRINT_DOMAIN: &[u8] = b"protolink-lua-plugin-gen/profile";

/// What a consumer's VM can carry back from a native operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeAbi {
    /// ABI name, such as `single-value-v1`.
    pub name: String,
    /// Most results one native operation may declare.
    pub max_results: u32,
}

/// A parsed language profile.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Profile {
    /// Profile name, such as `lua54-bounded-v1`.
    pub name: String,
    /// Profile version.
    pub version: String,
    /// Features the profile allows in remote regions.
    pub allowed: BTreeSet<String>,
    /// The consumer's native ABI, when it restricts native signatures.
    pub native_abi: Option<NativeAbi>,
    /// SHA-256 of the profile file bytes. Provenance only: comments and formatting change it.
    pub sha256: [u8; 32],
    /// SHA-256 over the profile's semantics: name, version, the sorted feature set and the native ABI. Formatting and
    /// comments do not change it; any policy change does. It is part of every contract digest that selects the profile.
    pub fingerprint: [u8; 32],
}

impl Profile {
    /// Parses a profile from its file contents.
    pub fn parse(text: &str, bytes: &[u8], file: &Path) -> Result<Self, Diagnostic> {
        let raw: RawProfile =
            toml::from_str(text).map_err(|e| Diagnostic::new(Code::Parse, file, e.to_string()))?;
        if raw.schema_version != SCHEMA_VERSION {
            return Err(Diagnostic::new(
                Code::UnsupportedSchema,
                file,
                format!(
                    "profile schema version {} is not supported",
                    raw.schema_version
                ),
            ));
        }
        let allowed: BTreeSet<String> = raw.features.allowed.into_iter().collect();
        let native_abi = raw.native_abi.map(|abi| NativeAbi {
            name: abi.name,
            max_results: abi.max_results,
        });
        let fingerprint = fingerprint(&raw.name, &raw.version, &allowed, native_abi.as_ref());
        Ok(Self {
            name: raw.name,
            version: raw.version,
            allowed,
            native_abi,
            sha256: Sha256::digest(bytes).into(),
            fingerprint,
        })
    }

    /// True when the profile allows `feature` in remote regions.
    pub fn allows(&self, feature: &str) -> bool {
        self.allowed.contains(feature)
    }
}

fn fingerprint(
    name: &str,
    version: &str,
    allowed: &BTreeSet<String>,
    native_abi: Option<&NativeAbi>,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    let mut put = |bytes: &[u8]| {
        hasher.update((bytes.len() as u32).to_le_bytes());
        hasher.update(bytes);
    };
    put(FINGERPRINT_DOMAIN);
    put(name.as_bytes());
    put(version.as_bytes());
    put(&(allowed.len() as u32).to_le_bytes());
    for feature in allowed {
        put(feature.as_bytes());
    }
    match native_abi {
        None => put(&[0]),
        Some(abi) => {
            put(&[1]);
            put(abi.name.as_bytes());
            put(&abi.max_results.to_le_bytes());
        }
    }
    hasher.finalize().into()
}
