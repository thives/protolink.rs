//! Portable project bytecode for the reduced remote Lua VM: the artifact format, its bounded decoder and its verifier.
//!
//! Host tooling compiles remote functions into an [`Artifact`]. The agent and the DUT receive the encoded bytes,
//! decode them under [`Limits`], and run [`Artifact::verify`] before anything executes. Nothing here parses Lua source.
//! Stock Lua bytecode is not accepted.
//!
//! Stack discipline is proven statically for every fixed-effect instruction. A dynamic result count exists only in the
//! pattern `Call` with [`RESULTS_MULTI`] followed by `ReturnMulti`. The verifier checks that pattern. The VM checks the
//! dynamic values against `max_stack` at run time.
#![no_std]
#![forbid(unsafe_code)]
#![deny(missing_docs)]

extern crate alloc;

mod decode;
mod encode;
mod error;
mod format;
mod verify;

#[cfg(test)]
mod tests;

pub use error::{DecodeError, VerifyError};
pub use format::{
    ABI_VERSION, Artifact, ChildLink, Constant, DIGEST_LEN, Entry, FORMAT_VERSION, Instr, Limits,
    MAGIC, MAX_NATIVE_ARGS, MAX_NATIVE_RESULTS, NativeRef, PROFILE_VERSION, Proto, RESULTS_MULTI,
    UpvalueSource,
};

use alloc::vec::Vec;
use sha2::{Digest, Sha256};

/// SHA-256 of encoded artifact bytes. It identifies content only, not authority.
pub fn digest(bytes: &[u8]) -> [u8; DIGEST_LEN] {
    Sha256::digest(bytes).into()
}

/// The contract digest in the fixed header of encoded artifact bytes. It returns `None` when the bytes are shorter than the
/// header or do not start with the magic. It does not verify the artifact, so a caller must still decode and verify it.
pub fn contract_of(bytes: &[u8]) -> Option<[u8; DIGEST_LEN]> {
    let header = bytes.get(..format::HEADER_LEN)?;
    if header[..MAGIC.len()] != MAGIC {
        return None;
    }
    let mut contract = [0u8; DIGEST_LEN];
    contract.copy_from_slice(&header[format::HEADER_LEN - DIGEST_LEN..]);
    Some(contract)
}

/// Why a recipient refuses artifact bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdmitError {
    /// The bytes do not decode canonically under the recipient's limits, or do not verify.
    Invalid(DecodeError),
    /// The artifact was compiled for another contract.
    WrongContract,
    /// A native reference is not an operation of the recipient's contract, with this argument count and result count.
    /// Nothing ran: the artifact is not stored.
    NotInContract {
        /// Index of the first reference that is not in the table.
        native: usize,
    },
}

impl core::fmt::Display for AdmitError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Invalid(cause) => write!(f, "artifact is refused: {cause}"),
            Self::WrongContract => f.write_str("artifact was compiled for another contract"),
            Self::NotInContract { native } => write!(
                f,
                "native {native} is not an operation of this contract with the declared argument and result counts"
            ),
        }
    }
}

impl core::error::Error for AdmitError {}

/// The admission check a recipient runs on uploaded bytes. It has three parts, in order:
///
/// 1. Structure: canonical decoding and verification of the artifact and every nested child under `limits`. This needs
///    no contract, so it says nothing about what the code may call.
/// 2. Compatibility: the artifact names the recipient's `contract`, and every native reference of its root is exactly
///    one of `operations`, the recipient's own table of module, operation, argument count and result count. The
///    compiler builds that table from the same resolved contract, so a compiled artifact passes exactly when its
///    natives are all declared.
/// 3. Authorization is not decided here. A matching digest and a listed operation do not allow an effect. The
///    platform decides at run time, and it may refuse any operation.
///
/// Children are checked for structure and digest only. A node checks each child's contract digest against its route when
/// a `Start` names it, and the node that runs the child admits the child against its own table. Admission says nothing
/// about capacity.
pub fn admit(
    bytes: &[u8],
    limits: &Limits,
    contract: &[u8; DIGEST_LEN],
    operations: &[NativeRef],
) -> Result<Artifact, AdmitError> {
    let artifact = Artifact::decode_canonical(bytes, limits).map_err(AdmitError::Invalid)?;
    artifact
        .verify(limits)
        .map_err(|cause| AdmitError::Invalid(DecodeError::Verify(cause)))?;
    if artifact.contract != *contract {
        return Err(AdmitError::WrongContract);
    }
    if let Some(native) = artifact
        .natives
        .iter()
        .position(|reference| !operations.contains(reference))
    {
        return Err(AdmitError::NotInContract { native });
    }
    Ok(artifact)
}

impl Artifact {
    /// Encodes the artifact in canonical form. Equal artifacts produce equal bytes.
    pub fn encode(&self) -> Vec<u8> {
        encode::encode(self)
    }

    /// Decodes bytes under `limits`. Counts and lengths are checked before allocation. This does not verify the code.
    pub fn decode(bytes: &[u8], limits: &Limits) -> Result<Self, DecodeError> {
        decode::decode(bytes, limits, 0)
    }

    /// Decodes bytes and requires that they are canonical: encoding the decoded artifact must reproduce them exactly.
    /// A node admits uploads through this function, so one program has one digest.
    pub fn decode_canonical(bytes: &[u8], limits: &Limits) -> Result<Self, DecodeError> {
        let artifact = Self::decode(bytes, limits)?;
        // The encoder is compared with the bytes as it writes, so no second copy of the artifact is built.
        if !encode::encodes_to(&artifact, &|index| &artifact.children[index].bytes, bytes) {
            return Err(DecodeError::NotCanonical);
        }
        Ok(artifact)
    }

    /// Decodes and verifies bytes. A nested child is verified against its own bounds and its parent's digest.
    pub fn decode_verified(bytes: &[u8], limits: &Limits) -> Result<Self, DecodeError> {
        let artifact = Self::decode(bytes, limits)?;
        verify::verify(&artifact, limits, 0).map_err(DecodeError::Verify)?;
        Ok(artifact)
    }

    /// Verifies an artifact that was built in memory, for example by the host compiler or a node's `Program::verified`.
    ///
    /// It checks the structure: entries, proto layouts, indices, jump targets, closure order, the multi-result pattern, and
    /// stack heights along every path. It also checks the count, string, constant and depth bounds that decoding checks,
    /// with the same limits, and the canonical form and digest of each child. It does not check the native table
    /// (that is [`admit`]'s compatibility step), and it does not bound the encoded size of the root, which is checked when
    /// bytes are admitted.
    pub fn verify(&self, limits: &Limits) -> Result<(), VerifyError> {
        verify::verify(self, limits, 0)
    }
}
