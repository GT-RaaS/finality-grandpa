// Copyright 2026 Parity Technologies (UK) Ltd
// SPDX-License-Identifier: Apache-2.0

//! A consensus history containing both blocks and skipped views.
//!
//! Instantiate GRANDPA with [`TargetId`] as its hash and [`View`] as its number.
//! Every consensus edge advances the view exactly once, including a
//! [`TargetKind::ViewTimeout`]; only a [`TargetKind::Block`] advances the real
//! block number. This preserves the consecutive-height assumption of GRANDPA's
//! vote graph without assigning artificial heights to real blocks.
//!
//! A timeout is a competing child of its parent, not a notification that a
//! timer elapsed. It becomes irreversible only when finalized by GRANDPA.
//! The application supplies target storage and ancestry through [`crate::Chain`],
//! and handles real-block finalization through its environment.

use blake2::{digest::consts::U32, Blake2b, Digest};
use parity_scale_codec::{Decode, DecodeWithMemTracking, Encode};

/// A consensus position, independent of both GRANDPA rounds and block numbers.
pub type View = u64;

/// Domain separator for version 1 of the consensus-target identity encoding.
///
/// Target IDs hash this prefix, the 32-byte chain domain and the SCALE-encoded
/// [`ConsensusTarget`], in that order, using Blake2b with a 256-bit output.
pub const TARGET_ID_DOMAIN: &[u8] = b"finality-grandpa/view-target/v1";

/// The identity of a consensus target, distinct from a real block hash.
#[derive(
	Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode, DecodeWithMemTracking,
)]
#[cfg_attr(feature = "derive-codec", derive(scale_info::TypeInfo))]
pub struct TargetId(pub [u8; 32]);

/// A real block referenced by a consensus target.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Encode, Decode, DecodeWithMemTracking)]
#[cfg_attr(feature = "derive-codec", derive(scale_info::TypeInfo))]
pub struct BlockRef<H, N> {
	/// The real block hash.
	pub hash: H,
	/// The real block number, which does not increase during timeout views.
	pub number: N,
}

/// The event represented by a consensus target.
#[derive(Clone, Debug, PartialEq, Eq, Encode, Decode, DecodeWithMemTracking)]
#[cfg_attr(feature = "derive-codec", derive(scale_info::TypeInfo))]
pub enum TargetKind<H, N> {
	/// A real block extending the parent's latest real block.
	#[codec(index = 0)]
	Block(BlockRef<H, N>),
	/// No block was selected for this view; inherit the parent's latest block.
	#[codec(index = 1)]
	ViewTimeout,
}

/// A block or timeout with a unique position in the consensus tree.
///
/// Decoding does not validate ancestry, height or application block validity.
/// The application must validate targets and make their ancestry available
/// through [`crate::Chain`] before delivering votes. It must also authenticate
/// the binding between a block and this consensus parent, and validate the
/// block's actual parent hash.
#[derive(Clone, Debug, PartialEq, Eq, Encode, Decode, DecodeWithMemTracking)]
#[cfg_attr(feature = "derive-codec", derive(scale_info::TypeInfo))]
pub struct ConsensusTarget<H, N> {
	/// The consensus parent. Only the trusted root checkpoint has no parent.
	pub parent: Option<TargetId>,
	/// Exactly the parent's view plus one, except at the trusted root.
	pub view: View,
	/// The block or timeout selected at this position.
	pub kind: TargetKind<H, N>,
}

impl<H: Encode, N: Encode> ConsensusTarget<H, N> {
	/// Compute the deterministic, chain-specific identity of this target.
	///
	/// All participants must use identical SCALE types for `H` and `N` and the
	/// same chain domain. Neither local clocks nor vote signatures enter the ID.
	pub fn id(&self, chain_domain: &[u8; 32]) -> TargetId {
		let mut hasher = Blake2b::<U32>::new();
		hasher.update(TARGET_ID_DOMAIN);
		hasher.update(chain_domain);
		hasher.update(self.encode());
		TargetId(hasher.finalize().into())
	}
}

/// A compact GRANDPA vote target: consensus identity and consensus height.
#[derive(
	Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode, DecodeWithMemTracking,
)]
#[cfg_attr(feature = "derive-codec", derive(scale_info::TypeInfo))]
pub struct TargetRef {
	/// The consensus target's identity.
	pub id: TargetId,
	/// The target's view, used as GRANDPA's target number.
	pub view: View,
}

impl TargetRef {
	/// Return the `(hash, number)` pair expected by GRANDPA's generic API.
	pub fn as_pair(self) -> (TargetId, View) {
		(self.id, self.view)
	}
}
