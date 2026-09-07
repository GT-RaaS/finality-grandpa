// Copyright 2026 Parity Technologies (UK) Ltd
// SPDX-License-Identifier: Apache-2.0

//! A consensus history containing both blocks and skipped views.
//!
//! Instantiate GRANDPA with the application's target ID as its hash and view
//! type as its number. This crate does not compute target IDs.
//! Every consensus edge advances the view exactly once, including a
//! [`TargetKind::ViewTimeout`]; only a [`TargetKind::Block`] advances the real
//! block number. This preserves the consecutive-height assumption of GRANDPA's
//! vote graph without assigning artificial heights to real blocks.
//!
//! A timeout is a competing child of its parent, not a notification that a
//! timer elapsed. It becomes irreversible only when finalized by GRANDPA.
//! The application supplies target storage and ancestry through [`crate::Chain`],
//! and handles real-block finalization through its environment.

#[cfg(feature = "derive-codec")]
use parity_scale_codec::{Decode, DecodeWithMemTracking, Encode};
#[cfg(feature = "derive-codec")]
use scale_info::TypeInfo;

/// A real block referenced by a consensus target.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode, DecodeWithMemTracking, TypeInfo))]
pub struct BlockRef<H, N> {
	/// The real block hash.
	pub hash: H,
	/// The real block number, which does not increase during timeout views.
	pub number: N,
}

/// The event represented by a consensus target.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode, DecodeWithMemTracking, TypeInfo))]
pub enum TargetKind<H, N> {
	/// A real block extending the parent's latest real block.
	#[cfg_attr(feature = "derive-codec", codec(index = 0))]
	Block(BlockRef<H, N>),
	/// No block was selected for this view; inherit the parent's latest block.
	#[cfg_attr(feature = "derive-codec", codec(index = 1))]
	ViewTimeout,
}

/// A block or timeout with a unique position in the consensus tree.
///
/// `I` and `V` are the application's consensus target ID and view types.
/// `H` and `N` are the real block hash and number types.
///
/// Decoding does not validate ancestry, height or application block validity.
/// The application must validate targets and make their ancestry available
/// through [`crate::Chain`] before delivering votes. It must also authenticate
/// the binding between a block and this consensus parent, and validate the
/// block's actual parent hash.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode, DecodeWithMemTracking, TypeInfo))]
pub struct ConsensusTarget<I, V, H, N> {
	/// The consensus parent. Only the trusted root checkpoint has no parent.
	pub parent: Option<I>,
	/// Exactly the parent's view plus one, except at the trusted root.
	pub view: V,
	/// The block or timeout selected at this position.
	pub kind: TargetKind<H, N>,
}

/// A compact GRANDPA vote target: consensus identity and consensus height.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode, DecodeWithMemTracking, TypeInfo))]
pub struct TargetRef<I, V> {
	/// The consensus target's identity.
	pub id: I,
	/// The target's view, used as GRANDPA's target number.
	pub view: V,
}

impl<I, V> TargetRef<I, V> {
	/// Return the `(hash, number)` pair expected by GRANDPA's generic API.
	pub fn as_pair(self) -> (I, V) {
		(self.id, self.view)
	}
}
