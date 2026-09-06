// Copyright 2026 Parity Technologies (UK) Ltd
// SPDX-License-Identifier: Apache-2.0

//! In-memory block/timeout chain used only by tests.

use crate::{
	std::{collections::BTreeMap, vec::Vec},
	view::{BlockRef, ConsensusTarget, TargetId, TargetKind, TargetRef, View},
	Chain, Error, VoteTarget,
};
use core::fmt;
use num::{CheckedAdd, One};
use parity_scale_codec::Encode;

#[derive(Clone, Debug)]
struct Entry<H, N> {
	target: ConsensusTarget<H, N>,
	latest_block: BlockRef<H, N>,
}

/// The changes made when a consensus target becomes finalized.
///
/// Finalizing a timeout can also finalize earlier, previously unfinalized real
/// blocks. Consumers must inspect `block_changed`, not just the target's kind.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Finalization<H, N> {
	/// The new finalized consensus position, including any timeout views.
	pub target: TargetRef,
	/// The latest real block in the newly finalized consensus history.
	pub block: BlockRef<H, N>,
	/// Newly finalized targets in ascending view order, excluding old finality.
	pub newly_finalized: Vec<TargetRef>,
	/// Whether the real-block finality projection advanced.
	pub block_changed: bool,
}

/// An in-memory consensus tree containing real blocks and timeout events.
///
/// Every edge advances the view by one. Historical branches are retained for
/// ancestry proofs, but new imports cannot leave the finalized
/// branch. Application code must validate real block content, parent hashes,
/// consensus-parent commitments and timeout eligibility before importing.
#[derive(Clone, Debug)]
pub(crate) struct ViewChain<H, N> {
	domain: [u8; 32],
	entries: BTreeMap<TargetId, Entry<H, N>>,
	root: TargetRef,
	finalized: TargetRef,
}

impl<H, N> ViewChain<H, N>
where
	H: Clone + Ord + Encode,
	N: Clone + Ord + Encode + CheckedAdd + One,
{
	/// Start at a trusted checkpoint and its latest finalized real block.
	///
	/// `root_view` need not equal `root.number`; views can have been skipped
	/// before the checkpoint. The checkpoint is considered finalized.
	///
	/// All peers must agree on this root checkpoint and its identity. Calling
	/// `new` at a later block or timeout synthesizes a new parentless identity;
	/// it does not preserve the original target ID. Recovery must restore the
	/// original root and target ancestry before applying the saved finality.
	pub fn new(domain: [u8; 32], root_view: View, root: BlockRef<H, N>) -> Self {
		let target = ConsensusTarget {
			parent: None,
			view: root_view,
			kind: TargetKind::Block(root.clone()),
		};
		let root_ref = TargetRef { id: target.id(&domain), view: root_view };
		let mut entries = BTreeMap::new();
		entries.insert(root_ref.id, Entry { target, latest_block: root });
		Self { domain, entries, root: root_ref, finalized: root_ref }
	}

	/// The trusted root checkpoint.
	pub fn root(&self) -> TargetRef {
		self.root
	}

	/// The chain-specific domain used to compute all consensus target IDs.
	pub fn domain(&self) -> &[u8; 32] {
		&self.domain
	}

	/// The current finalized consensus target, including finalized timeouts.
	pub fn finalized_target(&self) -> TargetRef {
		self.finalized
	}

	/// The current finalized real block, unchanged by timeout-only progress.
	pub fn finalized_block(&self) -> &BlockRef<H, N> {
		&self.entries[&self.finalized.id].latest_block
	}

	/// Look up a stored target, including historical non-finalized branches.
	pub fn target(&self, id: TargetId) -> Option<&ConsensusTarget<H, N>> {
		self.entries.get(&id).map(|entry| &entry.target)
	}

	/// The last real block in a target's ancestry, or the target itself if a block.
	pub fn latest_block(&self, id: TargetId) -> Option<&BlockRef<H, N>> {
		self.entries.get(&id).map(|entry| &entry.latest_block)
	}

	/// Whether an exact `(target identity, view)` pair exists in this tree.
	pub fn contains(&self, id: TargetId, view: View) -> bool {
		self.target(id).is_some_and(|target| target.view == view)
	}

	/// Construct the next-view block after `parent`, checking its real height.
	///
	/// This does not import the target or validate its header/content. A block's
	/// real parent must be [`Self::latest_block`] of the consensus parent.
	pub fn block(
		&self,
		parent: TargetId,
		block: BlockRef<H, N>,
	) -> Result<ConsensusTarget<H, N>, ViewError> {
		let target = ConsensusTarget {
			parent: Some(parent),
			view: self.next_view(parent)?,
			kind: TargetKind::Block(block),
		};
		self.validate_child(&target)?;
		Ok(target)
	}

	/// Construct the next-view timeout after `parent`, without importing it.
	///
	/// The application is responsible for deciding when the view has timed out.
	pub fn timeout(&self, parent: TargetId) -> Result<ConsensusTarget<H, N>, ViewError> {
		Ok(ConsensusTarget {
			parent: Some(parent),
			view: self.next_view(parent)?,
			kind: TargetKind::ViewTimeout,
		})
	}

	/// Validate and import a block or timeout target.
	///
	/// Unknown parents, skipped views, invalid real block heights and new forks
	/// conflicting with finality are rejected. Existing identical targets are
	/// accepted idempotently, even if retained solely for historical proofs.
	pub fn import(&mut self, target: ConsensusTarget<H, N>) -> Result<TargetRef, ViewError> {
		let id = target.id(&self.domain);
		let target_ref = TargetRef { id, view: target.view };
		if let Some(existing) = self.entries.get(&id) {
			return if existing.target == target {
				Ok(target_ref)
			} else {
				Err(ViewError::TargetCollision)
			};
		}
		let latest_block = self.validate_child(&target)?;
		let parent = target.parent.ok_or(ViewError::InvalidRoot)?;
		if !self.descends_from(self.finalized.id, parent) {
			return Err(ViewError::ConflictingFinality);
		}
		self.entries.insert(id, Entry { target, latest_block });
		Ok(target_ref)
	}

	/// Compute the finality changes without mutating the tree.
	///
	/// A finalization callback can persist this result before calling
	/// [`Self::finalize`]. It must serialize finalization operations so the preview
	/// remains current.
	/// This method validates ancestry, not the GRANDPA certificate; callers must
	/// establish finality through the voter or independently verify a certificate.
	pub fn preview_finalization(&self, target: TargetRef) -> Result<Finalization<H, N>, ViewError> {
		let entry = self.entries.get(&target.id).ok_or(ViewError::UnknownTarget)?;
		if entry.target.view != target.view {
			return Err(ViewError::InvalidView);
		}
		if !self.descends_from(self.finalized.id, target.id) {
			return Err(ViewError::ConflictingFinality);
		}
		let mut newly_finalized = Vec::new();
		let mut cursor = target.id;
		while cursor != self.finalized.id {
			let ancestor = &self.entries[&cursor].target;
			newly_finalized.push(TargetRef { id: cursor, view: ancestor.view });
			cursor = ancestor.parent.ok_or(ViewError::InvalidRoot)?;
		}
		newly_finalized.reverse();
		Ok(Finalization {
			target,
			block: entry.latest_block.clone(),
			newly_finalized,
			block_changed: &entry.latest_block != self.finalized_block(),
		})
	}

	/// Advance consensus finality and project it onto real-block finality.
	///
	/// Repeating the current target is idempotent. Any older or conflicting
	/// target is rejected. The caller must have verified a finality certificate;
	/// this method alone does not authenticate a finalization request.
	pub fn finalize(&mut self, target: TargetRef) -> Result<Finalization<H, N>, ViewError> {
		let update = self.preview_finalization(target)?;
		self.finalized = target;
		Ok(update)
	}

	fn next_view(&self, parent: TargetId) -> Result<View, ViewError> {
		self.target(parent)
			.ok_or(ViewError::UnknownTarget)?
			.view
			.checked_add(1)
			.ok_or(ViewError::ViewOverflow)
	}

	fn validate_child(&self, target: &ConsensusTarget<H, N>) -> Result<BlockRef<H, N>, ViewError> {
		let parent = target.parent.ok_or(ViewError::InvalidRoot)?;
		let parent_entry = self.entries.get(&parent).ok_or(ViewError::UnknownTarget)?;
		if target.view != self.next_view(parent)? {
			return Err(ViewError::InvalidView);
		}
		match &target.kind {
			TargetKind::ViewTimeout => Ok(parent_entry.latest_block.clone()),
			TargetKind::Block(block) => {
				let next_number = parent_entry
					.latest_block
					.number
					.checked_add(&N::one())
					.ok_or(ViewError::BlockNumberOverflow)?;
				if block.number != next_number {
					return Err(ViewError::InvalidBlockNumber);
				}
				Ok(block.clone())
			},
		}
	}

	fn descends_from(&self, base: TargetId, mut target: TargetId) -> bool {
		let base_view = match self.target(base) {
			Some(base) => base.view,
			None => return false,
		};
		while let Some(entry) = self.entries.get(&target) {
			if target == base {
				return true;
			}
			if entry.target.view <= base_view {
				return false;
			}
			match entry.target.parent {
				Some(parent) => target = parent,
				None => return false,
			}
		}
		false
	}
}

impl<H, N> Chain<TargetId, View> for ViewChain<H, N>
where
	H: Clone + Ord + Encode,
	N: Clone + Ord + Encode + CheckedAdd + One,
{
	fn vote_target(&self, hash: TargetId) -> Option<VoteTarget<TargetId, View>> {
		self.target(hash).map(|target| match &target.kind {
			TargetKind::Block(_) => VoteTarget::Block(hash, target.view),
			TargetKind::ViewTimeout => VoteTarget::ViewTimeout(hash, target.view),
		})
	}

	fn ancestry(&self, base: TargetId, block: TargetId) -> Result<Vec<TargetId>, Error> {
		if !self.descends_from(base, block) {
			return Err(Error::NotDescendent);
		}
		let mut ancestry = Vec::new();
		let mut cursor = block;
		while cursor != base {
			let parent = self.entries[&cursor].target.parent.ok_or(Error::NotDescendent)?;
			if parent != base {
				ancestry.push(parent);
			}
			cursor = parent;
		}
		Ok(ancestry)
	}

	fn is_equal_or_descendent_of(&self, base: TargetId, block: TargetId) -> bool {
		// Unlike the trait's default, unknown equal IDs are not valid targets.
		self.descends_from(base, block)
	}
}

/// Invalid fixture data or an operation incompatible with known finality.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ViewError {
	UnknownTarget,
	InvalidView,
	InvalidBlockNumber,
	ConflictingFinality,
	ViewOverflow,
	BlockNumberOverflow,
	InvalidRoot,
	TargetCollision,
}

impl fmt::Display for ViewError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(match self {
			Self::UnknownTarget => "unknown consensus target",
			Self::InvalidView => "invalid consensus view",
			Self::InvalidBlockNumber => "invalid real block number",
			Self::ConflictingFinality => "consensus target conflicts with finality",
			Self::ViewOverflow => "consensus view overflow",
			Self::BlockNumberOverflow => "real block number overflow",
			Self::InvalidRoot => "invalid consensus root",
			Self::TargetCollision => "consensus target identity collision",
		})
	}
}

#[cfg(feature = "std")]
impl std::error::Error for ViewError {}

mod tests;
