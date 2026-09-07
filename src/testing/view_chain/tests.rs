//! Tests for the block/timeout chain fixture and consensus target encoding.

use super::*;

type TestChain = ViewChain<u64, u64>;

fn chain() -> TestChain {
	TestChain::new(10, BlockRef { hash: 100, number: 100 })
}

fn block(chain: &mut TestChain, parent: TargetId, hash: u64, number: u64) -> TargetRef {
	let target = chain.block(parent, BlockRef { hash, number }).unwrap();
	chain.import(target).unwrap()
}

fn timeout(chain: &mut TestChain, parent: TargetId) -> TargetRef {
	let target = chain.timeout(parent).unwrap();
	chain.import(target).unwrap()
}

#[test]
fn target_id_and_view_types_are_independent_of_block_types() {
	let target: crate::view::ConsensusTarget<u16, u32, [u8; 32], u64> =
		crate::view::ConsensusTarget {
			parent: Some(7),
			view: 11,
			kind: TargetKind::Block(BlockRef { hash: [9; 32], number: 100 }),
		};
	let target_ref: crate::view::TargetRef<u16, u32> =
		crate::view::TargetRef { id: 8, view: target.view };
	assert_eq!(target.parent, Some(7));
	assert_eq!(target_ref.as_pair(), (8u16, 11u32));
	assert_eq!(target.kind, TargetKind::Block(BlockRef { hash: [9; 32], number: 100u64 }));
	#[cfg(feature = "derive-codec")]
	{
		use parity_scale_codec::{Decode, Encode};

		let decoded = crate::view::ConsensusTarget::decode(&mut &target.encode()[..]).unwrap();
		assert_eq!(target, decoded);
		let decoded_ref = crate::view::TargetRef::decode(&mut &target_ref.encode()[..]).unwrap();
		assert_eq!(target_ref, decoded_ref);
		assert_eq!(TargetKind::<u64, u64>::ViewTimeout.encode(), vec![1]);
		assert_eq!(TargetKind::Block(BlockRef { hash: 1u64, number: 1u64 }).encode()[0], 0);
	}
}

#[test]
fn consecutive_timeouts_advance_views_and_preserve_block_ancestry() {
	let mut chain = chain();
	let root = chain.root();
	let first = timeout(&mut chain, root.id);
	let second = timeout(&mut chain, first.id);
	let last = block(&mut chain, second.id, 101, 101);
	assert_eq!((first.view, second.view, last.view), (11, 12, 13));
	assert_eq!(chain.latest_block(second.id), Some(&BlockRef { hash: 100, number: 100 }));
	assert_eq!(chain.latest_block(last.id), Some(&BlockRef { hash: 101, number: 101 }));
	assert_eq!(chain.ancestry(root.id, last.id).unwrap(), vec![second.id, first.id]);
	assert_eq!(chain.ancestry(first.id, second.id).unwrap(), vec![]);
	assert_eq!(chain.ancestry(root.id, root.id).unwrap(), vec![]);
	assert!(chain.is_equal_or_descendent_of(first.id, last.id));
	assert!(!chain.is_equal_or_descendent_of(last.id, first.id));
	let unknown = 0;
	assert!(!chain.is_equal_or_descendent_of(unknown, unknown));
	assert!(chain.ancestry(unknown, unknown).is_err());
}

#[test]
fn vote_target_resolves_exact_event_kind_and_view() {
	let mut chain = chain();
	let root = chain.root();
	let skipped = timeout(&mut chain, root.id);
	let next_block = block(&mut chain, skipped.id, 101, 101);
	assert_eq!(chain.vote_target(root.id), Some(VoteTarget::Block(root.id, 10)));
	assert_eq!(chain.vote_target(skipped.id), Some(VoteTarget::ViewTimeout(skipped.id, 11)));
	assert_eq!(chain.vote_target(next_block.id), Some(VoteTarget::Block(next_block.id, 12)));
	assert_eq!(chain.vote_target(next_block.id).unwrap().into_target(), next_block.as_pair());
	assert_eq!(chain.vote_target(0), None);
}

#[test]
fn malformed_children_are_rejected_without_changing_the_tree() {
	let mut chain = chain();
	let root = chain.root();
	let root_target = chain.target(root.id).unwrap().clone();
	assert_eq!(chain.import(root_target.clone()), Ok(root));
	let mut target = chain.timeout(chain.root().id).unwrap();
	target.view += 1;
	assert_eq!(chain.import(target.clone()), Err(ViewError::InvalidView));
	target.view -= 1;
	target.parent = None;
	assert_eq!(chain.import(target.clone()), Err(ViewError::InvalidRoot));
	target.parent = Some(0);
	assert_eq!(chain.import(target.clone()), Err(ViewError::UnknownTarget));
	target.parent = Some(chain.root().id);
	target.kind = TargetKind::Block(BlockRef { hash: 102, number: 102 });
	assert_eq!(chain.import(target), Err(ViewError::InvalidBlockNumber));
	assert_eq!(chain.entries.len(), 1);
	assert_eq!(chain.target(root.id), Some(&root_target));
	assert_eq!(chain.root(), root);
	assert_eq!(chain.finalized_target(), root);
}

#[test]
fn numeric_overflow_does_not_create_wrapped_targets() {
	let chain = TestChain::new(u64::MAX, BlockRef { hash: 0, number: 0 });
	assert_eq!(chain.timeout(chain.root().id), Err(ViewError::ViewOverflow));
	let chain = TestChain::new(0, BlockRef { hash: 0, number: u64::MAX });
	assert_eq!(
		chain.block(chain.root().id, BlockRef { hash: 1, number: 0 }),
		Err(ViewError::BlockNumberOverflow)
	);
	assert!(chain.timeout(chain.root().id).is_ok());
}

#[test]
fn finalizing_timeout_can_finalize_an_earlier_block() {
	let mut chain = chain();
	let root = chain.root();
	let real = block(&mut chain, root.id, 101, 101);
	let skipped = timeout(&mut chain, real.id);
	let preview = chain.preview_finalization(skipped).unwrap();
	assert_eq!(chain.finalized_target(), root);
	assert_eq!(preview.newly_finalized, vec![real, skipped]);
	assert!(preview.block_changed);
	assert_eq!(preview.block, BlockRef { hash: 101, number: 101 });
	assert_eq!(chain.finalize(skipped).unwrap(), preview);
	assert_eq!(chain.finalized_target(), skipped);
	assert_eq!(chain.finalized_block(), &preview.block);
	let skipped_again = timeout(&mut chain, skipped.id);
	let update = chain.finalize(skipped_again).unwrap();
	assert_eq!(update.newly_finalized, vec![skipped_again]);
	assert!(!update.block_changed);
	let repeated = chain.finalize(skipped_again).unwrap();
	assert!(repeated.newly_finalized.is_empty());
	assert!(!repeated.block_changed);
	assert_eq!(chain.finalize(real), Err(ViewError::ConflictingFinality));
	assert_eq!(
		chain.finalize(TargetRef { view: skipped_again.view + 1, ..skipped_again }),
		Err(ViewError::InvalidView)
	);
}

#[test]
fn finalized_timeout_excludes_late_blocks_and_retains_old_proofs() {
	let mut chain = chain();
	let root = chain.root();
	let competing = block(&mut chain, root.id, 101, 101);
	let skipped = timeout(&mut chain, root.id);
	chain.finalize(skipped).unwrap();
	let late = chain.block(root.id, BlockRef { hash: 102, number: 101 }).unwrap();
	assert_eq!(chain.import(late), Err(ViewError::ConflictingFinality));
	let conflicting_child = chain.timeout(competing.id).unwrap();
	assert_eq!(chain.import(conflicting_child), Err(ViewError::ConflictingFinality));
	assert_eq!(chain.finalize(competing), Err(ViewError::ConflictingFinality));
	assert_eq!(chain.finalized_target(), skipped);
	assert!(!chain.is_equal_or_descendent_of(skipped.id, competing.id));
	assert!(chain.ancestry(root.id, competing.id).unwrap().is_empty());
	assert!(chain.contains(competing.id, competing.view));
	assert!(!chain.contains(competing.id, competing.view + 1));
	let retained = chain.target(competing.id).unwrap().clone();
	assert_eq!(chain.import(retained), Ok(competing));
}
