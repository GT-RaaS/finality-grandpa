//! Tests for the block/timeout chain fixture and consensus target encoding.

use super::*;

type TestChain = ViewChain<u64, u64>;

fn chain() -> TestChain {
	TestChain::new([7; 32], 10, BlockRef { hash: 100, number: 100 })
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
fn identities_bind_the_chain_parent_view_and_kind() {
	let chain = chain();
	let target = chain.timeout(chain.root().id).unwrap();
	// Independently computed Blake2b-256 vector for the documented SCALE layout.
	assert_eq!(
		target.id(chain.domain()),
		TargetId([
			0x94, 0x4d, 0x5c, 0xdd, 0xb9, 0xb6, 0xf7, 0x56, 0x8e, 0xf4, 0x96, 0x17, 0x89, 0x95,
			0xa4, 0x2f, 0xcd, 0xe9, 0x2a, 0x19, 0x91, 0x18, 0xf2, 0xf2, 0xc8, 0xd7, 0xa0, 0xda,
			0x72, 0x11, 0x19, 0x50,
		]),
	);
	assert_eq!(target.id(chain.domain()), target.clone().id(chain.domain()));
	assert_ne!(target.id(chain.domain()), target.id(&[8; 32]));
	let mut altered = target.clone();
	altered.parent = Some(TargetId([1; 32]));
	assert_ne!(target.id(chain.domain()), altered.id(chain.domain()));
	altered = target.clone();
	altered.view += 1;
	assert_ne!(target.id(chain.domain()), altered.id(chain.domain()));
	altered = target.clone();
	altered.kind = TargetKind::Block(BlockRef { hash: 101, number: 101 });
	assert_ne!(target.id(chain.domain()), altered.id(chain.domain()));
	assert_eq!(TargetKind::<u64, u64>::ViewTimeout.encode(), vec![1]);
	assert_eq!(TargetKind::Block(BlockRef { hash: 1u64, number: 1u64 }).encode()[0], 0);
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
	let unknown = TargetId([0; 32]);
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
	assert_eq!(chain.vote_target(TargetId([0; 32])), None);
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
	target.parent = Some(TargetId([0; 32]));
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
	let chain = TestChain::new([7; 32], u64::MAX, BlockRef { hash: 0, number: 0 });
	assert_eq!(chain.timeout(chain.root().id), Err(ViewError::ViewOverflow));
	let chain = TestChain::new([7; 32], 0, BlockRef { hash: 0, number: u64::MAX });
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
