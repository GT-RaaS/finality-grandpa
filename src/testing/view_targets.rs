// Copyright 2026 Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Exercise the existing GRANDPA graph, rounds and proofs with mixed view targets.

use super::view_chain::{ConsensusTarget, TargetId, TargetRef, ViewChain};
use crate::{
	round::{Round, RoundParams},
	validate_commit,
	view::{BlockRef, TargetKind},
	voter_set::VoterSet,
	Chain, Commit, Precommit, Prevote, SignedPrecommit,
};

type TestChain = ViewChain<[u8; 32], u64>;
type TestRound = Round<u64, TargetId, u64, u64>;

fn block(number: u64, tag: u8) -> BlockRef<[u8; 32], u64> {
	BlockRef { hash: [tag; 32], number }
}

fn chain() -> TestChain {
	TestChain::new(10, block(100, 0))
}

fn voters() -> VoterSet<u64> {
	VoterSet::new((0..4).map(|id| (id, 1))).unwrap()
}

fn round(base: TargetRef) -> TestRound {
	Round::new(RoundParams { round_number: 17, voters: voters(), base: (base.id, base.view) })
}

fn append_block(chain: &mut TestChain, parent: TargetRef, number: u64, tag: u8) -> TargetRef {
	let target = chain.block(parent.id, block(number, tag)).unwrap();
	chain.import(target).unwrap()
}

fn append_timeout(chain: &mut TestChain, parent: TargetRef) -> TargetRef {
	let target = chain.timeout(parent.id).unwrap();
	chain.import(target).unwrap()
}

fn signed_precommit(target: TargetRef, voter: u64) -> SignedPrecommit<TargetId, u64, u64, u64> {
	SignedPrecommit {
		precommit: Precommit::new(target.id, target.view),
		signature: voter,
		id: voter,
	}
}

#[test]
fn mixed_history_uses_view_for_ghost_finality_and_estimate() {
	let mut chain = chain();
	let root = chain.root();
	let first_block = append_block(&mut chain, root, 101, 1);
	let first_timeout = append_timeout(&mut chain, first_block);
	let second_timeout = append_timeout(&mut chain, first_timeout);
	let second_block = append_block(&mut chain, second_timeout, 102, 2);

	assert_eq!(second_block.view, 14);
	assert_eq!(
		chain.ancestry(root.id, second_block.id).unwrap(),
		vec![second_timeout.id, first_timeout.id, first_block.id],
	);

	let mut round = round(root);
	for id in 0..3 {
		round
			.import_prevote(&chain, Prevote::new(second_block.id, second_block.view), id, id)
			.unwrap();
	}
	assert_eq!(round.state().prevote_ghost, Some((second_block.id, 14)));
	assert_eq!(round.state().estimate, Some((second_block.id, 14)));
	assert_eq!(round.state().finalized, None);

	// One vote at the timeout plus two at its block descendant finalizes the
	// timeout, without inventing a real block at either timeout's view.
	for (id, target) in [(0, second_timeout), (1, second_block), (2, second_block)] {
		round
			.import_precommit(&chain, Precommit::new(target.id, target.view), id, id)
			.unwrap();
	}
	assert_eq!(round.precommit_ghost(), Some((second_timeout.id, 13)));
	assert_eq!(round.state().finalized, Some((second_timeout.id, 13)));
	assert_eq!(round.state().estimate, Some((second_block.id, 14)));

	// The fourth precommit supplies a true supermajority for the block itself.
	round
		.import_precommit(&chain, Precommit::new(second_block.id, second_block.view), 3, 3)
		.unwrap();
	assert_eq!(round.state().finalized, Some((second_block.id, 14)));
	assert_eq!(round.state().estimate, Some((second_block.id, 14)));
	assert!(round.state().completable);
}

#[test]
fn block_and_timeout_at_the_same_view_do_not_merge_voting_weight() {
	let mut chain = chain();
	let root = chain.root();
	let block = append_block(&mut chain, root, 101, 1);
	let timeout = append_timeout(&mut chain, root);
	assert_eq!(block.view, timeout.view);
	assert_ne!(block.id, timeout.id);
	assert!(!chain.is_equal_or_descendent_of(block.id, timeout.id));
	assert!(!chain.is_equal_or_descendent_of(timeout.id, block.id));

	let mut round = round(root);
	for (id, target) in [(0, block), (1, block), (2, timeout), (3, timeout)] {
		round
			.import_prevote(&chain, Prevote::new(target.id, target.view), id, id)
			.unwrap();
		round
			.import_precommit(&chain, Precommit::new(target.id, target.view), id, id)
			.unwrap();
	}

	assert_eq!(round.state().prevote_ghost, Some((root.id, root.view)));
	assert_eq!(round.precommit_ghost(), Some((root.id, root.view)));
	assert_eq!(round.state().finalized, Some((root.id, root.view)));
	assert_eq!(round.state().estimate, Some((root.id, root.view)));
}

#[test]
fn block_and_timeout_by_one_voter_are_equivocations_in_each_phase() {
	let mut chain = chain();
	let root = chain.root();
	let block = append_block(&mut chain, root, 101, 1);
	let timeout = append_timeout(&mut chain, root);
	let mut round = round(root);

	let block_prevote = Prevote::new(block.id, block.view);
	let timeout_prevote = Prevote::new(timeout.id, timeout.view);
	assert!(round
		.import_prevote(&chain, block_prevote.clone(), 0, 0)
		.unwrap()
		.equivocation
		.is_none());
	assert!(round.import_prevote(&chain, block_prevote.clone(), 0, 0).unwrap().duplicated);
	let equivocation = round
		.import_prevote(&chain, timeout_prevote.clone(), 0, 0)
		.unwrap()
		.equivocation
		.expect("Block and Timeout are distinct targets, even at the same view");
	assert_eq!(equivocation.round_number, 17);
	assert_eq!(equivocation.first.0, block_prevote);
	assert_eq!(equivocation.second.0, timeout_prevote);
	assert_eq!(round.prevote_participation().1, 1);

	let block_precommit = Precommit::new(block.id, block.view);
	let timeout_precommit = Precommit::new(timeout.id, timeout.view);
	round.import_precommit(&chain, block_precommit.clone(), 0, 0).unwrap();
	let equivocation = round
		.import_precommit(&chain, timeout_precommit.clone(), 0, 0)
		.unwrap()
		.equivocation
		.unwrap();
	assert_eq!(equivocation.first.0, block_precommit);
	assert_eq!(equivocation.second.0, timeout_precommit);
	assert_eq!(round.precommit_participation().1, 1);
}

#[test]
fn timeout_commit_accepts_descendant_votes_and_finalizes_preceding_real_block() {
	let mut chain = chain();
	let root = chain.root();
	let first_block = append_block(&mut chain, root, 101, 1);
	let timeout = append_timeout(&mut chain, first_block);
	let descendant = append_block(&mut chain, timeout, 102, 2);
	let commit = Commit {
		target_hash: timeout.id,
		target_number: timeout.view,
		precommits: vec![
			signed_precommit(timeout, 0),
			signed_precommit(descendant, 1),
			signed_precommit(descendant, 2),
		],
	};
	assert!(validate_commit(&commit, &voters(), &chain).unwrap().is_valid());

	let finalized = chain.finalize(timeout).unwrap();
	assert_eq!(finalized.target, timeout);
	assert_eq!(finalized.block, block(101, 1));
	assert!(finalized.block_changed);
	assert_eq!(finalized.newly_finalized.len(), 2);

	// Another timeout advances finality, but the projected block does not move.
	let next_timeout = append_timeout(&mut chain, timeout);
	let finalized = chain.finalize(next_timeout).unwrap();
	assert_eq!(finalized.target.view, timeout.view + 1);
	assert_eq!(finalized.block, block(101, 1));
	assert!(!finalized.block_changed);
}

#[test]
fn finalized_timeout_rejects_late_block_but_accepts_a_block_in_the_next_view() {
	let mut chain = chain();
	let root = chain.root();
	let timeout = append_timeout(&mut chain, root);
	// Capture the exact competing candidate before finality so its rejection
	// cannot be attributed to an inability to construct it after finality.
	let late_block = chain.block(root.id, block(101, 1)).unwrap();
	let existing_conflict = append_block(&mut chain, root, 101, 2);
	let finalized = chain.finalize(timeout).unwrap();
	assert_eq!(finalized.block, block(100, 0));
	assert!(!finalized.block_changed);
	assert!(chain.import(late_block).is_err());
	assert!(chain.finalize(existing_conflict).is_err());

	let next_block = append_block(&mut chain, timeout, 101, 3);
	assert_eq!(next_block.view, timeout.view + 1);
	assert_eq!(chain.finalize(next_block).unwrap().block, block(101, 3));
}

#[test]
fn malformed_view_and_real_block_height_are_rejected_before_round_import() {
	let mut chain = chain();
	let root = chain.root();
	let timeout = append_timeout(&mut chain, root);

	// View advances on every edge, even when real block height does not.
	let skipped_view = ConsensusTarget {
		parent: Some(timeout.id),
		view: timeout.view + 2,
		kind: TargetKind::ViewTimeout,
	};
	assert!(chain.import(skipped_view).is_err());
	let wrong_block_number = ConsensusTarget {
		parent: Some(timeout.id),
		view: timeout.view + 1,
		kind: TargetKind::Block(block(102, 1)),
	};
	assert!(chain.import(wrong_block_number).is_err());
	let same_block_number = ConsensusTarget {
		parent: Some(timeout.id),
		view: timeout.view + 1,
		kind: TargetKind::Block(block(100, 1)),
	};
	assert!(chain.import(same_block_number).is_err());

	// Neither invalid candidate should prevent importing the legitimate child.
	let next_block = append_block(&mut chain, timeout, 101, 1);
	assert_eq!(chain.ancestry(root.id, next_block.id).unwrap(), vec![timeout.id]);
}

#[test]
fn round_rejects_unknown_or_forged_views_without_consuming_the_voters_vote() {
	let mut chain = chain();
	let root = chain.root();
	let timeout = append_timeout(&mut chain, root);
	let mut round = round(root);
	for invalid in [
		TargetRef { id: 99, view: timeout.view },
		TargetRef { id: timeout.id, view: timeout.view + 1 },
	] {
		assert!(matches!(
			round.import_prevote(&chain, Prevote::new(invalid.id, invalid.view), 0, 0),
			Err(crate::Error::InvalidTarget),
		));
		assert!(matches!(
			round.import_precommit(&chain, Precommit::new(invalid.id, invalid.view), 0, 0),
			Err(crate::Error::InvalidTarget),
		));
	}
	assert_eq!(round.prevote_participation().1, 0);
	assert_eq!(round.precommit_participation().1, 0);
	assert_eq!(round.state().prevote_ghost, None);
	assert_eq!(round.state().finalized, None);

	let valid = round
		.import_prevote(&chain, Prevote::new(timeout.id, timeout.view), 0, 0)
		.unwrap();
	assert!(valid.valid_voter);
	assert!(!valid.duplicated);
	assert!(valid.equivocation.is_none());
	let valid = round
		.import_precommit(&chain, Precommit::new(timeout.id, timeout.view), 0, 0)
		.unwrap();
	assert!(valid.valid_voter);
	assert!(!valid.duplicated);
	assert!(valid.equivocation.is_none());
	assert_eq!(round.prevote_participation().1, 1);
	assert_eq!(round.precommit_participation().1, 1);
}

#[test]
fn round_rejects_a_known_competing_block_before_tracking_the_vote() {
	let mut chain = chain();
	let root = chain.root();
	let timeout = append_timeout(&mut chain, root);
	let block = append_block(&mut chain, root, 101, 1);
	let mut round = round(timeout);
	assert!(matches!(
		round.import_prevote(&chain, Prevote::new(block.id, block.view), 0, 0),
		Err(crate::Error::NotDescendent),
	));
	assert!(matches!(
		round.import_precommit(&chain, Precommit::new(block.id, block.view), 0, 0),
		Err(crate::Error::NotDescendent),
	));
	let prevote = round
		.import_prevote(&chain, Prevote::new(timeout.id, timeout.view), 0, 0)
		.unwrap();
	let precommit = round
		.import_precommit(&chain, Precommit::new(timeout.id, timeout.view), 0, 0)
		.unwrap();
	assert!(!prevote.duplicated && prevote.equivocation.is_none());
	assert!(!precommit.duplicated && precommit.equivocation.is_none());
}

#[test]
fn commit_validation_rejects_forged_views_but_ignores_nonvoter_metadata() {
	let mut chain = chain();
	let root = chain.root();
	let timeout = append_timeout(&mut chain, root);
	let valid = Commit {
		target_hash: timeout.id,
		target_number: timeout.view,
		precommits: (0..3).map(|id| signed_precommit(timeout, id)).collect(),
	};
	assert!(validate_commit(&valid, &voters(), &chain).unwrap().is_valid());

	let mut wrong_commit_view = valid.clone();
	wrong_commit_view.target_number += 1;
	let mut unknown_commit = valid.clone();
	unknown_commit.target_hash = 99;
	let mut wrong_vote_view = valid.clone();
	wrong_vote_view.precommits[0].precommit.target_number += 1;
	let mut unknown_vote = valid.clone();
	unknown_vote.precommits[0].precommit.target_hash = 99;
	let mut forged_height_everywhere = valid.clone();
	forged_height_everywhere.target_number += 100;
	for signed in &mut forged_height_everywhere.precommits {
		signed.precommit.target_number = forged_height_everywhere.target_number;
	}
	for invalid in
		[wrong_commit_view, unknown_commit, wrong_vote_view, unknown_vote, forged_height_everywhere]
	{
		assert!(!validate_commit(&invalid, &voters(), &chain).unwrap().is_valid());
	}

	let mut with_nonvoter = valid;
	with_nonvoter
		.precommits
		.push(signed_precommit(TargetRef { id: 99, view: u64::MAX }, 99));
	let validation = validate_commit(&with_nonvoter, &voters(), &chain).unwrap();
	assert!(validation.is_valid());
	assert_eq!(validation.num_invalid_voters(), 1);
}

#[test]
fn mixed_sequences_preserve_ancestry_round_finality_and_block_projection() {
	// Enumerate all eight-event Block/Timeout sequences. In particular this
	// includes runs consisting entirely of timeouts and transitions after
	// several timeouts, where using block_number as graph height would fail.
	for pattern in 0u16..256 {
		let mut chain = chain();
		let root = chain.root();
		let mut path = vec![root];
		let mut latest = block(100, 0);
		for step in 0..8 {
			let parent = *path.last().unwrap();
			let next = if pattern & (1 << step) == 0 {
				append_timeout(&mut chain, parent)
			} else {
				latest = block(latest.number + 1, step as u8 + 1);
				append_block(&mut chain, parent, latest.number, step as u8 + 1)
			};
			assert_eq!(next.view, root.view + step + 1);
			path.push(next);
		}
		let tip = *path.last().unwrap();
		for (index, ancestor) in path.iter().take(8).enumerate() {
			let expected: Vec<_> = path[index + 1..8].iter().rev().map(|x| x.id).collect();
			assert_eq!(chain.ancestry(ancestor.id, tip.id).unwrap(), expected);
		}

		let mut round = round(root);
		for voter in 0..3 {
			round
				.import_prevote(&chain, Prevote::new(tip.id, tip.view), voter, voter)
				.unwrap();
			round
				.import_precommit(&chain, Precommit::new(tip.id, tip.view), voter, voter)
				.unwrap();
		}
		assert_eq!(round.state().prevote_ghost, Some((tip.id, tip.view)));
		assert_eq!(round.state().finalized, Some((tip.id, tip.view)));
		assert_eq!(round.state().estimate, Some((tip.id, tip.view)));
		assert!(round.state().completable);
		let finalized = chain.finalize(tip).unwrap();
		assert_eq!(finalized.block, latest);
		assert_eq!(finalized.newly_finalized.len(), 8);
		assert_eq!(finalized.block_changed, pattern != 0);
	}
}

#[cfg(feature = "derive-codec")]
#[test]
fn catch_up_codec_preserves_timeout_base_and_vote_targets() {
	use crate::{CatchUp, SignedPrevote};
	use parity_scale_codec::{Decode, Encode};

	let mut chain = chain();
	let root = chain.root();
	let timeout = append_timeout(&mut chain, root);
	chain.finalize(timeout).unwrap();
	let descendant = append_block(&mut chain, timeout, 101, 1);
	let catch_up = CatchUp {
		round_number: 17,
		base_hash: timeout.id,
		base_number: timeout.view,
		prevotes: (0..3)
			.map(|id| SignedPrevote {
				prevote: Prevote::new(descendant.id, descendant.view),
				signature: id,
				id,
			})
			.collect(),
		precommits: (0..3).map(|id| signed_precommit(descendant, id)).collect(),
	};
	let encoded = catch_up.encode();
	let decoded = CatchUp::<TargetId, u64, u64, u64>::decode(&mut &encoded[..]).unwrap();
	assert_eq!(decoded, catch_up);

	let mut round = round(TargetRef { id: decoded.base_hash, view: decoded.base_number });
	for vote in decoded.prevotes {
		round.import_prevote(&chain, vote.prevote, vote.id, vote.signature).unwrap();
	}
	for vote in decoded.precommits {
		round.import_precommit(&chain, vote.precommit, vote.id, vote.signature).unwrap();
	}
	assert_eq!(round.state().finalized, Some((descendant.id, descendant.view)));
	assert!(round.state().completable);
}

mod end_to_end {
	use super::*;
	use crate::{
		round::State,
		testing::view_chain::{Finalization, ViewError},
		voter::{CommunicationIn, CommunicationOut, Environment, RoundData, Voter},
		Equivocation, HistoricalVotes, Message, PrimaryPropose, SignedMessage, VoteTarget,
	};
	use futures::{
		channel::mpsc,
		future::{self, BoxFuture, Ready},
		Future, Sink, SinkExt,
	};
	use parking_lot::Mutex;
	use std::{
		collections::VecDeque,
		fmt,
		pin::Pin,
		sync::Arc,
		task::{Context, Poll},
	};

	#[derive(Debug)]
	struct HostError(String);

	impl fmt::Display for HostError {
		fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
			f.write_str(&self.0)
		}
	}
	impl std::error::Error for HostError {}
	impl From<crate::Error> for HostError {
		fn from(error: crate::Error) -> Self {
			Self(format!("{error:?}"))
		}
	}
	impl From<ViewError> for HostError {
		fn from(error: ViewError) -> Self {
			Self(error.to_string())
		}
	}

	type WireMessage = SignedMessage<TargetId, u64, u64, u64>;
	type EmittedVotes = Arc<Mutex<Vec<(u64, Message<TargetId, u64>)>>>;

	struct Loopback {
		round: u64,
		sender: mpsc::UnboundedSender<Result<WireMessage, HostError>>,
		emitted: EmittedVotes,
	}

	impl Sink<Message<TargetId, u64>> for Loopback {
		type Error = HostError;

		fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
			Poll::Ready(Ok(()))
		}

		fn start_send(
			self: Pin<&mut Self>,
			message: Message<TargetId, u64>,
		) -> Result<(), Self::Error> {
			self.emitted.lock().push((self.round, message.clone()));
			self.sender
				.unbounded_send(Ok(SignedMessage { message, signature: 0, id: 0 }))
				.map_err(|_| HostError("round receiver dropped".into()))
		}

		fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
			Poll::Ready(Ok(()))
		}

		fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
			Poll::Ready(Ok(()))
		}
	}

	struct Host {
		chain: Mutex<TestChain>,
		candidates: Mutex<VecDeque<ConsensusTarget<[u8; 32], u64>>>,
		finalizations: Mutex<Vec<(u64, Finalization<[u8; 32], u64>)>>,
		durable: Mutex<Option<SavedFinality>>,
		fail_persistence: bool,
		emitted: EmittedVotes,
	}

	struct SavedFinality {
		target: TargetRef,
		round: u64,
		commit: Commit<TargetId, u64, u64, u64>,
	}

	impl Chain<TargetId, u64> for Host {
		fn vote_target(&self, hash: TargetId) -> Option<VoteTarget<TargetId, u64>> {
			self.chain.lock().vote_target(hash)
		}

		fn ancestry(
			&self,
			base: TargetId,
			target: TargetId,
		) -> Result<Vec<TargetId>, crate::Error> {
			self.chain.lock().ancestry(base, target)
		}

		fn is_equal_or_descendent_of(&self, base: TargetId, target: TargetId) -> bool {
			self.chain.lock().is_equal_or_descendent_of(base, target)
		}
	}

	impl Environment<TargetId, u64> for Host {
		type Timer = Ready<Result<(), HostError>>;
		type BestChain = BoxFuture<'static, Result<Option<VoteTarget<TargetId, u64>>, HostError>>;
		type Id = u64;
		type Signature = u64;
		type In = mpsc::UnboundedReceiver<Result<WireMessage, HostError>>;
		type Out = Loopback;
		type Error = HostError;

		fn best_chain_containing(&self, base: TargetId) -> Self::BestChain {
			match self.candidates.lock().pop_front() {
				Some(candidate) => {
					// Fixtures stand in for application-validated block and timeout data.
					// The host stores the payload before returning its typed vote target.
					let mut chain = self.chain.lock();
					let result =
						chain.import(candidate).map_err(HostError::from).and_then(|target| {
							if !chain.is_equal_or_descendent_of(base, target.id) {
								return Err(crate::Error::NotDescendent.into());
							}
							Ok(chain.vote_target(target.id))
						});
					Box::pin(future::ready(result))
				},
				None => Box::pin(future::pending()),
			}
		}

		fn round_data(&self, round: u64) -> RoundData<Self::Id, Self::Timer, Self::In, Self::Out> {
			let (sender, incoming) = mpsc::unbounded();
			RoundData {
				voter_id: Some(0),
				prevote_timer: future::ready(Ok(())),
				precommit_timer: future::ready(Ok(())),
				incoming,
				outgoing: Loopback { round, sender, emitted: self.emitted.clone() },
			}
		}

		fn round_commit_timer(&self) -> Self::Timer {
			future::ready(Ok(()))
		}
		fn proposed(&self, _: u64, _: PrimaryPropose<TargetId, u64>) -> Result<(), Self::Error> {
			Ok(())
		}
		fn prevoted(&self, _: u64, _: Prevote<TargetId, u64>) -> Result<(), Self::Error> {
			Ok(())
		}
		fn precommitted(&self, _: u64, _: Precommit<TargetId, u64>) -> Result<(), Self::Error> {
			Ok(())
		}
		fn completed(
			&self,
			_: u64,
			_: State<TargetId, u64>,
			_: (TargetId, u64),
			_: &HistoricalVotes<TargetId, u64, u64, u64>,
		) -> Result<(), Self::Error> {
			Ok(())
		}
		fn concluded(
			&self,
			_: u64,
			_: State<TargetId, u64>,
			_: (TargetId, u64),
			_: &HistoricalVotes<TargetId, u64, u64, u64>,
		) -> Result<(), Self::Error> {
			Ok(())
		}

		fn finalize_target(
			&self,
			hash: TargetId,
			view: u64,
			round: u64,
			commit: Commit<TargetId, u64, u64, u64>,
		) -> Result<(), Self::Error> {
			let mut chain = self.chain.lock();
			let target = TargetRef { id: hash, view };
			let finalization = chain.preview_finalization(target)?;
			assert_eq!((commit.target_hash, commit.target_number), finalization.target.as_pair());
			assert_eq!(commit.precommits.len(), 1);
			if self.fail_persistence {
				return Err(HostError("persistence failure".into()));
			}
			// Simulate the host accepting the finalization callback before applying
			// it to the target tree; storage format is not part of the core API.
			*self.durable.lock() = Some(SavedFinality { target, round, commit });
			self.finalizations.lock().push((round, finalization.clone()));
			chain.finalize(target)?;
			Ok(())
		}

		fn prevote_equivocation(&self, _: u64, _: Equivocation<u64, Prevote<TargetId, u64>, u64>) {
			panic!("the fixture should never double-prevote")
		}
		fn precommit_equivocation(
			&self,
			_: u64,
			_: Equivocation<u64, Precommit<TargetId, u64>, u64>,
		) {
			panic!("the fixture should never double-precommit")
		}
	}

	#[test]
	fn real_voter_finalizes_block_then_timeout_at_the_same_real_height() {
		let chain = chain();
		let root = chain.root();
		let first_block = chain.block(root.id, block(101, 1)).unwrap();
		let mut expected_chain = chain.clone();
		let block_ref = expected_chain.import(first_block.clone()).unwrap();
		let timeout = ConsensusTarget {
			parent: Some(block_ref.id),
			view: block_ref.view + 1,
			kind: TargetKind::ViewTimeout,
		};
		let timeout_ref = expected_chain.import(timeout.clone()).unwrap();
		let host = Arc::new(Host {
			chain: Mutex::new(chain),
			candidates: Mutex::new(VecDeque::from(vec![first_block, timeout])),
			finalizations: Mutex::new(Vec::new()),
			durable: Mutex::new(None),
			fail_persistence: false,
			emitted: Arc::new(Mutex::new(Vec::new())),
		});
		let global_in = futures::stream::pending::<
			Result<CommunicationIn<TargetId, u64, u64, u64>, HostError>,
		>();
		let global_out = futures::sink::drain::<CommunicationOut<TargetId, u64, u64, u64>>()
			.sink_map_err(|_| HostError("unexpected drain failure".into()));
		let mut voter = Voter::new(
			host.clone(),
			VoterSet::new([(0, 1)]).unwrap(),
			(global_in, global_out),
			0,
			Vec::new(),
			root.as_pair(),
			root.as_pair(),
		);
		let mut cx = Context::from_waker(futures::task::noop_waker_ref());
		for _ in 0..32 {
			let polled = Pin::new(&mut voter).poll(&mut cx);
			assert!(polled.is_pending(), "voter unexpectedly terminated: {polled:?}");
			if host.finalizations.lock().len() == 2 {
				break;
			}
		}

		let finalizations = host.finalizations.lock();
		assert_eq!(finalizations.len(), 2, "both consensus views must notify the host");
		assert_eq!(finalizations[0].0, 1);
		assert_eq!(finalizations[0].1.target, block_ref);
		assert_eq!(finalizations[0].1.block, block(101, 1));
		assert!(finalizations[0].1.block_changed);
		assert_eq!(finalizations[1].0, 2);
		assert_eq!(finalizations[1].1.target, timeout_ref);
		assert_eq!(finalizations[1].1.block, block(101, 1));
		assert!(!finalizations[1].1.block_changed);
		drop(finalizations);

		assert_eq!(host.chain.lock().finalized_target(), timeout_ref);
		assert_eq!(host.chain.lock().finalized_block(), &block(101, 1));
		assert_eq!(
			host.vote_target(block_ref.id),
			Some(VoteTarget::Block(block_ref.id, block_ref.view))
		);
		assert_eq!(
			host.vote_target(timeout_ref.id),
			Some(VoteTarget::ViewTimeout(timeout_ref.id, timeout_ref.view))
		);
		let durable = host.durable.lock();
		let durable =
			durable.as_ref().expect("finalization callback persisted the target and proof");
		assert_eq!(durable.round, 2);
		assert_eq!(durable.target, timeout_ref);
		assert!(validate_commit(
			&durable.commit,
			&VoterSet::new([(0, 1)]).unwrap(),
			&*host.chain.lock()
		)
		.unwrap()
		.is_valid());
		let emitted = host.emitted.lock();
		for (round, target) in [(1, block_ref), (2, timeout_ref)] {
			let prevotes = emitted
				.iter()
				.filter(|(r, message)| {
					*r == round &&
						matches!(
							message, Message::Prevote(vote) if (vote.target_hash, vote.target_number) == target.as_pair()
						)
				})
				.count();
			let precommits = emitted.iter().filter(|(r, message)| *r == round && matches!(
				message, Message::Precommit(vote) if (vote.target_hash, vote.target_number) == target.as_pair()
			)).count();
			assert_eq!((prevotes, precommits), (1, 1));
		}
	}

	#[test]
	fn real_voter_does_not_advance_host_finality_when_persistence_fails() {
		let chain = chain();
		let root = chain.root();
		let candidate = chain.block(root.id, block(101, 1)).unwrap();
		let candidate_ref = chain.clone().import(candidate.clone()).unwrap();
		let host = Arc::new(Host {
			chain: Mutex::new(chain),
			candidates: Mutex::new(VecDeque::from(vec![candidate])),
			finalizations: Mutex::new(Vec::new()),
			durable: Mutex::new(None),
			fail_persistence: true,
			emitted: Arc::new(Mutex::new(Vec::new())),
		});
		let global_in = futures::stream::pending::<
			Result<CommunicationIn<TargetId, u64, u64, u64>, HostError>,
		>();
		let global_out = futures::sink::drain::<CommunicationOut<TargetId, u64, u64, u64>>()
			.sink_map_err(|_| HostError("unexpected drain failure".into()));
		let mut voter = Voter::new(
			host.clone(),
			VoterSet::new([(0, 1)]).unwrap(),
			(global_in, global_out),
			0,
			Vec::new(),
			root.as_pair(),
			root.as_pair(),
		);
		let mut cx = Context::from_waker(futures::task::noop_waker_ref());
		let mut saw_persistence_failure = false;
		for _ in 0..32 {
			match Pin::new(&mut voter).poll(&mut cx) {
				Poll::Pending => {},
				Poll::Ready(Ok(())) => panic!("the voter must propagate the persistence error"),
				Poll::Ready(Err(error)) => {
					assert_eq!(error.0, "persistence failure");
					saw_persistence_failure = true;
					break;
				},
			}
		}
		assert!(saw_persistence_failure, "the real voter must reach the finalization callback");
		assert_eq!(host.chain.lock().finalized_target(), root);
		assert_eq!(host.chain.lock().finalized_block(), &block(100, 0));
		assert!(host.durable.lock().is_none());
		assert!(host.finalizations.lock().is_empty());
		// Admission succeeded; only finality and its durable journal stayed put.
		assert_eq!(
			host.vote_target(candidate_ref.id),
			Some(VoteTarget::Block(candidate_ref.id, candidate_ref.view)),
		);
	}
}
