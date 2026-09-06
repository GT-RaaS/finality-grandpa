// Copyright 2018-2019 Parity Technologies (UK) Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Logic for voting and handling messages within a single round.

#[cfg(feature = "std")]
use futures::ready;
use futures::{channel::mpsc::UnboundedSender, prelude::*};
#[cfg(feature = "std")]
use log::{debug, trace, warn};

use std::{
	pin::Pin,
	sync::Arc,
	task::{Context, Poll},
};

use super::{Buffered, Environment, FinalizedNotification};
use crate::{
	round::{Round, State as RoundState},
	validate_commit,
	voter_set::VoterSet,
	weights::VoteWeight,
	BlockNumberOps, Commit, HistoricalVotes, ImportResult, Message, Precommit, Prevote,
	PrimaryPropose, SignedMessage, SignedPrecommit, LOG_TARGET,
};

/// The state of a voting round.
pub(super) enum State<T, W> {
	Start(T, T),
	Proposed(T, T),
	Prevoting(T, W),
	Prevoted(T),
	Precommitted,
}

impl<T, W> std::fmt::Debug for State<T, W> {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		match self {
			State::Start(..) => write!(f, "Start"),
			State::Proposed(..) => write!(f, "Proposed"),
			State::Prevoting(..) => write!(f, "Prevoting"),
			State::Prevoted(_) => write!(f, "Prevoted"),
			State::Precommitted => write!(f, "Precommitted"),
		}
	}
}

/// Logic for a voter on a specific round.
pub(super) struct VotingRound<H, N, E: Environment<H, N>>
where
	H: Clone + Eq + Ord + ::std::fmt::Debug,
	N: Copy + BlockNumberOps + ::std::fmt::Debug,
{
	env: Arc<E>,
	voting: Voting,
	votes: Round<E::Id, H, N, E::Signature>,
	incoming: E::In,
	outgoing: Buffered<E::Out, Message<H, N>>,
	state: Option<State<E::Timer, (H, (H, N), E::BestChain)>>, // state machine driving votes.
	latest_finalized: (H, N), // includes finalized timeout views, not just real blocks.
	bridged_round_state: Option<crate::bridge_state::PriorView<H, N>>, // updates to later round
	last_round_state: Option<crate::bridge_state::LatterView<H, N>>, // updates from prior round
	primary_block: Option<(H, N)>, // a block posted by primary as a hint.
	finalized_sender: UnboundedSender<FinalizedNotification<H, N, E>>,
	best_finalized: Option<Commit<H, N, E::Signature, E::Id>>,
}

/// Whether we should vote in the current round (i.e. push votes to the sink.)
enum Voting {
	/// Voting is disabled for the current round.
	No,
	/// Voting is enabled for the current round (prevotes and precommits.)
	Yes,
	/// Voting is enabled for the current round and we are the primary proposer
	/// (we can also push primary propose messages).
	Primary,
}

impl Voting {
	/// Whether the voter should cast round votes (prevotes and precommits.)
	fn is_active(&self) -> bool {
		matches!(self, Voting::Yes | Voting::Primary)
	}

	/// Whether the voter is the primary proposer.
	fn is_primary(&self) -> bool {
		matches!(self, Voting::Primary)
	}
}

impl<H, N, E: Environment<H, N>> VotingRound<H, N, E>
where
	H: Clone + Eq + Ord + ::std::fmt::Debug,
	N: Copy + BlockNumberOps + ::std::fmt::Debug,
{
	/// Create a new voting round.
	pub(super) fn new(
		round_number: u64,
		voters: VoterSet<E::Id>,
		base: (H, N),
		last_round_state: Option<crate::bridge_state::LatterView<H, N>>,
		finalized_sender: UnboundedSender<FinalizedNotification<H, N, E>>,
		env: Arc<E>,
	) -> VotingRound<H, N, E> {
		let round_data = env.round_data(round_number);
		let latest_finalized = base.clone();
		let round_params = crate::round::RoundParams { voters, base, round_number };

		let votes = Round::new(round_params);

		let voting = if round_data.voter_id.as_ref() == Some(votes.primary_voter().0) {
			Voting::Primary
		} else if round_data.voter_id.as_ref().is_some_and(|id| votes.voters().contains(id)) {
			Voting::Yes
		} else {
			Voting::No
		};

		VotingRound {
			votes,
			voting,
			incoming: round_data.incoming,
			outgoing: Buffered::new(round_data.outgoing),
			state: Some(State::Start(round_data.prevote_timer, round_data.precommit_timer)),
			latest_finalized,
			bridged_round_state: None,
			primary_block: None,
			best_finalized: None,
			env,
			last_round_state,
			finalized_sender,
		}
	}

	/// Create a voting round from a completed `Round`. We will not vote further
	/// in this round.
	pub(super) fn completed(
		votes: Round<E::Id, H, N, E::Signature>,
		finalized_sender: UnboundedSender<FinalizedNotification<H, N, E>>,
		last_round_state: Option<crate::bridge_state::LatterView<H, N>>,
		env: Arc<E>,
	) -> VotingRound<H, N, E> {
		let round_data = env.round_data(votes.number());

		let latest_finalized = votes.base();
		VotingRound {
			votes,
			voting: Voting::No,
			incoming: round_data.incoming,
			outgoing: Buffered::new(round_data.outgoing),
			state: None,
			latest_finalized,
			bridged_round_state: None,
			primary_block: None,
			env,
			last_round_state,
			finalized_sender,
			best_finalized: None,
		}
	}

	/// Supply the latest target durably finalized by the outer voter.
	pub(super) fn note_finalized(&mut self, finalized: (H, N)) {
		if finalized.1 > self.latest_finalized.1 {
			self.latest_finalized = finalized;
		}
	}

	/// Stop creating local votes when a verified catch-up skips this round.
	///
	/// Dropping the state also cancels any pending best-chain query or timer.
	/// Keep the graph, historical votes and network channels so the background
	/// round can still receive votes and process existing finality proofs.
	pub(super) fn stop_voting(&mut self) {
		self.voting = Voting::No;
		self.state = None;
	}

	/// Poll the round. When the round is completable and messages have been flushed, it will return `Poll::Ready` but
	/// can continue to be polled.
	pub(super) fn poll(&mut self, cx: &mut Context) -> Poll<Result<(), E::Error>> {
		trace!(
			target: LOG_TARGET,
			"Polling round {}, state = {:?}, step = {:?}",
			self.votes.number(),
			self.votes.state(),
			self.state
		);

		let pre_state = self.votes.state();
		self.process_incoming(cx)?;

		// we only cast votes when we have access to the previous round state.
		// we might have started this round as a prospect "future" round to
		// check whether the voter is lagging behind the current round.
		let last_round_state = self.last_round_state.as_ref().map(|s| s.get(cx).clone());
		if let Some(ref last_round_state) = last_round_state {
			self.primary_propose(last_round_state)?;
			self.prevote(cx, last_round_state)?;
			self.precommit(cx, last_round_state)?;
		}

		ready!(self.outgoing.poll(cx))?;
		self.process_incoming(cx)?; // in case we got a new message signed locally.

		// broadcast finality notifications after attempting to cast votes
		let post_state = self.votes.state();
		self.notify(pre_state, post_state);

		// early exit if the current round is not completable
		if !self.votes.completable() {
			return Poll::Pending;
		}

		// make sure that the previous round estimate has been finalized
		let last_round_estimate_finalized = match last_round_state {
			Some(RoundState {
				estimate: Some((_, last_round_estimate)),
				finalized: Some((_, last_round_finalized)),
				..
			}) => {
				// either it was already finalized in the previous round
				let finalized_in_last_round = last_round_estimate <= last_round_finalized;

				// or it must be finalized in the current round
				let finalized_in_current_round =
					self.finalized().is_some_and(|(_, current_round_finalized)| {
						last_round_estimate <= *current_round_finalized
					});

				finalized_in_last_round || finalized_in_current_round
			},
			None => {
				// NOTE: when we catch up to a round we complete the round
				// without any last round state. in this case we already started
				// a new round after we caught up so this guard is unneeded.
				true
			},
			_ => false,
		};

		// the previous round estimate must be finalized
		if !last_round_estimate_finalized {
			trace!(
				target: LOG_TARGET,
				"Round {} completable but estimate not finalized.",
				self.round_number()
			);
			self.log_participation(log::Level::Trace);
			return Poll::Pending;
		}

		debug!(
			target: LOG_TARGET,
			"Completed round {}, state = {:?}, step = {:?}",
			self.votes.number(),
			self.votes.state(),
			self.state
		);

		self.log_participation(log::Level::Debug);

		// both exit conditions verified, we can complete this round
		Poll::Ready(Ok(()))
	}

	/// Inspect the state of this round.
	pub(super) fn state(&self) -> Option<&State<E::Timer, (H, (H, N), E::BestChain)>> {
		self.state.as_ref()
	}

	/// Get access to the underlying environment.
	pub(super) fn env(&self) -> &E {
		&self.env
	}

	/// Get the round number.
	pub(super) fn round_number(&self) -> u64 {
		self.votes.number()
	}

	/// Get the round state.
	pub(super) fn round_state(&self) -> RoundState<H, N> {
		self.votes.state()
	}

	/// Get the base block in the dag.
	pub(super) fn dag_base(&self) -> (H, N) {
		self.votes.base()
	}

	/// Get the voters in this round.
	pub(super) fn voters(&self) -> &VoterSet<E::Id> {
		self.votes.voters()
	}

	/// Get the best block finalized in this round.
	pub(super) fn finalized(&self) -> Option<&(H, N)> {
		self.votes.finalized()
	}

	/// Get the current total weight of prevotes.
	pub(super) fn prevote_weight(&self) -> VoteWeight {
		self.votes.prevote_participation().0
	}

	/// Get the current total weight of precommits.
	pub(super) fn precommit_weight(&self) -> VoteWeight {
		self.votes.precommit_participation().0
	}

	/// Get the Ids of the prevoters.
	pub(super) fn prevote_ids(&self) -> impl Iterator<Item = E::Id> {
		self.votes.prevotes().into_iter().map(|pv| pv.0)
	}

	/// Get the Ids of the precommitters.
	pub(super) fn precommit_ids(&self) -> impl Iterator<Item = E::Id> {
		self.votes.precommits().into_iter().map(|pv| pv.0)
	}

	/// Check a commit. If it's valid, import all the votes into the round as well.
	/// Returns the finalized base if it checks out.
	pub(super) fn check_and_import_from_commit(
		&mut self,
		commit: &Commit<H, N, E::Signature, E::Id>,
	) -> Result<Option<(H, N)>, E::Error> {
		if !validate_commit(commit, self.voters(), &*self.env)?.is_valid() {
			return Ok(None);
		}

		for SignedPrecommit { precommit, signature, id } in commit.precommits.iter().cloned() {
			let import_result =
				self.votes.import_precommit(&*self.env, precommit, id, signature)?;
			if let ImportResult { equivocation: Some(e), .. } = import_result {
				self.env.precommit_equivocation(self.round_number(), e);
			}
		}

		Ok(Some((commit.target_hash.clone(), commit.target_number)))
	}

	/// Get a clone of the finalized sender.
	pub(super) fn finalized_sender(&self) -> UnboundedSender<FinalizedNotification<H, N, E>> {
		self.finalized_sender.clone()
	}

	// call this when we build on top of a given round in order to get a handle
	// to updates to the latest round-state.
	pub(super) fn bridge_state(&mut self) -> crate::bridge_state::LatterView<H, N> {
		let (prior_view, latter_view) = crate::bridge_state::bridge_state(self.votes.state());
		if self.bridged_round_state.is_some() {
			warn!(
				target: LOG_TARGET,
				"Bridged state from round {} more than once.",
				self.votes.number()
			);
		}

		self.bridged_round_state = Some(prior_view);
		latter_view
	}

	/// Get a commit justifying the best finalized block.
	pub(super) fn finalizing_commit(&self) -> Option<&Commit<H, N, E::Signature, E::Id>> {
		self.best_finalized.as_ref()
	}

	/// Return all votes for the round (prevotes and precommits), sorted by
	/// imported order and indicating the indices where we voted. At most two
	/// prevotes and two precommits per voter are present, further equivocations
	/// are not stored (as they are redundant).
	pub(super) fn historical_votes(&self) -> &HistoricalVotes<H, N, E::Signature, E::Id> {
		self.votes.historical_votes()
	}

	/// Handle a vote manually.
	pub(super) fn handle_vote(
		&mut self,
		vote: SignedMessage<H, N, E::Signature, E::Id>,
	) -> Result<(), E::Error> {
		let SignedMessage { message, signature, id } = vote;
		if !super::target_is_known(&*self.env, message.target().0, message.target().1) {
			trace!(
				target: LOG_TARGET,
				"Ignoring message for unknown or incorrectly numbered consensus target {:?}",
				message.target(),
			);
			return Ok(());
		}
		if !self
			.env
			.is_equal_or_descendent_of(self.votes.base().0, message.target().0.clone())
		{
			trace!(
				target: LOG_TARGET,
				"Ignoring message targeting {:?} lower than round base {:?}",
				message.target(),
				self.votes.base(),
			);
			return Ok(());
		}

		match message {
			Message::Prevote(prevote) => {
				let import_result =
					self.votes.import_prevote(&*self.env, prevote, id, signature)?;
				if let ImportResult { equivocation: Some(e), .. } = import_result {
					self.env.prevote_equivocation(self.votes.number(), e);
				}
			},
			Message::Precommit(precommit) => {
				let import_result =
					self.votes.import_precommit(&*self.env, precommit, id, signature)?;
				if let ImportResult { equivocation: Some(e), .. } = import_result {
					self.env.precommit_equivocation(self.votes.number(), e);
				}
			},
			Message::PrimaryPropose(primary) => {
				let primary_id = self.votes.primary_voter().0.clone();
				// note that id here refers to the party which has cast the vote
				// and not the id of the party which has received the vote message.
				if id == primary_id {
					self.primary_block = Some((primary.target_hash, primary.target_number));
				}
			},
		}

		Ok(())
	}

	fn log_participation(&self, log_level: log::Level) {
		let total_weight = self.voters().total_weight();
		let threshold = self.voters().threshold();
		let n_voters = self.voters().len();
		let number = self.round_number();

		let (prevote_weight, n_prevotes) = self.votes.prevote_participation();
		let (precommit_weight, n_precommits) = self.votes.precommit_participation();

		log::log!(
			target: LOG_TARGET,
			log_level,
			"Round {}: prevotes: {}/{}/{} weight, {}/{} actual",
			number,
			prevote_weight,
			threshold,
			total_weight,
			n_prevotes,
			n_voters
		);

		log::log!(
			target: LOG_TARGET,
			log_level,
			"Round {}: precommits: {}/{}/{} weight, {}/{} actual",
			number,
			precommit_weight,
			threshold,
			total_weight,
			n_precommits,
			n_voters
		);
	}

	fn process_incoming(&mut self, cx: &mut Context) -> Result<(), E::Error> {
		while let Poll::Ready(Some(incoming)) = Stream::poll_next(Pin::new(&mut self.incoming), cx)
		{
			trace!(target: LOG_TARGET, "Round {}: Got incoming message", self.round_number());
			self.handle_vote(incoming?)?;
		}

		Ok(())
	}

	fn primary_propose(&mut self, last_round_state: &RoundState<H, N>) -> Result<(), E::Error> {
		match self.state.take() {
			Some(State::Start(prevote_timer, precommit_timer)) => {
				let maybe_estimate = last_round_state.estimate.clone();

				match (maybe_estimate, self.voting.is_primary()) {
					(Some(last_round_estimate), true) => {
						let maybe_finalized = last_round_state.finalized.clone();

						// Last round estimate has not been finalized.
						let should_send_primary =
							maybe_finalized.map_or(true, |f| last_round_estimate.1 > f.1);
						if should_send_primary {
							debug!(
								target: LOG_TARGET,
								"Sending primary block hint for round {}",
								self.votes.number()
							);
							let primary = PrimaryPropose {
								target_hash: last_round_estimate.0,
								target_number: last_round_estimate.1,
							};
							self.env.proposed(self.round_number(), primary.clone())?;
							self.outgoing.push(Message::PrimaryPropose(primary));
							self.state = Some(State::Proposed(prevote_timer, precommit_timer));

							return Ok(());
						} else {
							debug!(
								target: LOG_TARGET,
								"Last round estimate has been finalized, \
								not sending primary block hint for round {}",
								self.votes.number()
							);
						}
					},
					(None, true) => {
						debug!(
							target: LOG_TARGET,
							"Last round estimate does not exist, \
							not sending primary block hint for round {}",
							self.votes.number()
						);
					},
					_ => {},
				}

				self.state = Some(State::Start(prevote_timer, precommit_timer));
			},
			x => {
				self.state = x;
			},
		}

		Ok(())
	}

	fn prevote(
		&mut self,
		cx: &mut Context,
		last_round_state: &RoundState<H, N>,
	) -> Result<(), E::Error> {
		let state = self.state.take();

		let start_prevoting = |this: &mut Self,
		                       mut prevote_timer: E::Timer,
		                       precommit_timer: E::Timer,
		                       proposed: bool,
		                       cx: &mut Context| {
			let should_prevote = match prevote_timer.poll_unpin(cx) {
				Poll::Ready(Err(e)) => return Err(e),
				Poll::Ready(Ok(())) => true,
				Poll::Pending => this.votes.completable(),
			};

			if should_prevote {
				if this.voting.is_active() {
					debug!(
						target: LOG_TARGET,
						"Constructing prevote for round {}",
						this.votes.number()
					);

					let (base, best_chain) = this.construct_prevote(last_round_state);

					// since we haven't polled the future above yet we need to
					// manually schedule the current task to be awoken so the
					// `best_chain` future is then polled below after we switch the
					// state to `Prevoting`.
					cx.waker().wake_by_ref();

					this.state = Some(State::Prevoting(
						precommit_timer,
						(base, this.latest_finalized.clone(), best_chain),
					));
				} else {
					this.state = Some(State::Prevoted(precommit_timer));
				}
			} else if proposed {
				this.state = Some(State::Proposed(prevote_timer, precommit_timer));
			} else {
				this.state = Some(State::Start(prevote_timer, precommit_timer));
			}

			Ok(())
		};

		let finish_prevoting = |this: &mut Self,
		                        precommit_timer: E::Timer,
		                        base: H,
		                        observed_finality: (H, N),
		                        mut best_chain: E::BestChain,
		                        cx: &mut Context| {
			let best_chain = match best_chain.poll_unpin(cx) {
				Poll::Ready(Err(e)) => return Err(e),
				Poll::Ready(Ok(best_chain)) => best_chain,
				Poll::Pending => {
					this.state = Some(State::Prevoting(
						precommit_timer,
						(base, observed_finality, best_chain),
					));
					return Ok(());
				},
			};

			if let Some(target) = best_chain {
				let (target_hash, _) = target.target();
				if this.env.vote_target(target_hash.clone()).as_ref() != Some(&target) {
					return Err(crate::Error::InvalidTarget.into());
				}
				if !this.env.is_equal_or_descendent_of(base.clone(), target_hash.clone()) {
					return Err(crate::Error::NotDescendent.into());
				}
				if !this
					.env
					.is_equal_or_descendent_of(this.latest_finalized.0.clone(), target_hash.clone())
				{
					if this.latest_finalized == observed_finality {
						return Err(crate::Error::NotDescendent.into());
					}
					if this.env.is_equal_or_descendent_of(base, this.latest_finalized.0.clone()) {
						// The host query (possibly including a view timer) completed
						// using an old fork-choice snapshot. Requery the new finalized
						// descendant, preserving the initial round's safe anchor.
						let base = this.latest_finalized.0.clone();
						let query = this.env.best_chain_containing(base.clone());
						this.state = Some(State::Prevoting(
							precommit_timer,
							(base, this.latest_finalized.clone(), query),
						));
						cx.waker().wake_by_ref();
					} else {
						this.state = None;
						this.voting = Voting::No;
					}
					return Ok(());
				}
				let (target_hash, target_number) = target.into_target();
				let prevote = Prevote { target_hash, target_number };

				debug!(target: LOG_TARGET, "Casting prevote for round {}", this.votes.number());
				this.env.prevoted(this.round_number(), prevote.clone())?;
				this.votes.set_prevoted_index();
				this.outgoing.push(Message::Prevote(prevote));
				this.state = Some(State::Prevoted(precommit_timer));
			} else {
				// if this block is considered unknown, something has gone wrong.
				// log and handle, but skip casting a vote.
				warn!(
					target: LOG_TARGET,
					"Could not cast prevote: previously known block {:?} has disappeared", base,
				);

				// when we can't construct a prevote, we shouldn't precommit.
				this.state = None;
				this.voting = Voting::No;
			}

			Ok(())
		};

		match state {
			Some(State::Start(prevote_timer, precommit_timer)) => {
				start_prevoting(self, prevote_timer, precommit_timer, false, cx)?;
			},
			Some(State::Proposed(prevote_timer, precommit_timer)) => {
				start_prevoting(self, prevote_timer, precommit_timer, true, cx)?;
			},
			Some(State::Prevoting(precommit_timer, (base, observed_finality, best_chain))) => {
				finish_prevoting(self, precommit_timer, base, observed_finality, best_chain, cx)?;
			},
			x => {
				self.state = x;
			},
		}

		Ok(())
	}

	fn precommit(
		&mut self,
		cx: &mut Context,
		last_round_state: &RoundState<H, N>,
	) -> Result<(), E::Error> {
		match self.state.take() {
			Some(State::Prevoted(mut precommit_timer)) => {
				let last_round_estimate = last_round_state
					.estimate
					.clone()
					.expect("Rounds only started when prior round completable; qed");

				let should_precommit = {
					// we wait for the last round's estimate to be equal to or
					// the ancestor of the current round's p-Ghost before precommitting.
					self.votes.state().prevote_ghost.as_ref().is_some_and(|p_g| {
						p_g == &last_round_estimate ||
							self.env
								.is_equal_or_descendent_of(last_round_estimate.0, p_g.0.clone())
					})
				} && match precommit_timer.poll_unpin(cx) {
					Poll::Ready(Err(e)) => return Err(e),
					Poll::Ready(Ok(())) => true,
					Poll::Pending => self.votes.completable(),
				};

				if should_precommit {
					if self.voting.is_active() {
						debug!(
							target: LOG_TARGET,
							"Casting precommit for round {}",
							self.votes.number()
						);
						let precommit = self.construct_precommit();
						self.env.precommitted(self.round_number(), precommit.clone())?;
						self.votes.set_precommitted_index();
						self.outgoing.push(Message::Precommit(precommit));
					}
					self.state = Some(State::Precommitted);
				} else {
					self.state = Some(State::Prevoted(precommit_timer));
				}
			},
			x => {
				self.state = x;
			},
		}

		Ok(())
	}

	// construct a prevote message based on local state.
	fn construct_prevote(&self, last_round_state: &RoundState<H, N>) -> (H, E::BestChain) {
		let last_round_estimate = last_round_state
			.estimate
			.clone()
			.expect("Rounds only started when prior round completable; qed");

		let find_descendent_of = match self.primary_block {
			None => {
				// vote for best chain containing prior round-estimate.
				last_round_estimate.0
			},
			Some(ref primary_block) => {
				// we will vote for the best chain containing `p_hash` iff
				// the last round's prevote-GHOST included that block and
				// that block is a strict descendent of the last round-estimate that we are
				// aware of.
				let last_prevote_g = last_round_state
					.prevote_ghost
					.clone()
					.expect("Rounds only started when prior round completable; qed");

				// if the blocks are equal, we don't check ancestry.
				if primary_block == &last_prevote_g {
					primary_block.0.clone()
				} else if primary_block.1 >= last_prevote_g.1 {
					last_round_estimate.0
				} else {
					// from this point onwards, the number of the primary-broadcasted
					// block is less than the last prevote-GHOST's number.
					// if the primary block is in the ancestry of p-G we vote for the
					// best chain containing it.
					let &(ref p_hash, p_num) = primary_block;
					match self.env.ancestry(last_round_estimate.0.clone(), last_prevote_g.0.clone())
					{
						Ok(ancestry) => {
							let to_sub = p_num + N::one();

							let offset: usize = if last_prevote_g.1 < to_sub {
								0
							} else {
								(last_prevote_g.1 - to_sub).as_()
							};

							if ancestry.get(offset) == Some(p_hash) {
								p_hash.clone()
							} else {
								last_round_estimate.0
							}
						},
						Err(_) => {
							// This is only possible in case of massive equivocation
							warn!(
								target: LOG_TARGET,
								"Possible case of massive equivocation: \
								last round prevote GHOST: {:?} is not a descendant of last round estimate: {:?}",
								last_prevote_g,
								last_round_estimate,
							);

							last_round_estimate.0
						},
					}
				}
			},
		};

		(find_descendent_of.clone(), self.env.best_chain_containing(find_descendent_of))
	}

	// construct a precommit message based on local state.
	fn construct_precommit(&self) -> Precommit<H, N> {
		let t = match self.votes.state().prevote_ghost {
			Some(target) => target,
			None => self.votes.base(),
		};

		Precommit { target_hash: t.0, target_number: t.1 }
	}

	// notify when new blocks are finalized or when the round-estimate is updated
	fn notify(&mut self, last_state: RoundState<H, N>, new_state: RoundState<H, N>) {
		if last_state != new_state {
			if let Some(ref b) = self.bridged_round_state {
				b.update(new_state.clone());
			}
		}

		// send notification only when the round is completable and we've cast votes.
		// this is a workaround that ensures when we re-instantiate the voter after
		// a shutdown, we never re-create the same round with a base that was finalized
		// in this round or after.
		// we try to notify if either the round state changed or if we haven't
		// sent any notification yet (this is to guard against seeing enough
		// votes to finalize before having precommited)
		let state_changed = last_state.finalized != new_state.finalized;
		let sent_finality_notifications = self.best_finalized.is_some();

		if new_state.completable && (state_changed || !sent_finality_notifications) {
			let precommitted = matches!(self.state, Some(State::Precommitted));
			// we only cast votes when we have access to the previous round state,
			// which won't be the case whenever we catch up to a later round.
			let cant_vote = self.last_round_state.is_none();

			if precommitted || cant_vote {
				if let Some((f_hash, f_number)) = new_state.finalized {
					let commit = Commit {
						target_hash: f_hash.clone(),
						target_number: f_number,
						precommits: self.votes.finalizing_precommits(&*self.env)
							.expect("always returns none if something was finalized; this is checked above; qed")
							.collect(),
					};
					let finalized = (f_hash, f_number, self.votes.number(), commit.clone());
					let _ = self.finalized_sender.unbounded_send(finalized);
					self.best_finalized = Some(commit);
				}
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::{
		testing::{
			chain::GENESIS_HASH,
			environment::{self, Environment as TestEnvironment, Id, Signature},
		},
		VoteTarget,
	};
	use futures::{channel::mpsc, future};

	type TestRound = VotingRound<&'static str, u32, TestEnvironment>;

	fn round() -> (Arc<TestEnvironment>, TestRound) {
		let (network, _routing) = environment::make_network();
		let env = Arc::new(TestEnvironment::new(network, Id(0)));
		env.with_chain(|chain| chain.push_blocks(GENESIS_HASH, &["A", "B", "C"]));
		let (sender, _receiver) = mpsc::unbounded();
		let round = VotingRound::new(
			1,
			VoterSet::new(vec![(Id(0), 1)]).unwrap(),
			(GENESIS_HASH, 1),
			None,
			sender,
			env.clone(),
		);
		(env, round)
	}

	fn set_query(round: &mut TestRound, base: &'static str, target: VoteTarget<&'static str, u32>) {
		round.state = Some(State::Prevoting(
			Box::new(future::ready(Ok(()))),
			(base, (GENESIS_HASH, 1), Box::new(future::ready(Ok(Some(target))))),
		));
	}

	#[test]
	fn local_prevote_rejects_incorrect_target_kind_and_view() {
		for candidate in [VoteTarget::ViewTimeout("A", 2), VoteTarget::Block("A", 99)] {
			let (_, mut round) = round();
			set_query(&mut round, GENESIS_HASH, candidate);
			let waker = futures::task::noop_waker();
			let mut cx = Context::from_waker(&waker);
			assert_eq!(
				round.prevote(&mut cx, &RoundState::genesis((GENESIS_HASH, 1))),
				Err(crate::Error::InvalidTarget)
			);
			assert!(round.outgoing.buffer.is_empty());
		}
	}

	#[test]
	fn primary_and_votes_ignore_unknown_or_forged_target_pairs() {
		let (_, mut round) = round();
		for message in [
			Message::PrimaryPropose(PrimaryPropose::new("A", 99)),
			Message::Prevote(Prevote::new("unknown", 99)),
			Message::Precommit(Precommit::new("A", 99)),
		] {
			round
				.handle_vote(SignedMessage { message, signature: Signature(0), id: Id(0) })
				.unwrap();
		}
		assert_eq!(round.primary_block, None);
		assert_eq!(round.prevote_weight(), VoteWeight(0));
		assert_eq!(round.precommit_weight(), VoteWeight(0));
		round
			.handle_vote(SignedMessage {
				message: Message::PrimaryPropose(PrimaryPropose::new("A", 2)),
				signature: Signature(0),
				id: Id(0),
			})
			.unwrap();
		assert_eq!(round.primary_block, Some(("A", 2)));
	}

	#[test]
	fn delayed_query_restarts_from_new_finality_before_voting() {
		let (env, mut round) = round();
		set_query(&mut round, GENESIS_HASH, VoteTarget::Block("B", 3));
		env.with_chain(|chain| chain.set_last_finalized(("C", 4)));
		round.note_finalized(("C", 4));
		let waker = futures::task::noop_waker();
		let mut cx = Context::from_waker(&waker);
		let previous = RoundState::genesis((GENESIS_HASH, 1));
		round.prevote(&mut cx, &previous).unwrap();
		assert!(round.outgoing.buffer.is_empty());
		assert!(matches!(round.state(), Some(State::Prevoting(_, ("C", ("C", 4), _)))));
		round.prevote(&mut cx, &previous).unwrap();
		assert!(matches!(round.state(), Some(State::Prevoted(_))));
		assert_eq!(round.outgoing.buffer.pop_front(), Some(Message::Prevote(Prevote::new("C", 4))));
		round.prevote(&mut cx, &previous).unwrap();
		assert!(round.outgoing.buffer.is_empty());
	}

	#[test]
	fn delayed_query_stops_voting_if_its_anchor_conflicts_with_new_finality() {
		let (env, mut round) = round();
		env.with_chain(|chain| chain.push_blocks(GENESIS_HASH, &["X", "Y", "Z"]));
		set_query(&mut round, "A", VoteTarget::Block("B", 3));
		round.note_finalized(("Z", 4));
		let waker = futures::task::noop_waker();
		let mut cx = Context::from_waker(&waker);
		round.prevote(&mut cx, &RoundState::genesis((GENESIS_HASH, 1))).unwrap();
		assert!(round.state().is_none());
		assert!(!round.voting.is_active());
		assert!(round.outgoing.buffer.is_empty());
	}

	#[test]
	fn catch_up_cancels_the_skipped_rounds_pending_vote() {
		use crate::{
			voter::{Callback, CommunicationIn, Voter},
			CatchUp, SignedPrevote,
		};
		use futures::channel::oneshot;

		let (env, _) = round();
		let (global_sender, global_in) = mpsc::unbounded();
		let global_out = futures::sink::drain().sink_map_err(|never| match never {});
		let mut voter = Voter::new(
			env,
			VoterSet::new(vec![(Id(0), 1)]).unwrap(),
			(global_in, global_out),
			0,
			Vec::new(),
			(GENESIS_HASH, 1),
			(GENESIS_HASH, 1),
		);
		let (query_sender, query_receiver) = oneshot::channel::<VoteTarget<&'static str, u32>>();
		voter.inner.lock().best_round.state = Some(State::Prevoting(
			Box::new(future::ready(Ok(()))),
			(
				GENESIS_HASH,
				(GENESIS_HASH, 1),
				Box::new(
					query_receiver
						.map(|result| result.map(Some).map_err(|_| crate::Error::InvalidTarget)),
				),
			),
		));
		global_sender
			.unbounded_send(Ok(CommunicationIn::CatchUp(
				CatchUp {
					round_number: 2,
					base_hash: GENESIS_HASH,
					base_number: 1,
					prevotes: vec![SignedPrevote {
						prevote: Prevote::new("C", 4),
						signature: Signature(0),
						id: Id(0),
					}],
					precommits: vec![SignedPrecommit {
						precommit: Precommit::new("C", 4),
						signature: Signature(0),
						id: Id(0),
					}],
				},
				Callback::Blank,
			)))
			.unwrap();
		let waker = futures::task::noop_waker();
		let mut cx = Context::from_waker(&waker);
		voter.process_incoming(&mut cx).unwrap();
		assert!(query_sender.is_canceled());
		{
			let inner = voter.inner.lock();
			assert_eq!(inner.best_round.round_number(), 3);
			let skipped = inner
				.past_rounds
				.voting_rounds()
				.find(|round| round.round_number() == 1)
				.unwrap();
			assert!(skipped.state().is_none());
			assert!(!skipped.voting.is_active());
			assert!(skipped.outgoing.buffer.is_empty());
		}
		// Actually poll the background round: the dropped query cannot produce
		// a cached target even though this path does not receive note_finalized.
		voter.prune_background_rounds(&mut cx).unwrap();
		let inner = voter.inner.lock();
		assert!(inner
			.past_rounds
			.voting_rounds()
			.filter(|round| round.round_number() == 1)
			.all(|skipped| skipped.outgoing.buffer.is_empty()));
	}
}
