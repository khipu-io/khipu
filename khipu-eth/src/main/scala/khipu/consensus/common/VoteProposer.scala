package khipu.consensus.common

import java.util.function.IntUnaryOperator
import java.util.ArrayList
import java.util.Collection
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import khipu.domain.Address
import scala.collection.JavaConversions._

/** Container for pending votes and selecting a vote for new blocks */
class VoteProposer {

  private val proposals: Map[Address, VoteType] = new ConcurrentHashMap[Address, VoteType]()
  private val votePosition = new AtomicInteger(0)

  /**
   * Identifies an address that should be voted into the validator pool
   *
   * @param address The address to be voted in
   */
  def auth(address: Address) {
    proposals.put(address, VoteType.ADD)
  }

  /**
   * Identifies an address that should be voted out of the validator pool
   *
   * @param address The address to be voted out
   */
  def drop(address: Address) {
    proposals.put(address, VoteType.DROP)
  }

  /**
   * Discards a pending vote for an address if one exists
   *
   * @param address The address that should no longer be voted for
   */
  def discard(address: Address) {
    proposals.remove(address)
  }

  def getProposals = proposals

  def get(address: Address): Option[VoteType] = {
    Option(proposals.get(address))
  }

  private def voteNotYetCast(
    localAddress: Address,
    voteAddress:  Address,
    vote:         VoteType,
    validators:   Collection[Address],
    tally:        VoteTally
  ): Boolean = {

    // Pre evaluate if we have a vote outstanding to auth or drop the target address
    val votedAuth = tally.getOutstandingAddVotesFor(voteAddress).contains(localAddress)
    val votedDrop = tally.getOutstandingRemoveVotesFor(voteAddress).contains(localAddress)

    // if they're a validator, we want to see them dropped, and we haven't voted to drop them yet
    if (validators.contains(voteAddress) && !votedDrop && vote == VoteType.DROP) {
      return true
      // or if we've previously voted to auth them and we want to drop them
    } else if (votedAuth && vote == VoteType.DROP) {
      return true
      // if they're not currently a validator and we want to see them authed and we haven't voted to
      // auth them yet
    } else if (!validators.contains(voteAddress) && !votedAuth && vote == VoteType.ADD) {
      return true
      // or if we've previously voted to drop them and we want to see them authed
    } else if (votedDrop && vote == VoteType.ADD) {
      return true
    }

    return false
  }

  /**
   * Gets a valid vote from our list of pending votes
   *
   * @param localAddress The address of this validator node
   * @param tally the vote tally at the height of the chain we need a vote for
   * @return Either an address with the vote (auth or drop) or no vote if we have no valid pending
   *     votes
   */
  def getVote(localAddress: Address, tally: VoteTally): Option[ValidatorVote] = {
    val validators = tally.getValidators
    val validVotes = new ArrayList[Map.Entry[Address, VoteType]]()

    proposals.entrySet().foreach { proposal =>
      if (voteNotYetCast(localAddress, proposal.getKey, proposal.getValue, validators, tally)) {
        validVotes.add(proposal)
      }
    }

    if (validVotes.isEmpty) {
      return None
    }

    // Get the next position in the voting queue we should propose
    val currentVotePosition = votePosition.updateAndGet(new IntUnaryOperator {
      def apply(i: Int) = (i + 1) % validVotes.size
    })

    val voteToCast = validVotes.get(currentVotePosition)

    // Get a vote from the valid votes and return it
    Option(new ValidatorVote(voteToCast.getValue, localAddress, voteToCast.getKey))
  }
}
