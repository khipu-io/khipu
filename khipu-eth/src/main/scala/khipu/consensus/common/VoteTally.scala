package khipu.consensus.common

import java.util.Collection
import java.util.Collections
import java.util.HashMap
import java.util.HashSet
import java.util.Map
import java.util.Optional
import java.util.Set
import java.util.SortedSet
import java.util.TreeSet
import khipu.domain.Address
import scala.collection.JavaConversions._

/** Tracks the current list of validators and votes to add or drop validators. */
class VoteTally private (
    currentValidators:    SortedSet[Address],
    addVotesBySubject:    Map[Address, Set[Address]],
    removeVotesBySubject: Map[Address, Set[Address]]
) extends ValidatorProvider {

  private def this(
    initialValidators:    Collection[Address],
    addVotesBySubject:    Map[Address, Set[Address]],
    removeVotesBySubject: Map[Address, Set[Address]]
  ) =
    this(new TreeSet[Address](initialValidators), addVotesBySubject, removeVotesBySubject)

  def this(initialValidators: Collection[Address]) =
    this(new TreeSet[Address](initialValidators), new HashMap[Address, Set[Address]](), new HashMap[Address, Set[Address]]())

  /**
   * Add a vote to the current tally. The current validator list will be updated if this vote takes
   * the tally past the required votes to approve the change.
   *
   * @param validatorVote The vote which was cast in a block header.
   */
  def addVote(validatorVote: ValidatorVote) {
    val addVotesForSubject = addVotesBySubject.computeIfAbsent(validatorVote.recipient, new java.util.function.Function[Address, Set[Address]] {
      def apply(target: Address) = new HashSet[Address]()
    })
    val removeVotesForSubject = removeVotesBySubject.computeIfAbsent(validatorVote.recipient, new java.util.function.Function[Address, Set[Address]] {
      def apply(target: Address) = new HashSet[Address]()
    })

    if (validatorVote.isAuthVote) {
      addVotesForSubject.add(validatorVote.proposer)
      removeVotesForSubject.remove(validatorVote.proposer)
    } else {
      removeVotesForSubject.add(validatorVote.proposer)
      addVotesForSubject.remove(validatorVote.proposer)
    }

    val validatorLimit = calcValidatorLimit()
    if (addVotesForSubject.size >= validatorLimit) {
      currentValidators.add(validatorVote.recipient)
      discardOutstandingVotesFor(validatorVote.recipient)
    }
    if (removeVotesForSubject.size >= validatorLimit) {
      currentValidators.remove(validatorVote.recipient)
      discardOutstandingVotesFor(validatorVote.recipient)
      addVotesBySubject.values.foreach(votes => votes.remove(validatorVote.recipient))
      removeVotesBySubject.values.foreach(votes => votes.remove(validatorVote.recipient))
    }
  }

  private def discardOutstandingVotesFor(subject: Address) {
    addVotesBySubject.remove(subject);
    removeVotesBySubject.remove(subject);
  }

  def getOutstandingAddVotesFor(subject: Address): Set[Address] = {
    Optional.ofNullable(addVotesBySubject.get(subject)).orElse(Collections.emptySet())
  }

  def getOutstandingRemoveVotesFor(subject: Address): Set[Address] = {
    Optional.ofNullable(removeVotesBySubject.get(subject)).orElse(Collections.emptySet())
  }

  private def calcValidatorLimit() = (currentValidators.size() / 2) + 1

  /**
   * Reset the outstanding vote tallies as required at each epoch. The current validator list is
   * unaffected.
   */
  def discardOutstandingVotes() {
    addVotesBySubject.clear()
  }

  /**
   * @return The collection of validators after the voting at the most recent block has been
   *     finalised.
   */
  override def getValidators(): Collection[Address] = {
    currentValidators
  }

  def copy(): VoteTally = {
    val addVotesBySubject = new HashMap[Address, Set[Address]]()
    val removeVotesBySubject = new HashMap[Address, Set[Address]]()

    this.addVotesBySubject.foreach {
      case (key, value) => addVotesBySubject.put(key, new TreeSet[Address](value))
    }
    this.removeVotesBySubject.foreach {
      case (key, value) => removeVotesBySubject.put(key, new TreeSet[Address](value))
    }

    new VoteTally(new TreeSet[Address](this.currentValidators), addVotesBySubject, removeVotesBySubject)
  }
}
