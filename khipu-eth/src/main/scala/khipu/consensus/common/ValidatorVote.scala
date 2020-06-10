package khipu.consensus.common

import VoteType.ADD
import khipu.domain.Address

final case class ValidatorVote(votePolarity: VoteType, proposer: Address, recipient: Address) {
  def isAuthVote() = votePolarity == ADD
}