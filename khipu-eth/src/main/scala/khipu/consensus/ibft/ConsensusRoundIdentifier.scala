package khipu.consensus.ibft

import khipu.rlp.RLPImplicitConversions._
import khipu.rlp.RLPImplicits._
import khipu.rlp.RLPList
import khipu.rlp.RLPEncodeable

object ConsensusRoundIdentifier {
  /**
   * Constructor that derives the sequence and round information from an RLP encoded message
   *
   * @param in The RLP body of the message to check
   * @return A derived sequence and round number
   */
  def readFrom(in: RLPEncodeable): ConsensusRoundIdentifier = {
    in match {
      case RLPList(sequence, round) => ConsensusRoundIdentifier(sequence, round)
      case _                        => throw new RuntimeException("Cannot decode ConsensusRoundIdentifier")
    }
  }

}
/**
 * Represents the chain index (i.e. height) and number of attempted consensuses conducted at this
 * height.
 */
/**
 * Constructor for a round identifier
 *
 * @param sequence Sequence number for this round, synonymous with block height
 * @param round round number for the current attempt at achieving consensus
 */
final case class ConsensusRoundIdentifier(sequence: Long, round: Int) extends Ordered[ConsensusRoundIdentifier] {

  /**
   * Adds this rounds information to a given RLP buffer
   *
   * @param out The RLP buffer to add to
   */
  def writeTo(): RLPEncodeable = RLPList(sequence, round)

  /**
   * Comparator for round identifiers to achieve ordering
   *
   * @param v The round to compare this one to
   * @return a negative integer, zero, or a positive integer as this object is less than, equal to,
   *     or greater than the specified object.
   */
  override def compare(v: ConsensusRoundIdentifier) = {
    val sequenceComparison = java.lang.Long.compareUnsigned(sequence, v.sequence)
    if (sequenceComparison != 0) {
      sequenceComparison
    } else {
      java.lang.Integer.compare(round, v.round)
    }
  }

}
