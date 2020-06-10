package khipu.consensus.common

/**
 * Determines if a validator vote is indicating that they should be added, or removed. This does not
 * attempt to determine how said vote should be serialised/deserialised.
 */
trait VoteType
object VoteType {
  case object ADD extends VoteType
  case object DROP extends VoteType
}
