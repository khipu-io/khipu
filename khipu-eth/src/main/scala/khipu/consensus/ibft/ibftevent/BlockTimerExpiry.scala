package khipu.consensus.ibft.ibftevent

import khipu.consensus.ibft.ConsensusRoundIdentifier

/**
 * Event indicating a block timer has expired
 * @param roundIdentifier The roundIdentifier that the expired timer belonged to
 */
final case class BlockTimerExpiry(roundIdentifier: ConsensusRoundIdentifier) extends IbftEvent {
  override def getType() = IbftEvents.Type.BLOCK_TIMER_EXPIRY
}