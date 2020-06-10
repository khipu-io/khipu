package khipu.consensus.ibft.ibftevent

import khipu.consensus.ibft.ConsensusRoundIdentifier

/**
 * Event indicating a round timer has expired
 * @param round The round that the expired timer belonged to
 */
final case class RoundExpiry(round: ConsensusRoundIdentifier) extends IbftEvent {

  override def getType(): IbftEvents.Type = IbftEvents.Type.ROUND_EXPIRY

  def getView(): ConsensusRoundIdentifier = round
}
