package khipu.consensus.ibft.payload

import khipu.consensus.ibft.ConsensusRoundIdentifier

trait RoundSpecific {
  def roundIdentifier: ConsensusRoundIdentifier
}
