package khipu.consensus.common

import khipu.domain.Blockchain
import khipu.domain.BlockHeader
//import org.hyperledger.besu.ethereum.chain.Blockchain;

class ForkingVoteTallyCache(
  blockchain:         Blockchain,
  voteTallyUpdater:   VoteTallyUpdater,
  epochManager:       EpochManager,
  blockInterface:     BlockInterface,
  validatorOverrides: IbftValidatorOverrides
) extends VoteTallyCache(
  blockchain, voteTallyUpdater, epochManager, blockInterface
) {

  override protected def getValidatorsAfter(header: BlockHeader): VoteTally = {
    val nextBlockNumber = header.number + 1L

    validatorOverrides
      .getForBlock(nextBlockNumber)
      .map(new VoteTally(_))
      .getOrElse(super.getValidatorsAfter(header))
  }
}
