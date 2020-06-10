package khipu.consensus.common

import khipu.domain.Blockchain
import khipu.domain.BlockHeader

/**
 * Provides the logic to extract vote tally state from the blockchain and update it as blocks are
 * added.
 */
class VoteTallyUpdater(epochManager: EpochManager, blockInterface: BlockInterface) {

  /**
   * Create a new VoteTally based on the current blockchain state.
   *
   * @param blockchain the blockchain to load the current state from
   * @return a VoteTally reflecting the state of the blockchain head
   */
  def buildVoteTallyFromBlockchain(blockchain: Blockchain): VoteTally = {
    val chainHeadBlockNumber = blockchain.getBestBlockNumber
    val epochBlockNumber = epochManager.getLastEpochBlock(chainHeadBlockNumber)
    //LOG.info("Loading validator voting state starting from block {}", epochBlockNumber)
    val epochBlock = blockchain.getBlockHeaderByNumber(epochBlockNumber).get
    val initialValidators = blockInterface.validatorsInBlock(epochBlock)
    val voteTally = new VoteTally(initialValidators)
    var blockNumber = epochBlockNumber + 1
    while (blockNumber <= chainHeadBlockNumber) {
      updateForBlock(blockchain.getBlockHeaderByNumber(blockNumber).get, voteTally)
      blockNumber += 1
    }
    voteTally
  }

  /**
   * Update the vote tally to reflect changes caused by appending a new block to the chain.
   *
   * @param header the header of the block being added
   * @param voteTally the vote tally to update
   */
  def updateForBlock(header: BlockHeader, voteTally: VoteTally) {
    if (epochManager.isEpochBlock(header.number)) {
      voteTally.discardOutstandingVotes()
    } else {
      val vote = blockInterface.extractVoteFromHeader(header)
      vote.foreach(voteTally.addVote)
    }
  }
}
