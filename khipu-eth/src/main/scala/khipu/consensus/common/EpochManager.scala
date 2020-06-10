package khipu.consensus.common

class EpochManager(epochLengthInBlocks: Long) {

  def isEpochBlock(blockNumber: Long): Boolean = {
    (blockNumber % epochLengthInBlocks) == 0
  }

  def getLastEpochBlock(blockNumber: Long): Long = {
    blockNumber - (blockNumber % epochLengthInBlocks)
  }
}
