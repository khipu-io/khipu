package khipu.domain

import java.math.BigInteger
import khipu.util.BlockchainConfig

object DifficultyCalculator {
  val DifficultyBoundDivision = BigInteger.valueOf(2048)
  val FrontierTimestampDiffLimit = -99
  val ExpDifficultyPeriod = 100000
  val MinimumDifficulty = BigInteger.valueOf(131072)
  val TW0 = BigInteger.valueOf(2)
}
final class DifficultyCalculator(blockchainConfig: BlockchainConfig) {
  import DifficultyCalculator._

  val isEth = khipu.util.Config.chainType match {
    case "eth" => true
    case _     => false
  }

  def calculateDifficulty(currHeader: BlockHeader, parentHeader: BlockHeader): BigInteger =
    calculateDifficulty(currHeader.number, currHeader.unixTimestamp, parentHeader)
  def calculateDifficulty(blockNumber: Long, blockTimestamp: Long, parentHeader: BlockHeader): BigInteger = {
    val quotient = parentHeader.difficulty divide DifficultyBoundDivision
    val multiplier = getCalcDifficultyMultiplier(blockNumber, blockTimestamp, parentHeader)

    val fromParent = parentHeader.difficulty add (quotient multiply BigInteger.valueOf(multiplier))
    val difficulty = MinimumDifficulty.max(fromParent)

    val difficultyBombExponent = if (isEth) getBombExponent_eth(blockNumber) else getBombExponent_etc(blockNumber)
    val difficultyBomb = if (difficultyBombExponent >= 0) {
      TW0.pow(difficultyBombExponent.toInt)
    } else {
      BigInteger.ZERO
    }

    difficulty add difficultyBomb
  }

  private def getCalcDifficultyMultiplier(blockNumber: Long, blockTimestamp: Long, parentHeader: BlockHeader): Long = {
    if (blockNumber < blockchainConfig.homesteadBlockNumber) {
      if (blockTimestamp < parentHeader.unixTimestamp + 13) 1 else -1
    } else if (blockNumber < blockchainConfig.byzantiumBlockNumber) {
      math.max(1 - (blockTimestamp - parentHeader.unixTimestamp) / 10, FrontierTimestampDiffLimit)
    } else { // eip100
      val unclesAdj = if (parentHeader.nonUncles) 1 else 2
      math.max(unclesAdj - (blockTimestamp - parentHeader.unixTimestamp) / 9, FrontierTimestampDiffLimit)
    }
  }

  /**
   * Ethereum Classic HF on Block #3_000_000:
   * - EXP reprice (EIP-160)
   * - Replay Protection (EIP-155) (chainID: 61)
   * - Difficulty Bomb delay (ECIP-1010) (https://github.com/ethereumproject/ECIPs/blob/master/ECIPs/ECIP-1010.md)
   */
  private def getBombExponent_etc(blockNumber: Long): Long = {
    import blockchainConfig.{ homesteadBlockNumber, difficultyBombPauseBlockNumber, difficultyBombContinueBlockNumber }

    if (blockNumber < difficultyBombPauseBlockNumber) {
      (blockNumber / ExpDifficultyPeriod - 2)
    } else if (blockNumber < difficultyBombContinueBlockNumber) {
      ((difficultyBombPauseBlockNumber.longValue / ExpDifficultyPeriod) - 2)
    } else {
      val delay = (difficultyBombContinueBlockNumber - difficultyBombPauseBlockNumber) / ExpDifficultyPeriod
      ((blockNumber / ExpDifficultyPeriod) - delay - 2)
    }
  }

  private def getBombExponent_eth(blockNumber: Long): Long = {
    if (blockNumber < blockchainConfig.byzantiumBlockNumber) {
      (blockNumber / ExpDifficultyPeriod - 2)
    } else { // eip649
      ((blockNumber - 3000000).max(0) / ExpDifficultyPeriod - 2)
    }
  }
}

