package khipu.ledger

import java.math.BigInteger
import khipu.util.BlockchainConfig

/**
 * Calculates rewards for mining blocks and ommers.
 * Avoids floating point arithmetic. Because of that the formulas may look a bit unintuitive, but the important
 * thing here is that we want to defer any division to be a single and final operation
 */
final class BlockRewardCalculator(blockchainConfig: BlockchainConfig) {
  /** Era duration in blocks */
  val config = blockchainConfig.monetaryPolicyConfig
  val eraDuration = config.eraDuration

  /** Rate at which block and ommer rewards are reduced in successive eras (numerator) */
  val rewardReductionRateDenom: BigInteger = BigInteger.valueOf(BigDecimal(1 - config.rewardRedutionRate).precision * 10)
  /** Rate at which block and ommer rewards are reduced in successive eras (denominator) */
  val rewardReductionRateNumer: BigInteger = BigInteger.valueOf(((1 - config.rewardRedutionRate) * rewardReductionRateDenom.doubleValue).toInt)

  /** Reward to the block miner for inclusion of ommers as a fraction of block reward (numerator) */
  val ommerInclusionRewardNumer: BigInteger = BigInteger.ONE
  /** Reward to the block miner for inclusion of ommers as a fraction of block reward (denominator) */
  val ommerInclusionRewardDenom: BigInteger = BigInteger.valueOf(32)

  /**
   * Reward to the miner of an included ommer as a fraction of block reward (numerator).
   * For era 2+
   */
  val ommerMiningRewardNumer: BigInteger = BigInteger.ONE
  /**
   * Reward to the miner of an included ommer as a fraction of block reward (denominator).
   * For era 2+
   */
  val ommerMiningRewardDenom: BigInteger = BigInteger.valueOf(32)

  /**
   * Reward to the miner of an included ommer as a fraction of block reward (max numerator).
   * Different in the first era
   */
  val firstEraOmmerMiningRewardMaxNumer: BigInteger = BigInteger.valueOf(7)
  /**
   * Reward to the miner of an included ommer as a fraction of block reward (denominator).
   * Different in the first era
   */
  val firstEraOmmerMiningRewardDenom: BigInteger = BigInteger.valueOf(8)

  /** Base block reward in the first era */
  def blockRewardOf(blockNumber: Long): BigInteger = if (blockNumber < blockchainConfig.byzantiumBlockNumber) {
    config.firstEraBlockReward
  } else {
    config.byzantiumBlockReward
  }

  def calcBlockMinerReward(blockNumber: Long, ommersCount: Int): BigInteger = {
    val era = eraNumber(blockNumber)
    val eraMultiplier = rewardReductionRateNumer.pow(era)
    val eraDivisor = rewardReductionRateDenom.pow(era)

    val blockReward = blockRewardOf(blockNumber)
    val baseReward = (blockReward multiply eraMultiplier) divide eraDivisor
    val ommersReward =
      (blockReward multiply BigInteger.valueOf(ommersCount) multiply ommerInclusionRewardNumer multiply eraMultiplier) divide (ommerInclusionRewardDenom multiply eraDivisor)
    baseReward add ommersReward
  }

  def calcOmmerMinerReward(blockNumber: Long, ommerNumber: Long): BigInteger = {
    val era = eraNumber(blockNumber)
    val blockReward = blockRewardOf(blockNumber)

    if (era == 0) {
      val numer = firstEraOmmerMiningRewardMaxNumer subtract BigInteger.valueOf(blockNumber - ommerNumber - 1)
      (blockReward multiply numer) divide firstEraOmmerMiningRewardDenom
    } else {
      val eraMultiplier = rewardReductionRateNumer.pow(era)
      val eraDivisor = rewardReductionRateDenom.pow(era)
      (blockReward multiply ommerMiningRewardNumer multiply eraMultiplier) divide (ommerMiningRewardDenom multiply eraDivisor)
    }
  }

  /** era number counting from 0 */
  private def eraNumber(blockNumber: Long): Int = {
    ((blockNumber - 1) / eraDuration).toInt
  }
}
