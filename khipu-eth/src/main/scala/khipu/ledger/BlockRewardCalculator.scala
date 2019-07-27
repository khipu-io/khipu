package khipu.ledger

import khipu.EvmWord
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
  val rewardReductionRateDenom = EvmWord(BigDecimal(1 - config.rewardRedutionRate).precision * 10)
  /** Rate at which block and ommer rewards are reduced in successive eras (denominator) */
  val rewardReductionRateNumer = EvmWord(((1 - config.rewardRedutionRate) * rewardReductionRateDenom.n.doubleValue).toInt)

  /** Reward to the block miner for inclusion of ommers as a fraction of block reward (numerator) */
  val ommerInclusionRewardNumer = EvmWord.One
  /** Reward to the block miner for inclusion of ommers as a fraction of block reward (denominator) */
  val ommerInclusionRewardDenom = EvmWord.ThirtyTwo

  /**
   * Reward to the miner of an included ommer as a fraction of block reward (numerator).
   * For era 2+
   */
  val ommerMiningRewardNumer = EvmWord.One
  /**
   * Reward to the miner of an included ommer as a fraction of block reward (denominator).
   * For era 2+
   */
  val ommerMiningRewardDenom = EvmWord.safe(32)

  /**
   * Reward to the miner of an included ommer as a fraction of block reward (max numerator).
   * Different in the first era
   */
  val firstEraOmmerMiningRewardMaxNumer = EvmWord.safe(7)
  /**
   * Reward to the miner of an included ommer as a fraction of block reward (denominator).
   * Different in the first era
   */
  val firstEraOmmerMiningRewardDenom = EvmWord.safe(8)

  /** Base block reward in the first era */
  def blockRewardOf(blockNumber: Long): EvmWord = {
    if (blockNumber < blockchainConfig.byzantiumBlockNumber) {
      config.firstEraBlockReward
    } else if (blockNumber < blockchainConfig.constantinopleBlockNumber) {
      config.byzantiumBlockReward
    } else {
      config.constantinopleBlockReward
    }
  }

  def calcBlockMinerReward(blockNumber: Long, ommersCount: Int): EvmWord = {
    val era = eraNumber(blockNumber)
    val eraMultiplier = rewardReductionRateNumer.pow(era)
    val eraDivisor = rewardReductionRateDenom.pow(era)

    val blockReward = blockRewardOf(blockNumber)
    val baseReward = (blockReward * eraMultiplier) / eraDivisor
    val ommersReward =
      (blockReward * EvmWord.safe(ommersCount) * ommerInclusionRewardNumer * eraMultiplier) / (ommerInclusionRewardDenom * eraDivisor)
    baseReward + ommersReward
  }

  def calcOmmerMinerReward(blockNumber: Long, ommerNumber: Long): EvmWord = {
    val era = eraNumber(blockNumber)
    val blockReward = blockRewardOf(blockNumber)

    if (era == 0) {
      val numer = firstEraOmmerMiningRewardMaxNumer - EvmWord(blockNumber - ommerNumber - 1)
      (blockReward * numer) / firstEraOmmerMiningRewardDenom
    } else {
      val eraMultiplier = rewardReductionRateNumer.pow(era)
      val eraDivisor = rewardReductionRateDenom.pow(era)
      (blockReward * ommerMiningRewardNumer * eraMultiplier) / (ommerMiningRewardDenom * eraDivisor)
    }
  }

  /** era number counting from 0 */
  private def eraNumber(blockNumber: Long): Int = {
    ((blockNumber - 1) / eraDuration).toInt
  }
}
