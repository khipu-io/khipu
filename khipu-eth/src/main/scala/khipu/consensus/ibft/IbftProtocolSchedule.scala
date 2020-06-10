package khipu.consensus.ibft

import IbftBlockHeaderValidationRulesetFactory.ibftBlockHeaderValidator;

import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.config.IbftConfigOptions;
import org.hyperledger.besu.ethereum.MainnetBlockValidator;
import org.hyperledger.besu.ethereum.core.PrivacyParameters;
import org.hyperledger.besu.ethereum.core.Wei;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockBodyValidator;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockImporter;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleBuilder;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpecBuilder;

import java.math.BigInteger;

/** Defines the protocol behaviours for a blockchain using IBFT. */
object  IbftProtocolSchedule {

  private val DEFAULT_CHAIN_ID = BigInteger.ONE;

  def create(
      config: GenesisConfigOptions ,
      privacyParameters:PrivacyParameters ,
      isRevertReasonEnabled:Boolean):ProtocolSchedule =  {
    val ibftConfig = config.getIbftLegacyConfigOptions();
    val blockPeriod = ibftConfig.getBlockPeriodSeconds();

    return new ProtocolScheduleBuilder(
            config,
            DEFAULT_CHAIN_ID,
            builder => applyIbftChanges(blockPeriod, builder),
            privacyParameters,
            isRevertReasonEnabled)
        .createProtocolSchedule();
  }

  def create(config:GenesisConfigOptions , isRevertReasonEnabled:Boolean):ProtocolSchedule =  {
    create(config, PrivacyParameters.DEFAULT, isRevertReasonEnabled);
  }

  def create(config:GenesisConfigOptions ): ProtocolSchedule =  {
    create(config, PrivacyParameters.DEFAULT, false);
  }

  private def applyIbftChanges(secondsBetweenBlocks:Long, builder:ProtocolSpecBuilder ):ProtocolSpecBuilder =  {
    builder
        .blockHeaderValidatorBuilder(ibftBlockHeaderValidator(secondsBetweenBlocks))
        .ommerHeaderValidatorBuilder(ibftBlockHeaderValidator(secondsBetweenBlocks))
        .blockBodyValidatorBuilder(MainnetBlockBodyValidator::new)
        .blockValidatorBuilder(MainnetBlockValidator::new)
        .blockImporterBuilder(MainnetBlockImporter::new)
        .difficultyCalculator((time, parent, protocolContext) -> BigInteger.ONE)
        .blockReward(Wei.ZERO)
        .skipZeroBlockRewards(true)
        .blockHeaderFunctions(IbftBlockHeaderFunctions.forOnChainBlock());
  }
}
