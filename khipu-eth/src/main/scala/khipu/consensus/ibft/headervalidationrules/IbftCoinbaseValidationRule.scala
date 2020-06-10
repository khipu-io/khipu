package khipu.consensus.ibft.headervalidationrules

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.mainnet.AttachedBlockHeaderValidationRule;

import java.util.Collection;

import khipu.domain.BlockHeader

/**
 * Ensures that the coinbase (which corresponds to the block proposer) is included in the list of
 * validators
 */
class IbftCoinbaseValidationRule extends AttachedBlockHeaderValidationRule {

  override def validate(header: BlockHeader, parent: BlockHeader, context: ProtocolContext): Boolean = {

    val validatorProvider = context
      .getConsensusState(classOf[IbftContext])
      .getVoteTallyCache()
      .getVoteTallyAfterBlock(parent);

    val proposer = header.getCoinbase();

    val storedValidators = validatorProvider.getValidators();

    if (!storedValidators.contains(proposer)) {
      //LOGGER.trace(
      //    "Block proposer is not a member of the validators. proposer={}, validators={}",
      //    proposer,
      //    storedValidators);
      return false;
    }

    return true;
  }
}
