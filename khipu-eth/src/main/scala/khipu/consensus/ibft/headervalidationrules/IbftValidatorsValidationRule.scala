package khipu.consensus.ibft.headervalidationrules

import org.hyperledger.besu.consensus.common.ValidatorProvider;
import org.hyperledger.besu.consensus.ibft.IbftContext;
import org.hyperledger.besu.consensus.ibft.IbftExtraData;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Address;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.AttachedBlockHeaderValidationRule;
import org.hyperledger.besu.ethereum.rlp.RLPException;

import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.collect.Iterables;
import khipu.consensus.ibft.IbftExtraData
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Ensures the Validators listed in the block header match that tracked in memory (which was in-turn
 * created by tracking votes included on the block chain).
 */
class IbftValidatorsValidationRule extends AttachedBlockHeaderValidationRule {

  override def validate(header: BlockHeader, parent: BlockHeader, context: ProtocolContext): Boolean = {
    try {
      val validatorProvider = context
        .getConsensusState(classOf[IbftContext])
        .getVoteTallyCache()
        .getVoteTallyAfterBlock(parent);
      val ibftExtraData = IbftExtraData.decode(header);

      val sortedReportedValidators =
        new TreeSet[Address](ibftExtraData.getValidators());

      if (!Iterables.elementsEqual(ibftExtraData.getValidators(), sortedReportedValidators)) {
        //LOGGER.trace(
        //    "Validators are not sorted in ascending order. Expected {} but got {}.",
        //    sortedReportedValidators,
        //    ibftExtraData.getValidators());
        return false;
      }

      val storedValidators = validatorProvider.getValidators();
      if (!Iterables.elementsEqual(ibftExtraData.getValidators(), storedValidators)) {
        //LOGGER.trace(
        //    "Incorrect validators. Expected {} but got {}.",
        //    storedValidators,
        //    ibftExtraData.getValidators());
        return false;
      }

    } catch {
      case ex: RLPException =>
        //LOGGER.trace("ExtraData field was unable to be deserialised into an IBFT Struct.", ex);
        return false;
      case ex: IllegalArgumentException =>
        //LOGGER.trace("Failed to verify extra data", ex);
        return false;
    }

    return true;
  }
}
