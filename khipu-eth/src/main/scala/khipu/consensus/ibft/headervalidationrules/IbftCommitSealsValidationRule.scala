package khipu.consensus.ibft.headervalidationrules

import static org.hyperledger.besu.consensus.ibft.IbftHelpers.calculateRequiredValidatorQuorum;

import khipu.consensus.ibft.IbftBlockHashing
import khipu.consensus.ibft.IbftExtraData
import khipu.domain.Address
import khipu.domain.BlockHeader
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Address;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.AttachedBlockHeaderValidationRule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;


/**
 * Ensures the commit seals in the block header were created by known validators (as determined by
 * tracking votes and validator state on the blockchain).
 *
 * <p>This also ensures sufficient commit seals exist in the block to make it valid.
 */
class IbftCommitSealsValidationRule extends  AttachedBlockHeaderValidationRule {


  override def validate(header: BlockHeader , parent: BlockHeader , protocolContext: ProtocolContext ) : Boolean ={
    val validatorProvider = protocolContext
            .getConsensusState(classOf[IbftContext])
            .getVoteTallyCache()
            .getVoteTallyAfterBlock(parent);
    val ibftExtraData = IbftExtraData.decode(header);

    val committers = IbftBlockHashing.recoverCommitterAddresses(header, ibftExtraData);
    val committersWithoutDuplicates = new ArrayList[Address](new HashSet[Address](committers));

    if (committers.size() != committersWithoutDuplicates.size()) {
      LOGGER.trace("Duplicated seals found in header.");
      return false;
    }

    return validateCommitters(committersWithoutDuplicates, validatorProvider.getValidators());
  }

  private def validateCommitters(committers: Collection[Address] , storedValidators: Collection[Address] ): Boolean =  {

    val minimumSealsRequired = calculateRequiredValidatorQuorum(storedValidators.size());
    if (committers.size() < minimumSealsRequired) {
      //LOGGER.trace(
      //    "Insufficient committers to seal block. (Required {}, received {})",
      //    minimumSealsRequired,
      //    committers.size());
      return false;
    }

    if (!storedValidators.containsAll(committers)) {
      //LOGGER.trace(
      //    "Not all committers are in the locally maintained validator list. validators={} committers={}",
      //    storedValidators,
      //    committers);
      return false;
    }

    return true;
  }

  override def includeInLightValidation() = false;
}
