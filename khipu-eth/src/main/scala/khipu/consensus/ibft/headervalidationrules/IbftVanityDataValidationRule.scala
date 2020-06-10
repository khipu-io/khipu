package khipu.consensus.ibft.headervalidationrules

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.mainnet.AttachedBlockHeaderValidationRule;

import khipu.consensus.ibft.IbftExtraData
import khipu.domain.BlockHeader

class IbftVanityDataValidationRule extends AttachedBlockHeaderValidationRule {

  override def validate(header: BlockHeader, parent: BlockHeader, protocolContext: ProtocolContext): Boolean = {
    val extraData = IbftExtraData.decode(header);

    if (extraData.getVanityData().size() != IbftExtraData.EXTRA_VANITY_LENGTH) {
      //LOG.trace("Ibft Extra Data does not contain 32 bytes of vanity data.");
      return false;
    }
    return true;
  }
}
