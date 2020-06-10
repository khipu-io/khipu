package khipu.consensus.ibft

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.core.Hash;

import java.util.function.Function;

object IbftBlockHeaderFunctions {
  private val COMMITTED_SEAL = new IbftBlockHeaderFunctions(IbftBlockHashing :: calculateDataHashForCommittedSeal);
  private val ON_CHAIN = new IbftBlockHeaderFunctions(IbftBlockHashing :: calculateHashOfIbftBlockOnChain);

  def forOnChainBlock(): BlockHeaderFunctions = ON_CHAIN;

  def forCommittedSeal(): BlockHeaderFunctions = COMMITTED_SEAL;

}
class IbftBlockHeaderFunctions(hashFunction: Function[BlockHeader, Hash]) extends BlockHeaderFunctions {

  override def hash(header: BlockHeader): Hash = hashFunction.apply(header);

  override def parseExtraData(header: BlockHeader): IbftExtraData = IbftExtraData.decodeRaw(header.getExtraData());
}
