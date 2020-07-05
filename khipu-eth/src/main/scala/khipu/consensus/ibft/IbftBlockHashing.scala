package khipu.consensus.ibft

import akka.util.ByteString
import khipu.Hash
import khipu.domain.Address
import khipu.domain.BlockHeader
import khipu.domain.Blockchain
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.Util;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import java.util.List

object IbftBlockHashing {

  /**
   * Constructs a hash of the block header suitable for signing as a committed seal. The extra data
   * in the hash uses an empty list for the committed seals.
   *
   * @param header The header for which a proposer seal is to be calculated (with or without extra
   *     data)
   * @param ibftExtraData The extra data block which is to be inserted to the header once seal is
   *     calculated
   * @return the hash of the header including the validator and proposer seal in the extra data
   */
  def calculateDataHashForCommittedSeal(header: BlockHeader, ibftExtraData: IbftExtraData): Hash = {
    Hash(serializeHeader(header, ibftExtraData.encodeWithoutCommitSeals))
  }

  def calculateDataHashForCommittedSeal(header: BlockHeader): Hash = {
    val ibftExtraData = IbftExtraData.decode(header)
    Hash(serializeHeader(header, ibftExtraData.encodeWithoutCommitSeals))
  }

  /**
   * Constructs a hash of the block header, but omits the committerSeals and sets round number to 0
   * (as these change on each of the potentially circulated blocks at the current chain height).
   *
   * @param header The header for which a block hash is to be calculated
   * @return the hash of the header to be used when referencing the header on the blockchain
   */
  def calculateHashOfIbftBlockOnChain(header: BlockHeader): Hash = {
    val ibftExtraData = IbftExtraData.decode(header)
    Hash(serializeHeader(header, ibftExtraData.encodeWithoutCommitSealsAndRoundNumber));
  }

  /**
   * Recovers the {@link Address} for each validator that contributed a committed seal to the block.
   *
   * @param header the block header that was signed by the committed seals
   * @param ibftExtraData the parsed {@link IbftExtraData} from the header
   * @return the addresses of validators that provided a committed seal
   */
  def recoverCommitterAddresses(header: BlockHeader, ibftExtraData: IbftExtraData): List[Address] = {
    val committerHash = IbftBlockHashing.calculateDataHashForCommittedSeal(header, ibftExtraData)

    ibftExtraData.seals
      .map(p => Util.signatureToAddress(p, committerHash))
      .collect(Collectors.toList());
  }

  private def serializeHeader(header: BlockHeader, extraDataSerializer: => ByteString): Array[Byte] = {

    // create a block header which is a copy of the header supplied as parameter except of the
    // extraData field
    val builder = BlockHeaderBuilder.fromHeader(header);
    builder.blockHeaderFunctions(IbftBlockHeaderFunctions.forOnChainBlock())

    // set the extraData field using the supplied extraDataSerializer if the block height is not 0
    if (header.number == Blockchain.GENESIS_BLOCK_NUMBER) {
      builder.extraData(header.extraData)
    } else {
      builder.extraData(extraDataSerializer())
    }

    val out = new BytesValueRLPOutput()
    builder.buildBlockHeader().writeTo(out)
    out.encoded();
  }
}
