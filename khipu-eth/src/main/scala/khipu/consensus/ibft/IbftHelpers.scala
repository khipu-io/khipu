package khipu.consensus.ibft

import khipu.consensus.ibft.payload.PreparedCertificate
import khipu.consensus.ibft.payload.RoundChangePayload
import khipu.consensus.ibft.payload.SignedData
import org.hyperledger.besu.consensus.ibft.payload.PreparedCertificate;
import org.hyperledger.besu.consensus.ibft.payload.RoundChangePayload;
import org.hyperledger.besu.consensus.ibft.payload.SignedData;
import org.hyperledger.besu.crypto.SECP256K1.Signature;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.Hash;
import org.hyperledger.besu.ethereum.core.Util;

import java.util.Collection;
import java.util.Optional;

object IbftHelpers {

  val EXPECTED_MIX_HASH = Hash.fromHexString("0x63746963616c2062797a616e74696e65206661756c7420746f6c6572616e6365");

  def calculateRequiredValidatorQuorum(validatorCount: Int): Int = {
    return Util.fastDivCeiling(2 * validatorCount, 3);
  }

  def prepareMessageCountForQuorum(quorum: Long): Long = {
    return quorum - 1;
  }

  def createSealedBlock(block: Block, commitSeals: Collection[Signature]): Block = {
    val initialHeader = block.getHeader();
    val initialExtraData = IbftExtraData.decode(initialHeader);

    val sealedExtraData = new IbftExtraData(
      initialExtraData.getVanityData(),
      commitSeals,
      initialExtraData.getVote(),
      initialExtraData.getRound(),
      initialExtraData.getValidators()
    );

    val sealedHeader =
      BlockHeaderBuilder.fromHeader(initialHeader)
        .extraData(sealedExtraData.encode())
        .blockHeaderFunctions(IbftBlockHeaderFunctions.forOnChainBlock())
        .buildBlockHeader();

    return new Block(sealedHeader, block.getBody());
  }

  def findLatestPreparedCertificate(msgs: Collection[SignedData[RoundChangePayload]]): Option[PreparedCertificate] = {

    var result: Option[PreparedCertificate] = None

    val itr = msgs.iterator
    while (itr.hasNext) {
      val roundChangeMsg = itr.next()
      val payload = roundChangeMsg.getPayload();
      if (payload.getPreparedCertificate().isPresent()) {
        if (!result.isDefined) {
          result = payload.getPreparedCertificate();
        } else {
          val currentLatest = result.get;
          val nextCert = payload.getPreparedCertificate().get();

          if (currentLatest.getProposalPayload().getPayload().getRoundIdentifier().getRoundNumber()
            < nextCert.getProposalPayload().getPayload().getRoundIdentifier().getRoundNumber()) {
            result = Optional.of(nextCert);
          }
        }
      }
    }
    return result;
  }
}
