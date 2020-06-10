package khipu.consensus.ibft

import khipu.domain.Address
import khipu.domain.BlockHeader
import org.hyperledger.besu.consensus.common.BlockInterface;
import org.hyperledger.besu.consensus.common.ValidatorVote;
import org.hyperledger.besu.consensus.common.VoteType;
import org.hyperledger.besu.ethereum.core.Address;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;

import java.util.Collection;
import java.util.Optional;

object IbftBlockInterface {
  def replaceRoundInBlock(block: Block, round: Int, blockHeaderFunctions: BlockHeaderFunctions): Block = {
    val prevExtraData = IbftExtraData.decode(block.getHeader());
    val substituteExtraData =
      new IbftExtraData(
        prevExtraData.vanityData,
        prevExtraData.seals,
        prevExtraData.vote,
        round,
        prevExtraData.validators
      )

    val headerBuilder = BlockHeaderBuilder.fromHeader(block.getHeader());
    headerBuilder
      .extraData(substituteExtraData.encode())
      .blockHeaderFunctions(blockHeaderFunctions);

    val newHeader = headerBuilder.buildBlockHeader();

    return new Block(newHeader, block.getBody());
  }
}
class IbftBlockInterface extends BlockInterface {

  override def getProposerOfBlock(header: BlockHeader): Address = {
    header.getCoinbase();
  }

  override def getProposerOfBlock(header: org.hyperledger.besu.plugin.data.BlockHeader): Address = {
    Address.fromHexString(header.getCoinbase().toHexString());
  }

  override def extractVoteFromHeader(header: BlockHeader): Option[ValidatorVote] = {
    val ibftExtraData = IbftExtraData.decode(header)

    if (ibftExtraData.vote.isDefined) {
      val headerVote = ibftExtraData.vote.get
      val vote =
        new ValidatorVote(
          if (headerVote.isAuth) VoteType.ADD else VoteType.DROP,
          getProposerOfBlock(header),
          headerVote.getRecipient()
        );
      return Option(vote)
    }
    return None
  }

  override def validatorsInBlock(header: BlockHeader): Collection[Address] = {
    val ibftExtraData = IbftExtraData.decode(header);
    return ibftExtraData.validators
  }

}
