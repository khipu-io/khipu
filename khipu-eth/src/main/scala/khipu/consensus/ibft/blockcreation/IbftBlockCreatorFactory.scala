package khipu.consensus.ibft.blockcreation

import org.hyperledger.besu.consensus.common.ConsensusHelpers;
import org.hyperledger.besu.consensus.common.ValidatorVote;
import org.hyperledger.besu.consensus.common.VoteTally;
import org.hyperledger.besu.consensus.ibft.IbftContext;
import org.hyperledger.besu.consensus.ibft.IbftExtraData;
import org.hyperledger.besu.consensus.ibft.Vote;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Address;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MiningParameters;
import org.hyperledger.besu.ethereum.core.Wei;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import akka.util.ByteString
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import khipu.consensus.ibft.IbftExtraData
import khipu.consensus.ibft.Vote
import khipu.domain.Address
import khipu.domain.BlockHeader

object IbftBlockCreatorFactory {
  private def toVote(input: Option[ValidatorVote]): Option[Vote] = {
    input.map(v -> Optional.of(new Vote(v.getRecipient(), v.getVotePolarity())))
      .orElse(Optional.empty());
  }
}
class IbftBlockCreatorFactory(
    gasLimitCalculator:             Function[Long, Long],
    pendingTransactions:            PendingTransactions,
    protected val protocolContext:  ProtocolContext,
    protected val protocolSchedule: ProtocolSchedule,
    miningParams:                   MiningParameters,
    localAddress:                   Address
) {

  import IbftBlockCreatorFactory._

  private var minTransactionGasPrice = miningParams.getMinTransactionGasPrice();
  private val minBlockOccupancyRatio = miningParams.getMinBlockOccupancyRatio();
  private var vanityData = miningParams.getExtraData();

  def create(parentHeader: BlockHeader, round: Int): IbftBlockCreator = {
    return new IbftBlockCreator(
      localAddress,
      ph => createExtraData(round, ph),
      pendingTransactions,
      protocolContext,
      protocolSchedule,
      gasLimitCalculator,
      minTransactionGasPrice,
      minBlockOccupancyRatio,
      parentHeader
    );
  }

  def setExtraData(extraData: ByteString) {
    this.vanityData = extraData.copy;
  }

  def setMinTransactionGasPrice(minTransactionGasPrice: Wei) {
    this.minTransactionGasPrice = minTransactionGasPrice;
  }

  def getMinTransactionGasPrice(): Wei = {
    minTransactionGasPrice;
  }

  def createExtraData(round: Int, parentHeader: BlockHeader): ByteString = {
    val voteTally = protocolContext
      .getConsensusState(classOf[IbftContext])
      .getVoteTallyCache()
      .getVoteTallyAfterBlock(parentHeader);

    val proposal = protocolContext
      .getConsensusState(classOf[IbftContext])
      .getVoteProposer()
      .getVote(localAddress, voteTally);

    val validators = new ArrayList[Address](voteTally.getValidators());

    val extraData = new IbftExtraData(
      ConsensusHelpers.zeroLeftPad(vanityData, IbftExtraData.EXTRA_VANITY_LENGTH),
      Collections.emptyList(),
      toVote(proposal),
      round,
      validators
    );

    return extraData.encode();
  }

  def getLocalAddress(): Address = localAddress;

}
