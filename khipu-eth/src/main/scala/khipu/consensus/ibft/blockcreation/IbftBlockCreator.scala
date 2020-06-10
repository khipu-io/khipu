package khipu.consensus.ibft.blockcreation

import khipu.domain.BlockHeader
import org.hyperledger.besu.consensus.ibft.IbftBlockHeaderFunctions;
import org.hyperledger.besu.consensus.ibft.IbftHelpers;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.blockcreation.AbstractBlockCreator;
import org.hyperledger.besu.ethereum.core.Address;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.SealableBlockHeader;
import org.hyperledger.besu.ethereum.core.Wei;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.util.function.Function;

// This class is responsible for creating a block without committer seals (basically it was just
// too hard to coordinate with the state machine).
class IbftBlockCreator(
  localAddress:           Address,
  extraDataCalculator:    ExtraDataCalculator,
  pendingTransactions:    PendingTransactions,
  protocolContext:        ProtocolContext,
  protocolSchedule:       ProtocolSchedule,
  gasLimitCalculator:     Function[Long, Long],
  minTransactionGasPrice: Wei,
  minBlockOccupancyRatio: Double,
  parentHeader:           BlockHeader
) extends AbstractBlockCreator(
  localAddress,
  extraDataCalculator,
  pendingTransactions,
  protocolContext,
  protocolSchedule,
  gasLimitCalculator,
  minTransactionGasPrice,
  localAddress,
  minBlockOccupancyRatio,
  parentHeader
) {

  override protected def createFinalBlockHeader(sealableBlockHeader: SealableBlockHeader): BlockHeader = {
    val builder = BlockHeaderBuilder.create()
      .populateFrom(sealableBlockHeader)
      .mixHash(IbftHelpers.EXPECTED_MIX_HASH)
      .nonce(0L)
      .blockHeaderFunctions(IbftBlockHeaderFunctions.forCommittedSeal());

    return builder.buildBlockHeader();
  }
}
