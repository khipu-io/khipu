package khipu.consensus.ibft.statemachine

import khipu.domain.BlockHeader
import org.hyperledger.besu.consensus.ibft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.ibft.blockcreation.IbftBlockCreator;
import org.hyperledger.besu.consensus.ibft.blockcreation.IbftBlockCreatorFactory;
import org.hyperledger.besu.consensus.ibft.validation.MessageValidatorFactory;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MinedBlockObserver;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.util.Subscribers;

class IbftRoundFactory(
    finalState:              IbftFinalState,
    protocolContext:         ProtocolContext,
    protocolSchedule:        ProtocolSchedule,
    minedBlockObservers:     Subscribers[MinedBlockObserver],
    messageValidatorFactory: MessageValidatorFactory
) {
  private val blockCreatorFactory = finalState.getBlockCreatorFactory();

  def createNewRound(parentHeader: BlockHeader, round: Int): IbftRound = {
    val nextBlockHeight = parentHeader.getNumber() + 1;
    val roundIdentifier = new ConsensusRoundIdentifier(nextBlockHeight, round);

    val roundState =
      new RoundState(
        roundIdentifier,
        finalState.getQuorum(),
        messageValidatorFactory.createMessageValidator(roundIdentifier, parentHeader)
      );

    return createNewRoundWithState(parentHeader, roundState);
  }

  def createNewRoundWithState(parentHeader: BlockHeader, roundState: RoundState): IbftRound = {
    val roundIdentifier = roundState.getRoundIdentifier();
    val blockCreator = blockCreatorFactory.create(parentHeader, roundIdentifier.getRoundNumber());

    return new IbftRound(
      roundState,
      blockCreator,
      protocolContext,
      protocolSchedule.getByBlockNumber(roundIdentifier.getSequenceNumber()).getBlockImporter(),
      minedBlockObservers,
      finalState.getNodeKey(),
      finalState.getMessageFactory(),
      finalState.getTransmitter(),
      finalState.getRoundTimer()
    );
  }
}
