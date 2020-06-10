package khipu.consensus.ibft.statemachine

import khipu.domain.BlockHeader
import org.hyperledger.besu.consensus.ibft.IbftHelpers;
import org.hyperledger.besu.consensus.ibft.validation.MessageValidatorFactory;
import org.hyperledger.besu.ethereum.core.BlockHeader;

class IbftBlockHeightManagerFactory(
    finalState:              IbftFinalState,
    roundFactory:            IbftRoundFactory,
    messageValidatorFactory: MessageValidatorFactory
) {

  def create(parentHeader: BlockHeader): BlockHeightManager = {
    if (finalState.isLocalNodeValidator()) {
      return createFullBlockHeightManager(parentHeader);
    } else {
      return createNoOpBlockHeightManager(parentHeader);
    }
  }

  private def createNoOpBlockHeightManager(parentHeader: BlockHeader): BlockHeightManager = {
    return new NoOpBlockHeightManager(parentHeader);
  }

  private def createFullBlockHeightManager(parentHeader: BlockHeader): BlockHeightManager = {
    return new IbftBlockHeightManager(
      parentHeader,
      finalState,
      new RoundChangeManager(
        IbftHelpers.calculateRequiredValidatorQuorum(finalState.getValidators().size()),
        messageValidatorFactory.createRoundChangeMessageValidator(
          parentHeader.getNumber() + 1L, parentHeader
        )
      ),
      roundFactory,
      finalState.getClock(),
      messageValidatorFactory
    );
  }
}
