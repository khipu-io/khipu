package khipu.consensus.ibft.statemachine

import org.hyperledger.besu.consensus.common.VoteTallyCache;
import org.hyperledger.besu.consensus.ibft.blockcreation.IbftBlockCreatorFactory;
import org.hyperledger.besu.consensus.ibft.blockcreation.ProposerSelector;
import org.hyperledger.besu.consensus.ibft.network.IbftMessageTransmitter;
import org.hyperledger.besu.consensus.ibft.network.ValidatorMulticaster;
import org.hyperledger.besu.crypto.NodeKey;
import org.hyperledger.besu.ethereum.core.Address;

import java.time.Clock;
import java.util.Collection;

/** This is the full data set, or context, required for many of the aspects of the IBFT workflow. */
class IbftFinalState(
      voteTallyCache:VoteTallyCache ,
      nodeKey:NodeKey ,
      localAddress: Address ,
      proposerSelector: ProposerSelector ,
      validatorMulticaster: ValidatorMulticaster ,
      roundTimer:RoundTimer ,
      blockTimer:BlockTimer ,
      blockCreatorFactory: IbftBlockCreatorFactory ,
      messageFactory:MessageFactory ,
      clock:Clock ) {

  private messageTransmitter = new IbftMessageTransmitter(messageFactory, validatorMulticaster);


  def getQuorum(): Int = {
    return IbftHelpers.calculateRequiredValidatorQuorum(getValidators().size());
  }

  def getValidators():Collection[Address] =  {
    return voteTallyCache.getVoteTallyAtHead().getValidators();
  }

  def getNodeKey():NodeKey =  {
    return nodeKey;
  }

  def getLocalAddress():Address =  {
    return localAddress;
  }

  def isLocalNodeProposerForRound(roundIdentifier:ConsensusRoundIdentifier ): Boolean = {
    return getProposerForRound(roundIdentifier).equals(localAddress);
  }

  def isLocalNodeValidator():Boolean = {
    return getValidators().contains(localAddress);
  }

  def getRoundTimer():RoundTimer =  {
    roundTimer;
  }

  def getBlockTimer():BlockTimer=   {
    return blockTimer;
  }

  def getBlockCreatorFactory():IbftBlockCreatorFactory =  {
    return blockCreatorFactory;
  }

  def getMessageFactory():MessageFactory =  {
    return messageFactory;
  }

  def getProposerForRound(roundIdentifier:ConsensusRoundIdentifier ):Address=   {
    return proposerSelector.selectProposerForRound(roundIdentifier);
  }

  def getTransmitter():IbftMessageTransmitter=  {
    return messageTransmitter;
  }

  def getClock():Clock = {
    return clock;
  }
}
