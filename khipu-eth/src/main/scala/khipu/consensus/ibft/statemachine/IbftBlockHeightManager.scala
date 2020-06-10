package khipu.consensus.ibft.statemachine

import static org.hyperledger.besu.consensus.ibft.statemachine.IbftBlockHeightManager.MessageAge.CURRENT_ROUND;
import static org.hyperledger.besu.consensus.ibft.statemachine.IbftBlockHeightManager.MessageAge.FUTURE_ROUND;
import static org.hyperledger.besu.consensus.ibft.statemachine.IbftBlockHeightManager.MessageAge.PRIOR_ROUND;


import khipu.consensus.ibft.ConsensusRoundIdentifier
import khipu.consensus.ibft.ibftevent.RoundExpiry
import khipu.consensus.ibft.messagewrappers.IbftMessage
import khipu.consensus.ibft.messagewrappers.Proposal
import khipu.consensus.ibft.messagewrappers.RoundChange
import khipu.consensus.ibft.payload.Payload
import khipu.domain.BlockHeader
import org.hyperledger.besu.consensus.ibft.network.IbftMessageTransmitter;
import org.hyperledger.besu.consensus.ibft.payload.MessageFactory;

import org.hyperledger.besu.consensus.ibft.validation.FutureRoundProposalMessageValidator;
import org.hyperledger.besu.consensus.ibft.validation.MessageValidatorFactory;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.plugin.services.securitymodule.SecurityModuleException;

import java.time.Clock;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.collect.Maps;

/**
 * Responsible for starting/clearing Consensus rounds at a given block height. One of these is
 * created when a new block is imported to the chain. It immediately then creates a Round-0 object,
 * and sends a Proposal message. If the round times out prior to importing a block, this class is
 * responsible for creating a RoundChange message and transmitting it.
 */
object IbftBlockHeightManager {
    trait MessageAge
    object MessageAge {
      case object PRIOR_ROUND extends MessageAge
    case object CURRENT_ROUND extends MessageAge
    case object FUTURE_ROUND extends MessageAge 
  }
}
class IbftBlockHeightManager(
      parentHeader: BlockHeader ,
      finalState: IbftFinalState ,
      roundChangeManager: RoundChangeManager ,
      roundFactory: IbftRoundFactory ,
      clock: Clock ,
      messageValidatorFactory: MessageValidatorFactory ) extends BlockHeightManager {

  import IbftBlockHeightManager.MessageAge

  private val blockTimer = finalState.getBlockTimer();
  private val transmitter= finalState.getTransmitter();
  private val messageFactory = finalState.getMessageFactory()
  private val futureRoundStateBuffer: Map[Integer, RoundState] = Maps.newHashMap();
  private val futureRoundProposalMessageValidator =
        messageValidatorFactory.createFutureRoundProposalMessageValidator(
            getChainHeight(), parentHeader);
  private val roundStateCreator = {
        (roundIdentifier) =>
            new RoundState(
                roundIdentifier,
                finalState.getQuorum(),
                messageValidatorFactory.createMessageValidator(roundIdentifier, parentHeader));
  }


  private var latestPreparedRoundArtifacts: Option[PreparedRoundArtifacts] = None

  private val currentRound = roundFactory.createNewRound(parentHeader, 0);
  
  if (finalState.isLocalNodeProposerForRound(currentRound.getRoundIdentifier())) {
      blockTimer.startTimer(currentRound.getRoundIdentifier(), parentHeader);
  }
  

  def handleBlockTimerExpiry(roundIdentifier:ConsensusRoundIdentifier ) {
    if (roundIdentifier.equals(currentRound.getRoundIdentifier())) {
      currentRound.createAndSendProposalMessage(clock.millis() / 1000);
    } else {
      LOG.trace(
          "Block timer expired for a round ({}) other than current ({})",
          roundIdentifier,
          currentRound.getRoundIdentifier());
    }
  }

  def roundExpired(expire: RoundExpiry ) {
    if (!expire.getView().equals(currentRound.getRoundIdentifier())) {
      LOG.trace(
          "Ignoring Round timer expired which does not match current round. round={}, timerRound={}",
          currentRound.getRoundIdentifier(),
          expire.getView());
      return;
    }

    LOG.debug(
        "Round has expired, creating PreparedCertificate and notifying peers. round={}",
        currentRound.getRoundIdentifier());
    final Optional<PreparedRoundArtifacts> preparedRoundArtifacts =
        currentRound.constructPreparedRoundArtifacts();

    if (preparedRoundArtifacts.isPresent()) {
      latestPreparedRoundArtifacts = preparedRoundArtifacts;
    }

    startNewRound(currentRound.getRoundIdentifier().getRoundNumber() + 1);

    try {
      val localRoundChange = messageFactory.createRoundChange(
              currentRound.getRoundIdentifier(), latestPreparedRoundArtifacts);

      // Its possible the locally created RoundChange triggers the transmission of a NewRound
      // message - so it must be handled accordingly.
      handleRoundChangePayload(localRoundChange);
    } catch {
      case e: SecurityModuleException =>
      LOG.warn("Failed to create signed RoundChange message.", e);
    }

    transmitter.multicastRoundChange(
        currentRound.getRoundIdentifier(), latestPreparedRoundArtifacts);
  }

  def handleProposalPayload(proposal:Proposal ) {
    LOG.trace("Received a Proposal Payload.");
    val messageAge =
        determineAgeOfPayload(proposal.getRoundIdentifier().getRoundNumber());

    if (messageAge == MessageAge.PRIOR_ROUND) {
      LOG.trace("Received Proposal Payload for a prior round={}", proposal.getRoundIdentifier());
    } else {
      if (messageAge == MessageAge.FUTURE_ROUND) {
        if (!futureRoundProposalMessageValidator.validateProposalMessage(proposal)) {
          LOG.info("Received future Proposal which is illegal, no round change triggered.");
          return;
        }
        startNewRound(proposal.getRoundIdentifier().getRoundNumber());
      }
      currentRound.handleProposalMessage(proposal);
    }
  }

  def handlePreparePayload(prepare:Prepare ) {
    LOG.trace("Received a Prepare Payload.");
    actionOrBufferMessage( prepare, currentRound::handlePrepareMessage, RoundState::addPrepareMessage);
  }

  def handleCommitPayload(commit:Commit ) {
    LOG.trace("Received a Commit Payload.");
    actionOrBufferMessage(commit, currentRound::handleCommitMessage, RoundState::addCommitMessage);
  }

  private  def actionOrBufferMessage[P <: Payload, M <: IbftMessage[P]](
      ibftMessage:M ,
      inRoundHandler:Consumer[M] ,
      buffer:BiConsumer[RoundState, M] ) {
    val messageAge = determineAgeOfPayload(ibftMessage.getRoundIdentifier().getRoundNumber());
    if (messageAge == MessageAge.CURRENT_ROUND) {
      inRoundHandler.accept(ibftMessage);
    } else if (messageAge == MessageAge.FUTURE_ROUND) {
      val msgRoundId = ibftMessage.getRoundIdentifier();
      val roundstate =
          futureRoundStateBuffer.computeIfAbsent(
              msgRoundId.getRoundNumber(), k -> roundStateCreator.apply(msgRoundId));
      buffer.accept(roundstate, ibftMessage);
    }
  }

  def handleRoundChangePayload(message:RoundChange ) {
    val targetRound = message.getRoundIdentifier();
    LOG.trace("Received a RoundChange Payload for {}", targetRound);

    val messageAge =
        determineAgeOfPayload(message.getRoundIdentifier().getRoundNumber());
    if (messageAge == MessageAge.PRIOR_ROUND) {
      LOG.trace("Received RoundChange Payload for a prior round. targetRound={}", targetRound);
      return;
    }

    val  result = roundChangeManager.appendRoundChangeMessage(message);
    if (result.isPresent()) {
      LOG.debug(
          "Received sufficient RoundChange messages to change round to targetRound={}",
          targetRound);
      if (messageAge == MessageAge.FUTURE_ROUND) {
        startNewRound(targetRound.getRoundNumber());
      }

      val roundChangeArtifacts = RoundChangeArtifacts.create(result.get());

      if (finalState.isLocalNodeProposerForRound(targetRound)) {
        currentRound.startRoundWith(
            roundChangeArtifacts, TimeUnit.MILLISECONDS.toSeconds(clock.millis()));
      }
    }
  }

  private def startNewRound(roundNumber:Int) {
    LOG.debug("Starting new round {}", roundNumber);
    if (futureRoundStateBuffer.containsKey(roundNumber)) {
      currentRound =
          roundFactory.createNewRoundWithState(
              parentHeader, futureRoundStateBuffer.get(roundNumber));
      futureRoundStateBuffer.keySet().removeIf(k -> k <= roundNumber);
    } else {
      currentRound = roundFactory.createNewRound(parentHeader, roundNumber);
    }
    // discard roundChange messages from the current and previous rounds
    roundChangeManager.discardRoundsPriorTo(currentRound.getRoundIdentifier());
  }

  def getChainHeight(): Long = {
    parentHeader.getNumber() + 1;
  }

  def getParentBlockHeader(): BlockHeader =  {
    parentHeader;
  }

  private def determineAgeOfPayload(messageRoundNumber: Int):MessageAge = {
    val currentRoundNumber = currentRound.getRoundIdentifier().getRoundNumber();
    if (messageRoundNumber > currentRoundNumber) {
      return MessageAge.FUTURE_ROUND;
    } else if (messageRoundNumber == currentRoundNumber) {
      return MessageAge.CURRENT_ROUND;
    }
    return MessageAge.PRIOR_ROUND;
  }


}
