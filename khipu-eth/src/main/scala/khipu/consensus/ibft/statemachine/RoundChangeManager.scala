package khipu.consensus.ibft.statemachine

import org.hyperledger.besu.consensus.ibft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.ibft.messagewrappers.RoundChange;
import org.hyperledger.besu.consensus.ibft.validation.RoundChangeMessageValidator;
import org.hyperledger.besu.ethereum.core.Address;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import khipu.consensus.ibft.ConsensusRoundIdentifier
import khipu.consensus.ibft.messagewrappers.RoundChange
import khipu.domain.Address
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Responsible for handling all RoundChange messages received for a given block height
 * (theoretically, RoundChange messages for a older height should have been previously discarded,
 * and messages for a future round should have been buffered).
 *
 * <p>If enough RoundChange messages all targeting a given round are received (and this node is the
 * proposer for said round) - a proposal message is sent, and a new round should be started by the
 * controlling class.
 */

object RoundChangeManager {
  class RoundChangeStatus(quorum: Long) {

    // Store only 1 round change per round per validator
    @VisibleForTesting val receivedMessages: Map[Address, RoundChange] = Maps.newLinkedHashMap();

    private var actioned = false;

    def addMessage(msg: RoundChange) {
      if (!actioned) {
        receivedMessages.putIfAbsent(msg.getAuthor(), msg);
      }
    }

    def roundChangeReady(): Boolean = {
      return receivedMessages.size() >= quorum && !actioned;
    }

    def createRoundChangeCertificate(): Collection[RoundChange] = {
      if (roundChangeReady()) {
        actioned = true;
        return receivedMessages.values();
      } else {
        throw new IllegalStateException("Unable to create RoundChangeCertificate at this time.");
      }
    }
  }

}
class RoundChangeManager(quorum: Long, roundChangeMessageValidator: RoundChangeMessageValidator) {

  @VisibleForTesting
  val roundChangeCache: Map[ConsensusRoundIdentifier, RoundChangeStatus] = Maps.newHashMap();

  /**
   * Adds the round message to this manager and return a certificate if it passes the threshold
   *
   * @param msg The signed round change message to add
   * @return Empty if the round change threshold hasn't been hit, otherwise a round change
   *     certificate
   */
  def appendRoundChangeMessage(msg: RoundChange): Option[Collection[RoundChange]] = {
    if (!isMessageValid(msg)) {
      LOG.info("RoundChange message was invalid.");
      return None
    }

    val roundChangeStatus = storeRoundChangeMessage(msg);

    if (roundChangeStatus.roundChangeReady()) {
      return Option(roundChangeStatus.createRoundChangeCertificate());
    }

    return None
  }

  private def isMessageValid(msg: RoundChange): Boolean = {
    roundChangeMessageValidator.validateRoundChange(msg);
  }

  private def storeRoundChangeMessage(msg: RoundChange): RoundChangeStatus = {
    val msgTargetRound = msg.getRoundIdentifier();

    val roundChangeStatus = roundChangeCache.computeIfAbsent(msgTargetRound, ignored -> new RoundChangeStatus(quorum));

    roundChangeStatus.addMessage(msg);

    return roundChangeStatus;
  }

  /**
   * Clears old rounds from storage that have been superseded by a given round
   *
   * @param completedRoundIdentifier round identifier that has been identified as superseded
   */
  def discardRoundsPriorTo(completedRoundIdentifier: ConsensusRoundIdentifier) {
    roundChangeCache.keySet().removeIf(k -> isAnEarlierRound(k, completedRoundIdentifier));
  }

  private def isAnEarlierRound(left: ConsensusRoundIdentifier, right: ConsensusRoundIdentifier): Boolean = {
    return left.getRoundNumber() < right.getRoundNumber();
  }
}
