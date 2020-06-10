package khipu.consensus.ibft.statemachine

import org.hyperledger.besu.consensus.ibft.validation.MessageValidator;
import org.hyperledger.besu.crypto.SECP256K1.Signature;
import org.hyperledger.besu.ethereum.core.Block;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

// Data items used to define how a round will operate
class RoundState(
    roundIdentifier: ConsensusRoundIdentifier,
    quorum:          Int,
    validator:       MessageValidator
) {

  private var proposalMessage: Option[Proposal] = None

  // Must track the actual Prepare message, not just the sender, as these may need to be reused
  // to send out in a PrepareCertificate.
  private var prepareMessages: Set[Prepare] = Sets.newLinkedHashSet();
  private var commitMessages: Set[Commit] = Sets.newLinkedHashSet();

  private var prepared = false;
  private var committed = false;

  def getRoundIdentifier(): ConsensusRoundIdentifier = roundIdentifier;

  def setProposedBlock(msg: Proposal): Boolean = {
    if (!proposalMessage.isPresent()) {
      if (validator.validateProposal(msg)) {
        proposalMessage = Option(msg);
        prepareMessages.removeIf(p -> !validator.validatePrepare(p));
        commitMessages.removeIf(p -> !validator.validateCommit(p));
        updateState();
        return true;
      }
    }

    return false;
  }

  def addPrepareMessage(msg: Prepare) {
    if (!proposalMessage.isPresent() || validator.validatePrepare(msg)) {
      prepareMessages.add(msg);
      LOG.trace("Round state added prepare message prepare={}", msg);
    }
    updateState();
  }

  def addCommitMessage(msg: Commit) {
    if (!proposalMessage.isPresent() || validator.validateCommit(msg)) {
      commitMessages.add(msg);
      LOG.trace("Round state added commit message commit={}", msg);
    }

    updateState();
  }

  private def updateState() {
    // NOTE: The quorum for Prepare messages is 1 less than the quorum size as the proposer
    // does not supply a prepare message
    val prepareQuorum = IbftHelpers.prepareMessageCountForQuorum(quorum);
    prepared = (prepareMessages.size() >= prepareQuorum) && proposalMessage.isPresent();
    committed = (commitMessages.size() >= quorum) && proposalMessage.isPresent();
    LOG.trace(
      "Round state updated prepared={} committed={} preparedQuorum={}/{} committedQuorum={}/{}",
      prepared,
      committed,
      prepareMessages.size(),
      prepareQuorum,
      commitMessages.size(),
      quorum
    );
  }

  def getProposedBlock(): Option[Block] = {
    return proposalMessage.map(Proposal :: getBlock);
  }

  def isPrepared() = prepared;

  def isCommitted() = committed;

  def getCommitSeals(): Collection[Signature] = {
    return commitMessages.stream()
      .map(cp => cp.getSignedPayload().getPayload().getCommitSeal())
      .collect(Collectors.toList());
  }

  def constructPreparedRoundArtifacts(): Option[PreparedRoundArtifacts] = {
    if (isPrepared()) {
      return Option(new PreparedRoundArtifacts(proposalMessage.get(), prepareMessages));
    }
    return None
  }
}
