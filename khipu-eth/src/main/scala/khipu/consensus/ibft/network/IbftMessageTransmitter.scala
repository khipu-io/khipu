package khipu.consensus.ibft.network

import khipu.consensus.ibft.ConsensusRoundIdentifier
import khipu.consensus.ibft.messagedata.ProposalMessageData
import khipu.consensus.ibft.payload.RoundChangeCertificate
import org.hyperledger.besu.crypto.SECP256K1.Signature;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.Hash;
import org.hyperledger.besu.plugin.services.securitymodule.SecurityModuleException;

import java.util.Optional;

class IbftMessageTransmitter(messageFactory: MessageFactory, multicaster: ValidatorMulticaster) {

  def multicastProposal(
    roundIdentifier:        ConsensusRoundIdentifier,
    block:                  Block,
    roundChangeCertificate: Option[RoundChangeCertificate]
  ) {
    try {
      val data = messageFactory.createProposal(roundIdentifier, block, roundChangeCertificate);
      val message = ProposalMessageData.create(data);

      multicaster.send(message);
    } catch {
      case e: SecurityModuleException =>
      //LOG.warn("Failed to generate signature for Proposal (not sent): {} ", e.getMessage());
    }
  }

  def multicastPrepare(roundIdentifier: ConsensusRoundIdentifier, digest: Hash) {
    try {
      val data = messageFactory.createPrepare(roundIdentifier, digest);

      val message = PrepareMessageData.create(data);

      multicaster.send(message);
    } catch {
      case e: SecurityModuleException =>
      //LOG.warn("Failed to generate signature for Prepare (not sent): {} ", e.getMessage());
    }
  }

  def multicastCommit(
    roundIdentifier: ConsensusRoundIdentifier,
    digest:          Hash,
    commitSeal:      Signature
  ) {
    try {
      val data = messageFactory.createCommit(roundIdentifier, digest, commitSeal);

      val message = CommitMessageData.create(data);

      multicaster.send(message);
    } catch {
      case e: SecurityModuleException =>
      //LOG.warn("Failed to generate signature for Commit (not sent): {} ", e.getMessage());
    }
  }

  def multicastRoundChange(
    roundIdentifier:        ConsensusRoundIdentifier,
    preparedRoundArtifacts: Optional[PreparedRoundArtifacts]
  ) {
    try {
      val data = messageFactory.createRoundChange(roundIdentifier, preparedRoundArtifacts);

      val message = RoundChangeMessageData.create(data);

      multicaster.send(message);
    } catch {
      case e: SecurityModuleException =>
      //LOG.warn("Failed to generate signature for RoundChange (not sent): {} ", e.getMessage());
    }
  }
}
