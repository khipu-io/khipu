package khipu.consensus.ibft.validation

import khipu.consensus.ibft.ConsensusRoundIdentifier
import khipu.consensus.ibft.payload.CommitPayload
import khipu.consensus.ibft.payload.Payload
import khipu.consensus.ibft.payload.PreparePayload
import khipu.consensus.ibft.payload.ProposalPayload
import khipu.consensus.ibft.payload.SignedData
import khipu.domain.Address
import org.hyperledger.besu.ethereum.core.Hash;
import org.hyperledger.besu.ethereum.core.Util;

import java.util.Collection;
import java.util.Optional;

class SignedDataValidator(
      validators:Collection[Address] ,
      expectedProposer:Address ,
      roundIdentifier:ConsensusRoundIdentifier )  {

    private var proposal: Option[SignedData[ProposalPayload]] = None

  def validateProposal(msg:SignedData[ProposalPayload] ): Boolean = {

    if (proposal.isDefined) {
      return handleSubsequentProposal(proposal.get, msg);
    }

    if (!validateProposalSignedDataPayload(msg)) {
      return false;
    }

    proposal = Optional.of(msg);
    return true;
  }

  private def validateProposalSignedDataPayload(msg:SignedData[ProposalPayload] ):Boolean =  {

    if (!msg.getPayload().getRoundIdentifier().equals(roundIdentifier)) {
      //LOG.info("Invalid Proposal message, does not match current round.");
      return false;
    }

    if (!msg.getAuthor().equals(expectedProposer)) {
      //LOG.info(
      //    "Invalid Proposal message, was not created by the proposer expected for the "
      //        + "associated round.");
      return false;
    }

    return true;
  }

  private def handleSubsequentProposal( existingMsg:SignedData[ProposalPayload] , newMsg:SignedData[ProposalPayload] ):Boolean =  {
    if (!existingMsg.getAuthor().equals(newMsg.getAuthor())) {
      //LOG.info("Received subsequent invalid Proposal message; sender differs from original.");
      return false;
    }

    val existingData = existingMsg.getPayload();
    val newData = newMsg.getPayload();

    if (!proposalMessagesAreIdentical(existingData, newData)) {
      //LOG.info("Received subsequent invalid Proposal message; content differs from original.");
      return false;
    }

    return true;
  }

  def validatePrepare(msg: SignedData[PreparePayload] ):Boolean = {
    val msgType = "Prepare";

    if (!isMessageForCurrentRoundFromValidatorAndProposalAvailable(msg, msgType)) {
      return false;
    }

    if (msg.getAuthor().equals(expectedProposer)) {
      //LOG.info("Illegal Prepare message; was sent by the round's proposer.");
      return false;
    }

    return validateDigestMatchesProposal(msg.getPayload().getDigest(), msgType);
  }

  def validateCommit(msg: SignedData[CommitPayload] ):Boolean = {
    val msgType = "Commit";

    if (!isMessageForCurrentRoundFromValidatorAndProposalAvailable(msg, msgType)) {
      return false;
    }

    val proposedBlockDigest = proposal.get().getPayload().getDigest();
    val commitSealCreator = Util.signatureToAddress(msg.getPayload().getCommitSeal(), proposedBlockDigest);

    if (!commitSealCreator.equals(msg.getAuthor())) {
      //LOG.info("Invalid Commit message. Seal was not created by the message transmitter.");
      return false;
    }

    return validateDigestMatchesProposal(msg.getPayload().getDigest(), msgType);
  }

  private def isMessageForCurrentRoundFromValidatorAndProposalAvailable(msg:SignedData[_ <: Payload] , msgType: String ): Boolean = {

    if (!msg.getPayload().getRoundIdentifier().equals(roundIdentifier)) {
      //LOG.info("Invalid {} message, does not match current round.", msgType);
      return false;
    }

    if (!validators.contains(msg.getAuthor())) {
      //LOG.info(
      //    "Invalid {} message, was not transmitted by a validator for the " + "associated round.",
      //    msgType);
      return false;
    }

    if (!proposal.isPresent()) {
      //LOG.info(
      //    "Unable to validate {} message. No Proposal exists against which to validate "
      //        + "block digest.",
      //    msgType);
      return false;
    }
    return true;
  }

  private def validateDigestMatchesProposal(digest:Hash , msgType:String ):Boolean = {
    val proposedBlockDigest = proposal.get().getPayload().getDigest();
    if (!digest.equals(proposedBlockDigest)) {
      //LOG.info(
      //    "Illegal {} message, digest does not match the digest in the Prepare Message.", msgType);
      return false;
    }
    return true;
  }

  private def proposalMessagesAreIdentical(right:ProposalPayload , left:ProposalPayload ): Boolean = {
    return right.getDigest().equals(left.getDigest())
        && right.getRoundIdentifier().equals(left.getRoundIdentifier());
  }
}
