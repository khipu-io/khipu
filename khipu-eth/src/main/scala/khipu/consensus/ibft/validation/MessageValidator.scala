package khipu.consensus.ibft.validation

import khipu.consensus.ibft.messagewrappers.Commit
import khipu.consensus.ibft.messagewrappers.Prepare
import khipu.consensus.ibft.messagewrappers.Proposal
import org.hyperledger.besu.ethereum.BlockValidator;
import org.hyperledger.besu.ethereum.BlockValidator.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;

import java.util.Optional;

class MessageValidator(
    signedDataValidator:             SignedDataValidator,
    proposalConsistencyValidator:    ProposalBlockConsistencyValidator,
    blockValidator:                  BlockValidator,
    protocolContext:                 ProtocolContext,
    roundChangeCertificateValidator: RoundChangeCertificateValidator
) {

  def validateProposal(msg: Proposal): boolean = {

    if (!signedDataValidator.validateProposal(msg.getSignedPayload())) {
      //LOG.info("Illegal Proposal message, embedded signed data failed validation");
      return false;
    }

    if (!validateBlock(msg.getBlock())) {
      return false;
    }

    if (!validateProposalAndRoundChangeAreConsistent(msg)) {
      //LOG.info("Illegal Proposal message, embedded roundChange does not match proposal.");
      return false;
    }

    return proposalConsistencyValidator.validateProposalMatchesBlock(
      msg.getSignedPayload(), msg.getBlock()
    );
  }

  private def validateBlock(block: Block): Boolean = {
    final val validationResult = blockValidator.validateAndProcessBlock(
      protocolContext, block, HeaderValidationMode.LIGHT, HeaderValidationMode.FULL
    );

    if (!validationResult.isPresent()) {
      //LOG.info("Invalid Proposal message, block did not pass validation.");
      return false;
    }

    return true;
  }

  private def validateProposalAndRoundChangeAreConsistent(proposal: Proposal): Boolean = {
    val proposalRoundIdentifier = proposal.getRoundIdentifier();

    if (proposalRoundIdentifier.getRoundNumber() == 0) {
      validateRoundZeroProposalHasNoRoundChangeCertificate(proposal);
    } else {
      validateRoundChangeCertificateIsValidAndMatchesProposedBlock(proposal);
    }
  }

  private def validateRoundZeroProposalHasNoRoundChangeCertificate(proposal: Proposal): Boolean = {
    if (proposal.getRoundChangeCertificate().isPresent()) {
      //LOG.info(
      //    "Illegal Proposal message, round-0 proposal must not contain a round change certificate.");
      return false;
    }
    return true;
  }

  private def validateRoundChangeCertificateIsValidAndMatchesProposedBlock(proposal: Proposal): Boolean = {

    val roundChangeCertificate = proposal.getRoundChangeCertificate();

    if (!roundChangeCertificate.isPresent()) {
      //LOG.info(
      //    "Illegal Proposal message, rounds other than 0 must contain a round change certificate.");
      return false;
    }

    val roundChangeCert = roundChangeCertificate.get();

    if (!roundChangeCertificateValidator.validateRoundChangeMessagesAndEnsureTargetRoundMatchesRoot(
      proposal.getRoundIdentifier(), roundChangeCert
    )) {
      //LOG.info("Illegal Proposal message, embedded RoundChangeCertificate is not self-consistent");
      return false;
    }

    if (!roundChangeCertificateValidator.validateProposalMessageMatchesLatestPrepareCertificate(
      roundChangeCert, proposal.getBlock()
    )) {
      //LOG.info(
      //    "Illegal Proposal message, piggybacked block does not match latest PrepareCertificate");
      return false;
    }

    return true;
  }

  def validatePrepare(msg: Prepare): Boolean = {
    signedDataValidator.validatePrepare(msg.getSignedPayload());
  }

  def validateCommit(msg: Commit): Boolean = {
    signedDataValidator.validateCommit(msg.getSignedPayload());
  }
}
