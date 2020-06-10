package khipu.consensus.ibft.validation

import java.util.Collection
import khipu.consensus.ibft.ConsensusRoundIdentifier
import khipu.consensus.ibft.payload.RoundChangePayload
import khipu.consensus.ibft.payload.SignedData
import khipu.domain.Address

class RoundChangePayloadValidator(
    messageValidatorFactory: MessageValidatorForHeightFactory,
    validators:              Collection[Address],
    minimumPrepareMessages:  Long,
    chainHeight:             Long
) {

  def validateRoundChange(msg: SignedData[RoundChangePayload]): Boolean = {

    if (!validators.contains(msg.getAuthor())) {
      //LOG.info(
      //    "Invalid RoundChange message, was not transmitted by a validator for the associated"
      //        + " round.");
      return false;
    }

    val targetRound = msg.getPayload().getRoundIdentifier();

    if (targetRound.getSequenceNumber() != chainHeight) {
      //LOG.info("Invalid RoundChange message, not valid for local chain height.");
      return false;
    }

    if (msg.getPayload().getPreparedCertificate().isPresent()) {
      val certificate = msg.getPayload().getPreparedCertificate().get();

      return validatePrepareCertificate(certificate, targetRound);
    }

    return true;
  }

  private def validatePrepareCertificate(certificate: PreparedCertificate, roundChangeTarget: ConsensusRoundIdentifier): Boolean = {
    val proposalMessage = certificate.getProposalPayload();

    val proposalRoundIdentifier = proposalMessage.getPayload().getRoundIdentifier();

    if (!validatePreparedCertificateRound(proposalRoundIdentifier, roundChangeTarget)) {
      return false;
    }

    val signedDataValidator = messageValidatorFactory.createAt(proposalRoundIdentifier);
    return validateConsistencyOfPrepareCertificateMessages(certificate, signedDataValidator);
  }

  private def validateConsistencyOfPrepareCertificateMessages(certificate: PreparedCertificate, signedDataValidator: SignedDataValidator): Boolean = {

    if (!signedDataValidator.validateProposal(certificate.getProposalPayload())) {
      //LOG.info("Invalid RoundChange message, embedded Proposal message failed validation.");
      return false;
    }

    if (certificate.getPreparePayloads().size() < minimumPrepareMessages) {
      //LOG.info(
      //    "Invalid RoundChange message, insufficient Prepare messages exist to justify "
      //        + "prepare certificate.");
      return false;
    }

    for (prepareMsg <- certificate.getPreparePayloads()) {
      if (!signedDataValidator.validatePrepare(prepareMsg)) {
        //LOG.info("Invalid RoundChange message, embedded Prepare message failed validation.");
        return false;
      }
    }

    return true;
  }

  private def validatePreparedCertificateRound(prepareCertRound: ConsensusRoundIdentifier, roundChangeTarget: ConsensusRoundIdentifier): Boolean = {

    if (prepareCertRound.getSequenceNumber() != roundChangeTarget.getSequenceNumber()) {
      //LOG.info("Invalid RoundChange message, PreparedCertificate is not for local chain height.");
      return false;
    }

    if (prepareCertRound.getRoundNumber() >= roundChangeTarget.getRoundNumber()) {
      //LOG.info(
      //    "Invalid RoundChange message, PreparedCertificate not older than RoundChange target.");
      return false;
    }
    return true;
  }

  @FunctionalInterface
  trait MessageValidatorForHeightFactory {
    def createAt(roundIdentifier: ConsensusRoundIdentifier): SignedDataValidator
  }
}
