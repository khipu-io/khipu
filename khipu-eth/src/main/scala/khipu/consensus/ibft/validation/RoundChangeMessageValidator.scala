package khipu.consensus.ibft.validation

import khipu.consensus.ibft.messagewrappers.RoundChange

class RoundChangeMessageValidator(
    roundChangePayloadValidator:       RoundChangePayloadValidator,
    proposalBlockConsistencyValidator: ProposalBlockConsistencyValidator
) {

  def validateRoundChange(msg: RoundChange): Boolean = {

    if (!roundChangePayloadValidator.validateRoundChange(msg.getSignedPayload())) {
      //LOG.info("Invalid RoundChange message, signed data did not validate correctly.");
      return false;
    }

    if (msg.getPreparedCertificate().isDefined != msg.getProposedBlock().isPresent()) {
      //LOG.info(
      //    "Invalid RoundChange message, availability of certificate does not correlate with availability of block.");
      return false;
    }

    if (msg.getPreparedCertificate().isDefined) {
      if (!proposalBlockConsistencyValidator.validateProposalMatchesBlock(
        msg.getPreparedCertificate().get.getProposalPayload(), msg.getProposedBlock().get()
      )) {
        //LOG.info("Invalid RoundChange message, proposal did not align with supplied block.");
        return false;
      }
    }

    return true;
  }
}
