package khipu.consensus.ibft.validation

import khipu.consensus.ibft.messagewrappers.Proposal
import khipu.domain.BlockHeader

/* One of these will be created by the IbftBlockHeightManager and will exist for the life of the
chainheight, and used to ensure supplied Proposals are suitable for starting a new round.
 */
class FutureRoundProposalMessageValidator(
    messageValidatorFactory: MessageValidatorFactory,
    chainHeight:             Long,
    parentHeader:            BlockHeader
) {

  def validateProposalMessage(msg: Proposal): Boolean = {

    if (msg.getRoundIdentifier().getSequenceNumber() != chainHeight) {
      //LOG.info("Illegal Proposal message, does not target the correct round height.");
      return false;
    }

    val messageValidator = messageValidatorFactory.createMessageValidator(msg.getRoundIdentifier(), parentHeader);

    return messageValidator.validateProposal(msg);
  }
}
