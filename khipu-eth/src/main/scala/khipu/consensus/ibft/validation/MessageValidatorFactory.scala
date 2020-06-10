/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package khipu.consensus.ibft.validation

import khipu.domain.Address
import khipu.domain.BlockHeader
import org.hyperledger.besu.consensus.ibft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.ibft.IbftContext;
import org.hyperledger.besu.consensus.ibft.IbftHelpers;
import org.hyperledger.besu.consensus.ibft.blockcreation.ProposerSelector;
import org.hyperledger.besu.ethereum.BlockValidator;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Address;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.util.Collection;

class MessageValidatorFactory(
    proposerSelector: ProposerSelector,
    protocolSchedule: ProtocolSchedule,
    protocolContext:  ProtocolContext
) {

  private def getValidatorsAfterBlock(parentHeader: BlockHeader): Collection[Address] = {
    return protocolContext
      .getConsensusState(classOf[IbftContext])
      .getVoteTallyCache()
      .getVoteTallyAfterBlock(parentHeader)
      .getValidators();
  }

  private def createSignedDataValidator(roundIdentifier: ConsensusRoundIdentifier, parentHeader: BlockHeader): SignedDataValidator = {
    return new SignedDataValidator(
      getValidatorsAfterBlock(parentHeader),
      proposerSelector.selectProposerForRound(roundIdentifier),
      roundIdentifier
    );
  }

  def createMessageValidator(roundIdentifier: ConsensusRoundIdentifier, parentHeader: BlockHeader): MessageValidator = {
    val blockValidator = protocolSchedule.getByBlockNumber(roundIdentifier.getSequenceNumber()).getBlockValidator();
    val validators = getValidatorsAfterBlock(parentHeader);

    return new MessageValidator(
      createSignedDataValidator(roundIdentifier, parentHeader),
      new ProposalBlockConsistencyValidator(),
      blockValidator,
      protocolContext,
      new RoundChangeCertificateValidator(
        validators,
        (ri) -> createSignedDataValidator(ri, parentHeader),
        roundIdentifier.getSequenceNumber()
      )
    );
  }

  def createRoundChangeMessageValidator(chainHeight: Long, parentHeader: BlockHeader): RoundChangeMessageValidator = {
    val validators = getValidatorsAfterBlock(parentHeader);

    return new RoundChangeMessageValidator(
      new RoundChangePayloadValidator(
        (roundIdentifier) -> createSignedDataValidator(roundIdentifier, parentHeader),
        validators,
        IbftHelpers.prepareMessageCountForQuorum(
          IbftHelpers.calculateRequiredValidatorQuorum(validators.size())
        ),
        chainHeight
      ),
      new ProposalBlockConsistencyValidator()
    );
  }

  def createFutureRoundProposalMessageValidator(chainHeight: Long, parentHeader: BlockHeader): FutureRoundProposalMessageValidator = {
    new FutureRoundProposalMessageValidator(this, chainHeight, parentHeader);
  }
}
