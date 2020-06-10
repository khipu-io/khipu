/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package khipu.consensus.ibft.validation

import khipu.consensus.ibft.IbftExtraData
import khipu.consensus.ibft.payload.ProposalPayload
import khipu.consensus.ibft.payload.SignedData
import org.hyperledger.besu.ethereum.core.Block;

class ProposalBlockConsistencyValidator {

  def validateProposalMatchesBlock(signedPayload: SignedData[ProposalPayload], proposedBlock: Block): Boolean = {

    if (!signedPayload.getPayload().getDigest().equals(proposedBlock.getHash())) {
      //LOG.info("Invalid Proposal, embedded digest does not match block's hash.");
      return false;
    }

    if (proposedBlock.getHeader().getNumber()
      != signedPayload.getPayload().getRoundIdentifier().getSequenceNumber()) {
      //LOG.info("Invalid proposal/block - message sequence does not align with block number.");
      return false;
    }

    if (!validateBlockMatchesProposalRound(signedPayload.getPayload(), proposedBlock)) {
      return false;
    }

    return true;
  }

  private def validateBlockMatchesProposalRound(payload: ProposalPayload, block: Block): Boolean = {
    val msgRound = payload.getRoundIdentifier();
    val extraData = IbftExtraData.decode(block.getHeader());
    if (extraData.round != msgRound.getRoundNumber()) {
      //LOG.info("Invalid Proposal message, round number in block does not match that in message.");
      return false;
    }
    return true;
  }
}
