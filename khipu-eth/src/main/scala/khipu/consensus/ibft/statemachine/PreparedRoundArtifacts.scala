package khipu.consensus.ibft.statemachine

import khipu.consensus.ibft.messagewrappers.Prepare
import khipu.consensus.ibft.messagewrappers.Proposal
import khipu.consensus.ibft.payload.PreparedCertificate
import org.hyperledger.besu.ethereum.core.Block;

import java.util.Collection;
import java.util.stream.Collectors;

class PreparedRoundArtifacts(proposal: Proposal, prepares: Collection[Prepare]) {

  def getBlock(): Block = proposal.getBlock();

  def getPreparedCertificate(): PreparedCertificate = {
    new PreparedCertificate(
      proposal.getSignedPayload(),
      prepares.stream().map(Prepare :: getSignedPayload).collect(Collectors.toList())
    );
  }
}
