package khipu.consensus.ibft.statemachine

import khipu.consensus.ibft.messagewrappers.RoundChange
import khipu.consensus.ibft.payload.RoundChangeCertificate
import khipu.consensus.ibft.payload.RoundChangePayload
import khipu.consensus.ibft.payload.SignedData
import org.hyperledger.besu.ethereum.core.Block;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

object RoundChangeArtifacts {
  def create(roundChanges: Collection[RoundChange]): RoundChangeArtifacts = {
    val preparedRoundComparator: Comparator[RoundChange] = {
      (o1, o2) =>
        {
          if (!o1.getPreparedCertificateRound().isPresent()) {
            return -1;
          }
          if (!o2.getPreparedCertificateRound().isPresent()) {
            return 1;
          }
          return o1.getPreparedCertificateRound()
            .get()
            .compareTo(o2.getPreparedCertificateRound().get());
        };
    }

    val payloads = roundChanges.stream().map(RoundChange :: getSignedPayload).collect(Collectors.toList());

    val roundChangeWithNewestPrepare = roundChanges.stream().max(preparedRoundComparator);

    val proposedBlock = roundChangeWithNewestPrepare.flatMap(RoundChange :: getProposedBlock);
    return new RoundChangeArtifacts(proposedBlock, payloads);
  }
}
class RoundChangeArtifacts(block: Option[Block], roundChangePayloads: List[SignedData[RoundChangePayload]]) {

  def getBlock() = block;

  def getRoundChangeCertificate(): RoundChangeCertificate = {
    new RoundChangeCertificate(roundChangePayloads)
  }

}
