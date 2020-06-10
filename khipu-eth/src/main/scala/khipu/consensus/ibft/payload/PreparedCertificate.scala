package khipu.consensus.ibft.payload

import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

object PreparedCertificate {
  def readFrom(rlpInput:RLPInput ):PreparedCertificate = {
    final SignedData<ProposalPayload> proposalMessage;
    final List<SignedData<PreparePayload>> prepareMessages;

    rlpInput.enterList();
    proposalMessage = SignedData.readSignedProposalPayloadFrom(rlpInput);
    prepareMessages = rlpInput.readList(SignedData::readSignedPreparePayloadFrom);
    rlpInput.leaveList();

    PreparedCertificate(proposalMessage, prepareMessages);
  }


}
final case class PreparedCertificate(proposalPayload:SignedData[ProposalPayload] ,preparePayloads: List[SignedData[PreparePayload]] ) {
  def writeTo(rlpOutput:RLPOutput ) {
    rlpOutput.startList();
    proposalPayload.writeTo(rlpOutput);
    rlpOutput.writeList(preparePayloads, SignedData::writeTo);
    rlpOutput.endList();
  }

}
