package khipu.consensus.ibft.messagewrappers

import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import akka.util.ByteString
import java.util.Optional;

import khipu.consensus.ibft.payload.PreparedCertificate
import khipu.consensus.ibft.payload.RoundChangePayload
import khipu.consensus.ibft.payload.SignedData

object RoundChange{
    def decode(data:ByteString ):RoundChange =  {

    final RLPInput rlpIn = RLP.input(data);
    rlpIn.enterList();
    final SignedData<RoundChangePayload> payload =
        SignedData.readSignedRoundChangePayloadFrom(rlpIn);
    Optional<Block> block = Optional.empty();
    if (!rlpIn.nextIsNull()) {
      block = Optional.of(Block.readFrom(rlpIn, IbftBlockHeaderFunctions.forCommittedSeal()));
    } else {
      rlpIn.skipNext();
    }
    rlpIn.leaveList();

    return new RoundChange(payload, block);
  }
}
class RoundChange(payload: SignedData[RoundChangePayload], val proposedBlock: Option[Block] ) extends IbftMessage[RoundChangePayload](payload) {


  def getPreparedCertificate():Option[PreparedCertificate] = {
    getPayload().preparedCertificate;
  }

  def getPreparedCertificateRound():Option[ConsensusRoundIdentifier] = {
    getPreparedCertificate()
        .map(prepCert -> prepCert.getProposalPayload().getPayload().getRoundIdentifier());
  }

  override def encode():ByteString =  {
    final BytesValueRLPOutput rlpOut = new BytesValueRLPOutput();
    rlpOut.startList();
    getSignedPayload().writeTo(rlpOut);
    if (proposedBlock.isPresent()) {
      proposedBlock.get().writeTo(rlpOut);
    } else {
      rlpOut.writeNull();
    }
    rlpOut.endList();
    return rlpOut.encoded();
  }


}
