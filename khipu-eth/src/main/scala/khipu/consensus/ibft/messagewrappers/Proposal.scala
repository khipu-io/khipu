package khipu.consensus.ibft.messagewrappers

import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.Hash;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.Optional;


object Proposal {
  def decode(data: ByteString ):Proposal =  {
    final RLPInput rlpIn = RLP.input(data);
    rlpIn.enterList();
    final SignedData<ProposalPayload> payload = SignedData.readSignedProposalPayloadFrom(rlpIn);
    final Block proposedBlock = Block.readFrom(rlpIn, IbftBlockHeaderFunctions.forCommittedSeal());

    final Optional<RoundChangeCertificate> roundChangeCertificate =
        readRoundChangeCertificate(rlpIn);

    rlpIn.leaveList();
    return new Proposal(payload, proposedBlock, roundChangeCertificate);
  }

  private def readRoundChangeCertificate(rlpIn: RLPInput ): Option[RoundChangeCertificate] = {
    RoundChangeCertificate roundChangeCertificate = null;
    if (!rlpIn.nextIsNull()) {
      roundChangeCertificate = RoundChangeCertificate.readFrom(rlpIn);
    } else {
      rlpIn.skipNext();
    }

    return Option(roundChangeCertificate);
  } 
}
class Proposal(
      payload: SignedData[ProposalPayload] ,
      proposedBlock: Block ,
      val roundChangeCertificate: Option[RoundChangeCertificate] ) extends IbftMessage[ProposalPayload](payload) {


  def getBlock(): Block =  {
    proposedBlock;
  }

  def getDigest(): Hash =  {
    getPayload().digest
  }

  override def encode(): ByteString =  {
    final BytesValueRLPOutput rlpOut = new BytesValueRLPOutput();
    rlpOut.startList();
    getSignedPayload().writeTo(rlpOut);
    proposedBlock.writeTo(rlpOut);
    if (roundChangeCertificate.isPresent()) {
      roundChangeCertificate.get().writeTo(rlpOut);
    } else {
      rlpOut.writeNull();
    }
    rlpOut.endList();
    return rlpOut.encoded();
  }

 
}
