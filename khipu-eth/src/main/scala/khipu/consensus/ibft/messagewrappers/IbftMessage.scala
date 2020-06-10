package khipu.consensus.ibft.messagewrappers

import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import java.util.StringJoiner;

import khipu.consensus.ibft.ConsensusRoundIdentifier
import khipu.consensus.ibft.payload.Authored
import khipu.consensus.ibft.payload.Payload
import khipu.consensus.ibft.payload.RoundSpecific
import khipu.consensus.ibft.payload.SignedData
import khipu.domain.Address

class IbftMessage[P <: Payload](payload:SignedData[P] ) extends Authored with RoundSpecific {

  override def getAuthor():Address =  {
    return payload.getAuthor();
  }

  override def roundIdentifier(): ConsensusRoundIdentifier =  {
    payload.getPayload().getRoundIdentifier();
  }

  def encode(): ByteString =  {
    final BytesValueRLPOutput rlpOut = new BytesValueRLPOutput();
    payload.writeTo(rlpOut);
    return rlpOut.encoded();
  }

  def getSignedPayload(): SignedData[P] =  {
    payload;
  }

  def getMessageType(): Int = {
    payload.payload.getMessageType()
  }

  protected def getPayload(): P = {
    payload.payload
  }

  override def toString() = {
    new StringJoiner(", ", IbftMessage.class.getSimpleName() + "[", "]")
        .add("payload=" + payload)
        .toString();
  }
}
