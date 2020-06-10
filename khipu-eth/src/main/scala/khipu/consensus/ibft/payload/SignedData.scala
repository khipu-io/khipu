package khipu.consensus.ibft.payload

import org.hyperledger.besu.crypto.SECP256K1.Signature;
import org.hyperledger.besu.ethereum.core.Address;
import org.hyperledger.besu.ethereum.core.Util;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.Objects;
import java.util.StringJoiner;


object SignedData {
    def readSignedProposalPayloadFrom(rlpInput:RLPInput ):SignedData[ProposalPayload] =  {

    rlpInput.enterList();
    final ProposalPayload unsignedMessageData = ProposalPayload.readFrom(rlpInput);
    final Signature signature = readSignature(rlpInput);
    rlpInput.leaveList();

    from(unsignedMessageData, signature);
  }

  def readSignedPreparePayloadFrom(rlpInput:RLPInput ):SignedData[PreparePayload] =  {

    rlpInput.enterList();
    final PreparePayload unsignedMessageData = PreparePayload.readFrom(rlpInput);
    final Signature signature = readSignature(rlpInput);
    rlpInput.leaveList();

    from(unsignedMessageData, signature);
  }

  def readSignedCommitPayloadFrom(rlpInput:RLPInput ):SignedData[CommitPayload] =  {

    rlpInput.enterList();
    final CommitPayload unsignedMessageData = CommitPayload.readFrom(rlpInput);
    final Signature signature = readSignature(rlpInput);
    rlpInput.leaveList();

    from(unsignedMessageData, signature);
  }

  def readSignedRoundChangePayloadFrom(rlpInput:RLPInput ):SignedData[RoundChangePayload] =  {

    rlpInput.enterList();
    final RoundChangePayload unsignedMessageData = RoundChangePayload.readFrom(rlpInput);
    final Signature signature = readSignature(rlpInput);
    rlpInput.leaveList();

    from(unsignedMessageData, signature);
  }

  protected def from[M <: Payload](unsignedMessageData:M,  signature: Signature ):SignedData[M] =  {

    val sender = recoverSender(unsignedMessageData, signature);

    SignedData[M](unsignedMessageData, sender, signature);
  }

  protected def readSignature(signedMessage:RLPInput ):Signature =  {
    signedMessage.readBytes(Signature::decode);
  }

  protected def recoverSender( unsignedMessageData:Payload , signature:Signature ):Address =  {
    Util.signatureToAddress(signature, MessageFactory.hashForSignature(unsignedMessageData));
  }

}
final case class SignedData[M <: Payload](unsignedPayload:M , sender:Address , signature:Signature ) extends Authored {

  def author = sender;

  def  payload: M = unsignedPayload;

  def writeTo(output:RLPOutput ) {

    output.startList();
    unsignedPayload.writeTo(output);
    output.writeBytes(signature.encodedBytes());
    output.endList();
  }

  def encode():ByteString =  {
    val rlpEncode = new BytesValueRLPOutput();
    writeTo(rlpEncode);
    return rlpEncode.encoded();
  }

}
