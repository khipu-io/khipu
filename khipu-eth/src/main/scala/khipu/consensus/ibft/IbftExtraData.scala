package khipu.consensus.ibft

import static com.google.common.base.Preconditions.checkNotNull;

import khipu.domain.Address
import khipu.domain.BlockHeader
import org.hyperledger.besu.crypto.SECP256K1.Signature;
import org.hyperledger.besu.ethereum.core.ParsedExtraData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import akka.util.ByteString
import java.util.Collection;
import java.util.Collections;
import java.util.List;


object IbftExtraData {
  val EXTRA_VANITY_LENGTH = 32

   private trait EncodingType
   private object EncodingType {
     case object ALL extends EncodingType
    case object EXCLUDE_COMMIT_SEALS extends EncodingType
    case object  EXCLUDE_COMMIT_SEALS_AND_ROUND_NUMBER extends EncodingType 
  } 
  def fromAddresses(addresses:Collection[Address] ): IbftExtraData =  {
    new IbftExtraData(
      ByteString(Array.ofDim[Byte](32)), Collections.emptyList(), None, 0, addresses)
  }

  def decode(blockHeader:BlockHeader ):IbftExtraData = {
    blockHeader.getParsedExtraData() match {
      case inputExtraData: IbftExtraData => inputExtraData
        case _ =>
              //LOG.warn(
    //    "Expected a IbftExtraData instance but got {}. Reparsing required.",
    //    inputExtraData != null ? inputExtraData.getClass().getName() : "null");
    decodeRaw(blockHeader.getExtraData());
    }
    
  }

  protected def decodeRaw(input:ByteString ): IbftExtraData =  {
    if (input.isEmpty) {
      throw new IllegalArgumentException("Invalid Bytes supplied - Ibft Extra Data required.");
    }

    final RLPInput rlpInput = new BytesValueRLPInput(input, false);

    rlpInput.enterList(); // This accounts for the "root node" which contains IBFT data items.
    final Bytes vanityData = rlpInput.readBytes();
    final List<Address> validators = rlpInput.readList(Address::readFrom);
    final Optional<Vote> vote;
    if (rlpInput.nextIsNull()) {
      vote = Optional.empty();
      rlpInput.skipNext();
    } else {
      vote = Optional.of(Vote.readFrom(rlpInput));
    }
    final int round = rlpInput.readInt();
    final List<Signature> seals = rlpInput.readList(rlp -> Signature.decode(rlp.readBytes()));
    rlpInput.leaveList();

    return new IbftExtraData(vanityData, seals, vote, round, validators);
  }

 def createGenesisExtraDataString(validators: List[Address] ):String =  {
    val extraData = new IbftExtraData(ByteString(Array.ofDim[Byte](32)), Collections.emptyList(), None, 0, validators)
    extraData.encode().toString();
  }

}
/**
 * Represents the data structure stored in the extraData field of the BlockHeader used when
 * operating under an IBFT 2.0 consensus mechanism.
 */
final case class IbftExtraData(
      vanityData:ByteString ,
      seals:Seq[Signature],
      vote: Option[Vote],
      round:Int,
      validators:Collection[Address] ) /* extends ParsedExtraData */ {
      import IbftExtraData.EncodingType
  //private static final Logger LOG = LogManager.getLogger();


 {

    checkNotNull(vanityData);
    checkNotNull(seals);
    checkNotNull(validators);

  }

  def encode():ByteString =  {
    encode(EncodingType.ALL);
  }

  def encodeWithoutCommitSeals():ByteString =  {
    encode(EncodingType.EXCLUDE_COMMIT_SEALS);
  }

  def encodeWithoutCommitSealsAndRoundNumber(): ByteString = {
    encode(EncodingType.EXCLUDE_COMMIT_SEALS_AND_ROUND_NUMBER);
  }



  private def encode(encodingType:EncodingType ):ByteString =  {

    final BytesValueRLPOutput encoder = new BytesValueRLPOutput();
    encoder.startList();
    encoder.writeBytes(vanityData);
    encoder.writeList(validators, (validator, rlp) -> rlp.writeBytes(validator));
    if (vote.isPresent()) {
      vote.get().writeTo(encoder);
    } else {
      encoder.writeNull();
    }

    if (encodingType != EncodingType.EXCLUDE_COMMIT_SEALS_AND_ROUND_NUMBER) {
      encoder.writeInt(round);
      if (encodingType != EncodingType.EXCLUDE_COMMIT_SEALS) {
        encoder.writeList(seals, (committer, rlp) -> rlp.writeBytes(committer.encodedBytes()));
      }
    }
    encoder.endList();

    return encoder.encoded();
  }

 
 

  override def toString() = {
    return new StringJoiner(", ", IbftExtraData.class.getSimpleName() + "[", "]")
        .add("vanityData=" + vanityData)
        .add("seals=" + seals)
        .add("vote=" + vote)
        .add("round=" + round)
        .add("validators=" + validators)
        .toString();
  }
}
