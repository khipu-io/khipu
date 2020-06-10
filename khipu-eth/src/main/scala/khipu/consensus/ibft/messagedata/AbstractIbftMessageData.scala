package khipu.consensus.ibft.messagedata

import org.hyperledger.besu.ethereum.p2p.rlpx.wire.AbstractMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;

import java.util.function.Function;


object AbstractIbftMessageData {
  protected def   fromMessageData[T <: AbstractIbftMessageData](
      messageData:MessageData ,
      messageCode:Int,
      clazz:Class[T] ,
      constructor:Function[ByteString, T] ):T = {

    if (clazz.isInstance(messageData)) {
      @SuppressWarnings("unchecked")
      T castMessage = (T) messageData;
      return castMessage;
    }
    final int code = messageData.getCode();
    if (code != messageCode) {
      throw new IllegalArgumentException(
          String.format(
              "MessageData has code %d and thus is not a %s", code, clazz.getSimpleName()));
    }

    return constructor.apply(messageData.getData());
  } 
}
abstract class AbstractIbftMessageData protected (data: ByteString ) extends AbstractMessageData(data)