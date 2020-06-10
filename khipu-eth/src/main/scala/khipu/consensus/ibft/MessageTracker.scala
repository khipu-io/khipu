package khipu.consensus.ibft

import static java.util.Collections.newSetFromMap;

import org.hyperledger.besu.ethereum.core.Hash;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;

import java.util.Set;

class MessageTracker(messageTrackingLimit:Int) {

val seenMessages: Set[Hash] = newSetFromMap(new SizeLimitedMap[Hash](messageTrackingLimit));

  def addSeenMessage(message:MessageData ) {
    val uniqueID = Hash.hash(message.getData());
    seenMessages.add(uniqueID);
  }

  def hasSeenMessage(message:MessageData ):Boolean = {
    val uniqueID = Hash.hash(message.getData());
    return seenMessages.contains(uniqueID);
  }
}
