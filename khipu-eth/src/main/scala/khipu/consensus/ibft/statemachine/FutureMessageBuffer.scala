package khipu.consensus.ibft.statemachine

import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Message;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.google.common.annotations.VisibleForTesting;

/**
 * Buffer which holds future IBFT messages.
 *
 * <p>This buffer only allows messages to be added which have a chain height greater than current
 * height and up to chain futureMessagesMaxDistance from the current chain height.
 *
 * <p>When the total number of messages is greater futureMessagesLimit then messages are evicted.
 *
 * <p>If there is more than one height in the buffer then all messages for the highest chain height
 * are removed. Otherwise if there is only one height the oldest inserted message is removed.
 */
class FutureMessageBuffer(
    futureMessagesMaxDistance: Long,
    futureMessagesLimit:       Long,
    private var chainHeight:   Long
) {

  private var buffer: NavigableMap[Long, List[Message]] = new TreeMap[Long, List[Message]]();

  def addMessage(msgChainHeight: Long, rawMsg: Message) {
    if (futureMessagesLimit == 0 || !validMessageHeight(msgChainHeight, chainHeight)) {
      return ;
    }

    addMessageToBuffer(msgChainHeight, rawMsg);

    if (totalMessagesSize() > futureMessagesLimit) {
      evictMessages();
    }
  }

  private def addMessageToBuffer(msgChainHeight: Long, rawMsg: Message) {
    buffer.putIfAbsent(msgChainHeight, new ArrayList[Message]());
    buffer.get(msgChainHeight).add(rawMsg);
  }

  private def validMessageHeight(msgChainHeight: Long, currentHeight: Long): Boolean = {
    val isFutureMsg = msgChainHeight > currentHeight;
    val withinMaxChainHeight = msgChainHeight <= currentHeight + futureMessagesMaxDistance;
    return isFutureMsg && withinMaxChainHeight;
  }

  private def evictMessages() {
    if (buffer.size() > 1) {
      buffer.remove(buffer.lastKey());
    } else if (buffer.size() == 1) {
      val messages = buffer.firstEntry().getValue();
      messages.remove(0);
    }
  }

  def retrieveMessagesForHeight(height: Long): List[Message] = {
    chainHeight = height;
    val messages = buffer.getOrDefault(height, Collections.emptyList());
    discardPreviousHeightMessages();
    return messages;
  }

  private def discardPreviousHeightMessages() {
    if (!buffer.isEmpty()) {
      var h = buffer.firstKey()
      while (h <= chainHeight) {
        buffer.remove(h);
        h += 1
      }
    }
  }

  @VisibleForTesting
  def totalMessagesSize(): Long = {
    return buffer.values().stream().map(List :: size).reduce(0, Integer :: sum).longValue();
  }
}
