package khipu.consensus.ibft

import khipu.consensus.ibft.network.ValidatorMulticaster
import khipu.domain.Address
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;

import java.util.Collection;
import java.util.Collections;

import com.google.common.annotations.VisibleForTesting;

/**
 *
 * @param multicaster Network connections to the remote validators
 * @param gossipHistoryLimit Maximum messages to track as seen
 */
//@VisibleForTesting
class UniqueMessageMulticaster(multicaster: ValidatorMulticaster, gossipedMessageTracker: MessageTracker) extends ValidatorMulticaster {

  def this(multicaster: ValidatorMulticaster, gossipHistoryLimit: Int) =
    this(multicaster, new MessageTracker(gossipHistoryLimit))

  override def send(message: MessageData) {
    send(message, Collections.emptyList());
  }

  override def send(message: MessageData, blackList: Collection[Address]) {
    if (gossipedMessageTracker.hasSeenMessage(message)) {
      return ;
    }
    multicaster.send(message, blackList);
    gossipedMessageTracker.addSeenMessage(message);
  }
}
