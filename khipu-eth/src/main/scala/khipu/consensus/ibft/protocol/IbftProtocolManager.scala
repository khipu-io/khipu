package khipu.consensus.ibft.protocol

import org.hyperledger.besu.consensus.ibft.IbftEventQueue;
import org.hyperledger.besu.consensus.ibft.ibftevent.IbftEvent;
import org.hyperledger.besu.consensus.ibft.ibftevent.IbftEvents;
import org.hyperledger.besu.consensus.ibft.network.PeerConnectionTracker;
import org.hyperledger.besu.ethereum.p2p.network.ProtocolManager;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Message;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;

import java.util.Arrays;
import java.util.List;

/**
 * Constructor for the ibft protocol manager
 *
 * @param ibftEventQueue Entry point into the ibft event processor
 * @param peers Used to track all connected IBFT peers.
 */
class IbftProtocolManager(ibftEventQueue: IbftEventQueue, peers: PeerConnectionTracker) extends ProtocolManager {
  override def getSupportedProtocol(): String = {
    IbftSubProtocol.get().getName();
  }

  override def getSupportedCapabilities(): List[Capability] = {
    Arrays.asList(IbftSubProtocol.IBFV1);
  }

  override def stop() {}

  @throws(classOf[InterruptedException])
  override def awaitStop() {}

  /**
   * This function is called by the P2P framework when an "IBF" message has been received. This
   * function is responsible for:
   *
   * <ul>
   *   <li>Determining if the message was from a current validator (discard if not)
   *   <li>Determining if the message received was for the 'current round', discarding if old and
   *       buffering for the future if ahead of current state.
   *   <li>If the received message is otherwise valid, it is sent to the state machine which is
   *       responsible for determining how to handle the message given its internal state.
   * </ul>
   *
   * @param cap The capability under which the message was transmitted.
   * @param message The message to be decoded.
   */
  override def processMessage(cap: Capability, message: Message) {
    val messageEvent = IbftEvents.fromMessage(message);
    ibftEventQueue.add(messageEvent);
  }

  override def handleNewConnection(peerConnection: PeerConnection) {
    peers.add(peerConnection);
  }

  override def handleDisconnect(
    peerConnection:   PeerConnection,
    disconnectReason: DisconnectReason,
    initiatedByPeer:  Boolean
  ) {
    peers.remove(peerConnection);
  }
}
