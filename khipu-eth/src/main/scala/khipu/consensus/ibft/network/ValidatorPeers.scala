package khipu.consensus.ibft.network

import org.hyperledger.besu.consensus.common.VoteTallyCache;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection.PeerNotConnected;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import khipu.domain.Address

/**
 * Responsible for tracking the network peers which have a connection to this node, then
 * multicasting packets to ONLY the peers which have been identified as being validators.
 */
object ValidatorPeers {
  private val PROTOCOL_NAME = "IBF";

}
class ValidatorPeers(voteTallyCache: VoteTallyCache ) extends ValidatorMulticaster with PeerConnectionTracker {

  // It's possible for multiple connections between peers to exist for brief periods, so map each
  // address to a set of connections
  private val connectionsByAddress: Map[Address, Set[PeerConnection]]  = new ConcurrentHashMap[Address, Set[PeerConnection]]();


  override
  def add(newConnection:PeerConnection ) {
    val peerAddress = newConnection.getPeerInfo().getAddress();
    val connections =
        connectionsByAddress.computeIfAbsent(
            peerAddress, (k) -> Collections.newSetFromMap(new ConcurrentHashMap<>()));
    connections.add(newConnection);
  }

  override
  def remove(removedConnection:PeerConnection ) {
    val peerAddress = removedConnection.getPeerInfo().getAddress();
    val connections = connectionsByAddress.get(peerAddress);
    if (connections == null) {
      return;
    }
    connections.remove(removedConnection);
  }

  override
  def send(message:MessageData ) {
    sendMessageToSpecificAddresses(getLatestValidators(), message);
  }

  override
  def send(message:MessageData , blackList: Collection[Address] ) {
    val includedValidators = getLatestValidators().stream()
            .filter(a -> !blackList.contains(a))
            .collect(Collectors.toSet());
    sendMessageToSpecificAddresses(includedValidators, message);
  }

  private def sendMessageToSpecificAddresses( recipients:Collection[Address] , message: MessageData ) {
    //LOG.trace(
    //    "Sending message to peers messageCode={} recipients={}", message.getCode(), recipients);
    recipients.stream()
        .map(connectionsByAddress::get)
        .filter(Objects::nonNull)
        .flatMap(Set::stream)
        .forEach(
            connection => {
              try {
                connection.sendForProtocol(PROTOCOL_NAME, message);
              } catch (final PeerNotConnected peerNotConnected) {
                LOG.trace(
                    "Lost connection to a validator. remoteAddress={} peerInfo={}",
                    connection.getRemoteAddress(),
                    connection.getPeerInfo());
              }
            });
  }

  private def getLatestValidators(): Collection[Address] = {
    return voteTallyCache.getVoteTallyAtHead().getValidators();
  }
}
