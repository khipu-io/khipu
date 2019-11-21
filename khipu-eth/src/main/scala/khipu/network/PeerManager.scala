package khipu.network

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.OneForOneStrategy
import akka.actor.Props
import akka.actor.SupervisorStrategy
import akka.actor.Timers
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout
import java.net.URI
import khipu.network.PeerEntity.Status.Handshaked
import khipu.network.handshake.EtcHandshake
import khipu.network.p2p.MessageSerializable
import khipu.network.rlpx.RLPx
import khipu.network.rlpx.auth.AuthHandshake
import khipu.network.rlpx.discovery.NodeDiscoveryService
import khipu.service.ServiceBoard
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Success

object PeerManager {
  def props(peerConfig: PeerConfiguration) =
    Props(classOf[PeerManager], peerConfig)

  final case object GetPeers
  final case class Peers(peers: Map[Peer, PeerEntity.Status]) {
    def handshaked: Seq[Peer] = peers.collect { case (peer, Handshaked) => peer }.toSeq
  }
  final case class DropNode(peerId: String)

  final case class SendMessage(peerId: String, message: MessageSerializable)

  final case object UpdateNodesTask
  final case object UpdateNodesTick
}
class PeerManager(peerConfig: PeerConfiguration) extends Actor with Timers with ActorLogging {
  import context.dispatcher
  import PeerManager._

  private var peers = Map[String, Peer]()
  private var peersHandshaked = Map[String, Peer]()
  private var peersGoingToConnect = Map[String, Peer]()
  private var droppedNodes = Set[String]()
  private var triedNodes = mutable.LinkedHashMap[String, URI]()

  private def incomingPeers = peersHandshaked.filter(_._2.isInstanceOf[IncomingPeer])
  private def outgoingPeers = peersHandshaked.filter(_._2.isInstanceOf[OutgoingPeer])

  private val serviceBoard = ServiceBoard(context.system)
  private def nodeDiscovery = NodeDiscoveryService.proxy(context.system)

  private def reportIntervalThreshold = if (peersHandshaked.size >= 5 || peersHandshaked.size == peerConfig.maxPeers) 16 else 1
  private var reportIntervalCount = 0

  override val supervisorStrategy = OneForOneStrategy() {
    case _ => SupervisorStrategy.Restart
  }

  timers.startTimerWithFixedDelay(UpdateNodesTask, UpdateNodesTick, peerConfig.updateNodesInterval)

  //knownNodesService ! KnownNodesService.GetKnownNodes

  override def postStop() {
    timers.cancel(UpdateNodesTask)
    super.postStop()
    log.info("[peer] PeerManager stopped")
  }

  override def receive: Receive = {
    case KnownNodesService.KnownNodes(addresses) =>
      // toMap to make sure only one node of same interface:port is left
      val idToNode = addresses.flatMap(uri => Peer.peerId(uri).map(_ -> uri)).toMap.filterNot {
        case (peerId, uri) => isInConnecting(peerId)
      }

      val nOutgoing = (peersHandshaked -- droppedNodes).filter(_._2.isInstanceOf[OutgoingPeer]).size
      val nToConnect = peerConfig.maxPeers - peerConfig.maxIncomingPeers - nOutgoing
      val urisToConnect = idToNode.take(nToConnect)

      if (urisToConnect.nonEmpty) {
        log.debug("[peer] Trying to connect to {} known nodes", urisToConnect.size)
        urisToConnect foreach {
          case (peerId, uri) =>
            connectTo(peerId, uri)
        }
      }

    case NodeDiscoveryService.DiscoveredNodes(nodes) =>
      // toMap to make sure only one node of same interface:port is left
      val idToNode = nodes.flatMap { node =>
        Peer.peerId(node.uri).map(_ -> node)
      }.toMap.filterNot {
        case (peerId, uri) => isInConnecting(peerId) || droppedNodes.contains(peerId) || triedNodes.contains(peerId)
      }

      val nOutgoing = (peersHandshaked -- droppedNodes).filter(_._2.isInstanceOf[OutgoingPeer]).size
      val nToConnect = peerConfig.maxPeers - peerConfig.maxIncomingPeers - nOutgoing
      var urisToConnect = idToNode.toList.sortBy(-_._2.addTimestamp).take(nToConnect).map(x => (x._1, x._2.uri))

      if (urisToConnect.size < nToConnect) {
        val (older, newer) = triedNodes.splitAt(nToConnect - urisToConnect.size)
        urisToConnect ++= older
        triedNodes = newer
      }

      reportIntervalCount += 1
      if (reportIntervalCount >= reportIntervalThreshold) {
        log.info(
          s"""|[peer] Discovered ${nodes.size} nodes (${droppedNodes.size} dropped), handshaked ${peersHandshaked.size}/${peerConfig.maxPeers} 
            |(in/out): (${incomingPeers.size}/${outgoingPeers.size}). Connecting to ${urisToConnect.size} more nodes.""".stripMargin.replace("\n", " ")
        )
        reportIntervalCount = 0
      }

      urisToConnect foreach {
        case (peerId, uri) =>
          triedNodes += (peerId -> uri)
          connectTo(peerId, uri)
      }

    case PeerEntity.PeerEntityCreated(peer) =>
      if (peer.entity eq null) {
        log.debug(s"[peer] PeerEntityCreated: $peer entity is null")
      } else {
        peers += (peer.id -> peer)
        peersGoingToConnect -= peer.id
      }

    case PeerEntity.PeerHandshaked(peer, peerInfo) =>
      peers.get(peer.id) match {
        case Some(peer) =>
          log.debug(s"[peer] PeerHandshaked: $peer")
          peersHandshaked += (peer.id -> peer)
        case None =>
          log.debug(s"PeerHandshaked $peer does not found in peers")
      }

    case PeerEntity.PeerDisconnected(peerId) =>
      log.debug(s"[peer] PeerDisconnected: $peerId")
      peersHandshaked -= peerId

    case DropNode(peerId) =>
      log.debug(s"[peer] Dropped: $peerId")
      droppedNodes += peerId

    case GetPeers =>
      getPeers().pipeTo(sender())

    case SendMessage(peerId, message) =>
      peersHandshaked.get(peerId) foreach { _.entity ! PeerEntity.MessageToPeer(peerId, message) }

    case UpdateNodesTick =>
      if (nodeDiscovery ne null) { // we are not sure whether nodeDiscovery started
        nodeDiscovery ! NodeDiscoveryService.GetDiscoveredNodes
      }

    case PeerEntity.PeerEntityStopped(peerId) =>
      log.debug(s"[peer] PeerEntityStopped: $peerId")
      peers -= peerId
      peersHandshaked -= peerId
  }

  private def connectTo(peerId: String, uri: URI) {
    val nOutgoing = (peersHandshaked -- droppedNodes).filter(_._2.isInstanceOf[OutgoingPeer]).size
    if (nOutgoing < (peerConfig.maxPeers - peerConfig.maxIncomingPeers)) {
      log.debug(s"[peer] Connecting to $peerId - $uri")

      val peer = new OutgoingPeer(peerId, uri)
      peersGoingToConnect += (peerId -> peer)

      val authHandshake = AuthHandshake(serviceBoard.nodeKey, serviceBoard.secureRandom)
      val handshake = new EtcHandshake(serviceBoard.nodeStatus, serviceBoard.blockchain, serviceBoard.storages.appStateStorage, peerConfig, serviceBoard.forkResolverOpt)
      try {
        RLPx.startOutgoing(peer, serviceBoard.messageDecoder, serviceBoard.protocolVersion, authHandshake, handshake)(context.system)
      } catch {
        case e: Throwable =>
          //droppedNodes += peer.id // seems mostly from 'unique name exeception', not a big deal, and happens rarely now
          log.debug(s"[peer] Error during connect to $peer, ${e.getMessage}")
      }
    } else {
      log.debug("[peer] Maximum number of connected peers reached.")
    }
  }

  private def isInConnecting(peerId: String): Boolean = {
    peersGoingToConnect.contains(peerId) || peers.contains(peerId)
  }

  private def getPeers(): Future[Peers] = {
    implicit val timeout = Timeout(2.seconds)

    Future.traverse(peersHandshaked.values) { peer =>
      (peer.entity ? PeerEntity.GetStatus).mapTo[PeerEntity.StatusResponse] map {
        sr => Success((peer, sr.status))
      }
    } map {
      r => Peers.apply(r.collect { case Success(v) => v }.toMap)
    }
  }
}

