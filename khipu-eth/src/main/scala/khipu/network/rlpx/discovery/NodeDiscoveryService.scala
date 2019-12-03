package khipu.network.rlpx.discovery

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.PoisonPill
import akka.actor.Timers
import akka.cluster.singleton.ClusterSingletonManager
import akka.cluster.singleton.ClusterSingletonManagerSettings
import akka.cluster.singleton.ClusterSingletonProxy
import akka.cluster.singleton.ClusterSingletonProxySettings
import akka.io.IO
import akka.io.Udp
import akka.util.ByteString
import java.net.{ InetSocketAddress, URI }
import java.util.Arrays
import khipu.NodeStatus
import khipu.ServerStatus
import khipu.rlp.RLPEncoder
import khipu.storage.KnownNodesStorage
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Random
import scala.util.Success

/**
 * Full cone NAT:
 * This is also known as one to one NAT. It is basically simple port forwarding
 * where there is a static binding from client ip:port to NAT's ip:port and any
 * one from Internet can write to NAT's ip:port and it will be forwarded to the
 * client. This kind of NAT is used very infrequently.
 *
 * (Address) Restricted cone NAT:
 * In this scenario, client can only receive packets from the host where it has
 * already sent packets before. For example, if the client sends a packet to
 * server with address 8.8.8.8 then NAT will accept the reply from any port of
 * the server as long as the source IP (8.8.8.8) remains the same.
 *
 * Port Restricted cone NAT:
 * In this scenario, client can only receive packets from the host where it has
 * already sent packets before as long as they come from the same server port.
 * For example, if the client sends a packet to server with address 8.8.8.8 on
 * port 5555 then NAT will only accept reply originating from port 5555 from the
 * server. This NAT is more restricted than Address  Restricted NAT.
 *
 * Symmetric NAT:
 * In general all the above NAT types preserve the port. For example if the
 * client is sending a packet from 192.168.0.2:54321 to 8.8.8.8:80, then NAT
 * will usually map 192.168.0.2:54321 to 1.2.3.4:54321 preserving the port
 * number. But in Symmetric NAT a random port is chosen for every new connection.
 * This makes port prediction very difficult and techniques like UDP hole
 * punching fails in this scenario
 *
 * Also see src/test/NATDiscovery to test the type of your NAT
 *
 * $ sudo firewall-cmd --get-default-zone
 * $ sudo firewall-cmd --zone=public --permanent --add-port=30303/udp
 * $ sudo systemctl restart firewalld.service
 *
 * $ sudo ethtool -K enp0s25 tx off rx off
 * $ tcpdump -i enp0s25 udp port 30303 -vv -X
 * $ sudo ethtool -K enp0s25 tx on rx on
 * $ echo "test data" > /dev/udp/127.0.0.1/30303
 */
object NodeDiscoveryService {
  private def props(
    discoveryConfig:   DiscoveryConfig,
    knownNodesStorage: KnownNodesStorage,
    nodeStatus:        NodeStatus
  ) = Props(classOf[NodeDiscoveryService], discoveryConfig, knownNodesStorage, nodeStatus)

  val name = "nodeDiscovery"
  val managerName = "khipuSingleton-" + name
  val managerPath = "/user/" + managerName
  val path = managerPath + "/" + name
  val proxyName = "khipuSingletonProxy-" + name
  val proxyPath = "/user/" + proxyName

  def start(system: ActorSystem, role: Option[String],
            discoveryConfig:   DiscoveryConfig,
            knownNodesStorage: KnownNodesStorage,
            nodeStatus:        NodeStatus): ActorRef = {
    val settings = ClusterSingletonManagerSettings(system).withRole(role).withSingletonName(name)
    system.actorOf(
      ClusterSingletonManager.props(
        singletonProps = props(discoveryConfig, knownNodesStorage, nodeStatus),
        terminationMessage = PoisonPill,
        settings = settings
      ), name = managerName
    )
  }

  def startProxy(system: ActorSystem, role: Option[String]): ActorRef = {
    val settings = ClusterSingletonProxySettings(system).withRole(role).withSingletonName(name)
    val proxy = system.actorOf(
      ClusterSingletonProxy.props(
        singletonManagerPath = managerPath,
        settings = settings
      ), name = proxyName
    )
    proxy
  }

  def proxy(system: ActorSystem) = system.actorSelection(proxyPath)

  case object GetDiscoveredNodes
  final case class DiscoveredNodes(nodes: Iterable[Node])

  case object Start

  private sealed trait ScanTick {
    def round: Int
    def prevScanned: Set[ByteString]
  }

  private case object DiscoveryTask
  private case object DiscoveryRoundTask
  private case class DiscoveryRoundTick(round: Int, prevScanned: Set[ByteString]) extends ScanTick

  private case object RefreshTask
  private case object RefreshRoundTask
  private case class RefreshRoundTick(round: Int, prevScanned: Set[ByteString]) extends ScanTick

  private final case class PingTimeout(nodeId: ByteString)

  final case class MessageReceived(message: Message, from: InetSocketAddress, packet: Packet)
}

/**
 * A cluster singleton, may be mixed with NodeStatus and NodeDiscovery
 */
class NodeDiscoveryService(
    discoveryConfig:        DiscoveryConfig,
    knownNodesStorage:      KnownNodesStorage,
    private var nodeStatus: NodeStatus
) extends Actor with Timers with ActorLogging {
  import Node.State
  import NodeDiscoveryService._
  import context.system

  private val pingTimeout = 15000.milliseconds

  private var socket: ActorRef = _

  // TODO if want to add homeNode to kRoutingTable, should specify the correct out ip address in DiscoveryConfig
  private val kRoutingTable = new KRoutingTable(nodeStatus.id, None)

  private var idToNode = Map[ByteString, Node]()

  self ! Start

  override def receive: Receive = idleBehavior

  override def postStop {
    super.postStop()
    log.info("[disc] DiscoveryService stopped")
  }

  def idleBehavior: Receive = {
    case Start =>
      log.info("[disc] DiscoveryService starting...")
      IO(Udp) ! Udp.Bind(self, new InetSocketAddress(discoveryConfig.interface, discoveryConfig.port))

    case Udp.Bound(local) =>
      log.info(s"[disc] DiscoveryService UDP bound at $local")
      socket = sender()

      nodeStatus = nodeStatus.copy(discoveryStatus = ServerStatus.Listening(local))
      context become (udpBehavior orElse businessBehavior)

      initKnownNodes()

      timers.startTimerWithFixedDelay(DiscoveryTask, DiscoveryRoundTick(1, Set()), KademliaOptions.DISCOVER_CYCLE.second)
      timers.startTimerWithFixedDelay(RefreshTask, RefreshRoundTick(1, Set()), KademliaOptions.BUCKET_REFRESH.millisecond)
  }

  def udpBehavior: Receive = {
    case Udp.Received(data, remote) =>
      val msgReceived = for {
        packet <- Packet.decodePacket(data)
        message <- Packet.extractMessage(packet)
      } yield MessageReceived(message, remote, packet)

      msgReceived match {
        case Success(msg) => receivedMessage(msg)
        case Failure(ex)  => log.debug(s"[disc] Unable to decode discovery packet from ${remote}: ${ex.getMessage}")
      }

    case Udp.Unbind =>
      log.info(s"[disc] DiscoveryService going to Unbind")
      socket ! Udp.Unbind

    case Udp.Unbound =>
      log.info(s"[disc] DiscoveryService unbound")
      nodeStatus = nodeStatus.copy(discoveryStatus = ServerStatus.NotListening)
      context become idleBehavior
  }

  def businessBehavior: Receive = {
    case x: DiscoveryRoundTick => scan(kRoutingTable.homeNodeId, x)
    case x: RefreshRoundTick   => scan(getRandomNodeId, x)

    case GetDiscoveredNodes =>
      sender() ! DiscoveredNodes(kRoutingTable.getAllNodes.asScala.map(_.node))

    case PingTimeout(nodeId) =>
      idToNode.get(nodeId) foreach { node =>
        node.waitForPong = None
        node.pingTrials += 1
        if (node.pingTrials < 3) {
          sendPing(node.id, node.udpAddress)
        } else {
          node.state match {
            case State.Discovered     => changeState(node, State.Dead)
            case State.EvictCandidate => changeState(node, State.NonActive)
            case _                    =>
          }
        }
      }
  }

  private def initKnownNodes() {
    val uris = discoveryConfig.bootstrapNodes.map(new URI(_)) ++ Set()
    //(if (discoveryConfig.discoveryEnabled) knownNodesStorage.getKnownNodes() else Set())

    val nodes = uris.map(Node(_))

    nodes foreach nodeDiscovered
    //nodes foreach kRoutingTable.tryAddNode
  }

  private def scan(targetId: Array[Byte], scanTick: ScanTick) {
    if (scanTick.round == 1) {
      log.debug(s"[disc] Scanning $scanTick $kRoutingTable")
      timers.cancel(DiscoveryRoundTask)
      timers.cancel(RefreshRoundTask)
    } else {
      log.debug(s"[disc] Scanning $scanTick")
    }

    val closest = kRoutingTable.getClosestNodes(targetId)
    var scanned = Map[ByteString, Node]()
    val itr = closest.iterator
    while (itr.hasNext && scanned.size < KademliaOptions.ALPHA) {
      val node = itr.next
      val nodeId = ByteString(node.id)
      if (!scanned.contains(nodeId) && !scanTick.prevScanned.contains(nodeId)) {
        scanned += (nodeId -> node)
      }
    }

    scanned foreach {
      case (nodeId, node) => sendMessage(FindNode(nodeId, expirationTimestamp), node.udpAddress)
    }

    if (scanTick.round < KademliaOptions.MAX_STEPS) {
      scanTick match {
        case DiscoveryRoundTick(round, prevScanned) =>
          timers.startSingleTimer(DiscoveryRoundTask, DiscoveryRoundTick(round + 1, prevScanned ++ scanned.keys), (KademliaOptions.ALPHA * 50).milliseconds)
        case RefreshRoundTick(round, prevScanned) =>
          timers.startSingleTimer(RefreshRoundTask, RefreshRoundTick(round + 1, prevScanned ++ scanned.keys), (KademliaOptions.ALPHA * 50).milliseconds)
      }
    }
  }

  def getRandomNodeId: Array[Byte] = {
    val gen = new Random()
    val id = Array.ofDim[Byte](32)
    gen.nextBytes(id)
    id
  }

  private def receivedMessage(msg: MessageReceived) {
    msg match {
      case MessageReceived(Ping(_version, _from, _to, _expiration), from, packet) =>
        log.debug(s"[disc] Received Ping version ${_version} from ${khipu.toHexString(packet.nodeId)}@$from ")

        if (_version >= VERSION) {
          if (!java.util.Arrays.equals(packet.nodeId, kRoutingTable.homeNodeId)) {
            idToNode.get(ByteString(packet.nodeId)) match {
              case None => nodeDiscovered(Node(packet.nodeId, from, from))
              case _    =>
            }

            val to = Endpoint(packet.nodeId, from.getPort, from.getPort)
            sendMessage(Pong(to, packet.mdc, expirationTimestamp), from)
          } else {
            log.debug(s"${khipu.toHexString(packet.nodeId)} is same as homenode: ${khipu.toHexString(kRoutingTable.homeNodeId)}")
          }
        } else {
          log.debug(s"[disc] Received Ping unmatched version ${_version} from ${khipu.toHexString(packet.nodeId)}@$from ")
        }

      case MessageReceived(Pong(_to, _pingHash, _expiration), from, packet) =>
        log.debug(s"[disc] Received Pong from $from")

        val nodeId = ByteString(packet.nodeId)
        idToNode.get(nodeId) match {
          case Some(node) =>
            node.waitForPong match {
              case Some(pingHash) =>
                // TODO check the pingHash?
                timers.cancel(PingTimeout(nodeId))
                node.pingTrials = 0
                node.waitForPong = None
                changeState(node, State.Alive)
              case _ =>
            }
          case None =>
            // if we received Pong from this node, usually means we've sent Ping
            // to it, or it should have been addToNode(node). So here should not
            // been reached.
            val node = Node(packet.nodeId, from, from)
            nodeDiscovered(node)
        }

      case MessageReceived(FindNode(_target, _expiration), from, packet) =>
        log.debug(s"[disc] Received FindNode from $from")

        val closest = kRoutingTable.getClosestNodes(_target.toArray)
        //if (publicHomeNode != null) {
        //    if (closest.size() == KademliaOptions.BUCKET_SIZE) closest.remove(closest.size() - 1);
        //    closest.add(publicHomeNode);
        //} 
        val neighbours = closest.asScala.map { n =>
          Neighbour(Endpoint(n.tcpAddress.getAddress.getAddress, n.udpAddress.getPort, n.tcpAddress.getPort), n.id)
        }
        sendMessage(Neighbours(neighbours, expirationTimestamp), from)

      case MessageReceived(Neighbours(_nodes, _expiration), from, packet) =>
        log.debug(s"[disc] Received Neighbours ${_nodes.size} from $from")

        _nodes foreach { n =>
          idToNode.get(ByteString(n.nodeId)) match {
            case Some(node) =>
              node.tcpAddress = n.endpoint.tcpAddress
              node
            case None =>
              val node = Node(n.nodeId, n.endpoint.udpAddress, n.endpoint.tcpAddress)
              nodeDiscovered(node)
              node
          }
        }
    }
  }

  /**
   * @return ping hash - mdc of ping packet
   */
  private def sendPing(toNodeId: Array[Byte], toAddr: InetSocketAddress): Option[ByteString] = {
    nodeStatus.discoveryStatus match {
      case ServerStatus.Listening(address) =>

        val tcpPort = nodeStatus.serverStatus match {
          case ServerStatus.Listening(addr) => addr.getPort
          case _                            => 0
        }

        val from = Endpoint(address.getAddress.getAddress, address.getPort, tcpPort)
        val to = Endpoint(toNodeId, toAddr.getPort, toAddr.getPort)
        val mdc = sendMessage(Ping(VERSION, from, to, expirationTimestamp), toAddr)

        val nodeId = ByteString(toNodeId)
        timers.cancel(PingTimeout(nodeId))
        timers.startSingleTimer(PingTimeout(nodeId), PingTimeout(nodeId), pingTimeout)
        mdc

      case ServerStatus.NotListening =>
        log.warning("[disc] UDP server not running. Not sending ping message.")
        None
    }
  }

  /**
   * @return packed mdc
   */
  private def sendMessage[M <: Message](message: M, to: InetSocketAddress)(implicit rlpEnc: RLPEncoder[M]): Option[ByteString] = {
    nodeStatus.discoveryStatus match {
      case ServerStatus.Listening(_) =>
        log.debug(s"Send ${message} to $to")

        val (mdc, packet) = Packet.encodePacket(message, nodeStatus.key)
        socket ! Udp.Send(packet, to)
        Some(mdc)

      case ServerStatus.NotListening =>
        log.warning(s"[disc] UDP server not running. Not sending message $message.")
        None
    }
  }

  private def nodeDiscovered(node: Node) {
    if (this.idToNode.size < discoveryConfig.nodesLimit) {
      this.idToNode += (ByteString(node.id) -> node)
      changeState(node, State.Discovered)
    } else {
      val (earliestNode, _) = this.idToNode.minBy { case (_, node) => node.addTimestamp }
      this.idToNode -= earliestNode
      this.idToNode += (ByteString(node.id) -> node)
      changeState(node, State.Discovered)
    }
  }

  private def expirationTimestamp = discoveryConfig.messageExpiration.toSeconds + System.currentTimeMillis / 1000

  // Manages state transfers
  private def changeState(node: Node, newState: Node.State) {
    var toState = newState
    (newState, node.state) match {
      case (State.Discovered, _) =>
        // will wait for Pong to assume this alive
        val pingHash = sendPing(node.id, node.udpAddress)
        node.waitForPong = pingHash

      //if (!node.isDiscoveryNode()) {
      case (State.Alive, _) =>
        kRoutingTable.tryAddNode(node) match {
          case Some(evictCandidate) =>
            if (evictCandidate.state != State.EvictCandidate) {
              evictCandidate.replaceCandicate = Some(node)
              changeState(evictCandidate, State.EvictCandidate)
            }
          case None =>
            toState = State.Active
        }
        log.debug(s"[disc] ${node.udpAddress} change to Alive, $kRoutingTable")

      case (State.Active, State.Alive) =>
        // new node won the challenge
        kRoutingTable.tryAddNode(node)

      case (State.Active, State.EvictCandidate) => // nothing to do here the node is already in the table
      case (State.Active, _)                    => // wrong state transition

      case (State.NonActive, State.EvictCandidate) =>
        // lost the challenge, removing ourselves from the table
        kRoutingTable.dropNode(node)
        // Congratulate the winner
        node.replaceCandicate foreach { changeState(_, State.Active) }
        node.replaceCandicate = None
        log.debug(s"[disc] ${node.udpAddress} changed from EvictCandidate to NonActive, $kRoutingTable")

      case (State.NonActive, State.Alive) => // ok the old node was better, nothing to do here
      case (State.NonActive, _)           => // wrong state transition
      //}

      case (State.EvictCandidate, _) =>
        // trying to survive, sending ping and waiting for pong
        sendPing(node.id, node.udpAddress)

      case (_, _) =>
    }
    node.state = toState
  }

}
