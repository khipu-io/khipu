package khipu.network.rlpx.discovery

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.PoisonPill
import akka.actor.Timers
import akka.cluster.client.ClusterClientReceptionist
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
import khipu.crypto
import khipu.crypto.ECDSASignature
import khipu.rlp
import khipu.rlp.RLPEncoder
import khipu.storage.KnownNodesStorage
import org.spongycastle.crypto.AsymmetricCipherKeyPair
import org.spongycastle.util.BigIntegers
import scala.util.Failure
import scala.util.Success
import scala.util.Try

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
    ClusterClientReceptionist(system).registerService(proxy)
    proxy
  }

  def proxy(system: ActorSystem) = system.actorSelection(proxyPath)

  object Node {
    def fromUri(uri: URI): Node = {
      val nodeId = ByteString(khipu.hexDecode(uri.getUserInfo))
      Node(nodeId, new InetSocketAddress(uri.getHost, uri.getPort), System.currentTimeMillis)
    }
  }
  final case class Node(id: ByteString, remoteAddress: InetSocketAddress, addTimestamp: Long) {
    val uri = new URI(s"enode://${khipu.toHexString(id)}@${remoteAddress.getAddress.getHostAddress}:${remoteAddress.getPort}")
  }

  case object GetDiscoveredNodes
  final case class DiscoveredNodes(nodes: Iterable[Node])

  private case object ScanTask
  private case object ScanTick
  case object Start

  final case class MessageReceived(message: Message, from: InetSocketAddress, packet: Packet)

  object Packet {
    private val MdcLength = 32
    private val PacketTypeByteIndex = MdcLength + ECDSASignature.EncodedLength
    private val DataOffset = PacketTypeByteIndex + 1

    def decodePacket(input: ByteString): Try[Packet] = {
      if (input.length < 98) {
        Failure(new RuntimeException("Bad message"))
      } else {
        Try(Packet(input)) match {
          case x @ Success(packet) =>
            if (Arrays.equals(packet.mdc.toArray, crypto.kec256(input.drop(32)))) {
              x
            } else {
              Failure(new RuntimeException("MDC check failed"))
            }
          case e: Failure[_] =>
            e
        }
      }
    }

    def extractMessage(packet: Packet): Try[Message] = Try {
      packet.packetType match {
        case Ping.packetType       => rlp.decode[Ping](packet.data.toArray[Byte])
        case Pong.packetType       => rlp.decode[Pong](packet.data.toArray[Byte])
        case FindNode.packetType   => rlp.decode[FindNode](packet.data.toArray[Byte])
        case Neighbours.packetType => rlp.decode[Neighbours](packet.data.toArray[Byte])
        case _                     => throw new RuntimeException(s"Unknown packet type ${packet.packetType}")
      }
    }

    def encodePacket[M <: Message](msg: M, keyPair: AsymmetricCipherKeyPair)(implicit rlpEnc: RLPEncoder[M]): ByteString = {
      val encodedData = rlp.encode(msg)

      val payload = Array(msg.packetType) ++ encodedData
      val forSig = crypto.kec256(payload)
      val signature = ECDSASignature.sign(forSig, keyPair, None)

      val sigBytes =
        BigIntegers.asUnsignedByteArray(32, signature.r) ++
          BigIntegers.asUnsignedByteArray(32, signature.s) ++
          Array[Byte]((signature.v - 27).toByte)

      val forSha = sigBytes ++ Array(msg.packetType) ++ encodedData
      val mdc = crypto.kec256(forSha)

      ByteString(mdc ++ sigBytes ++ Array(msg.packetType) ++ encodedData)
    }
  }
  final case class Packet(wire: ByteString) {
    import Packet._

    val mdc: ByteString = wire.take(MdcLength)

    val signature: ECDSASignature = {
      val signatureBytes = wire.drop(MdcLength).take(ECDSASignature.EncodedLength)
      val r = signatureBytes.take(ECDSASignature.RLength)
      val s = signatureBytes.drop(ECDSASignature.RLength).take(ECDSASignature.SLength)
      val v = ByteString(Array[Byte]((signatureBytes(ECDSASignature.EncodedLength - 1) + 27).toByte))

      ECDSASignature(r, s, v)
    }

    val nodeId: ByteString = {
      val msgHash = crypto.kec256(wire.drop(MdcLength + ECDSASignature.EncodedLength))
      ECDSASignature.recoverPublicKey(signature, msgHash, None).map(ByteString.apply)
    }.get

    val packetType: Byte = wire(PacketTypeByteIndex)
    val data: ByteString = wire.drop(DataOffset)
  }
}

/**
 * A cluster singleton, may be mixed with NodeStatus and NodeDiscovery
 */
class NodeDiscoveryService(
    discoveryConfig:        DiscoveryConfig,
    knownNodesStorage:      KnownNodesStorage,
    private var nodeStatus: NodeStatus
) extends Actor with Timers with ActorLogging {
  import NodeDiscoveryService._
  import context.system

  private var nodes: Map[ByteString, Node] = {
    val uris = discoveryConfig.bootstrapNodes.map(new URI(_)) ++
      Set()
    //(if (discoveryConfig.discoveryEnabled) knownNodesStorage.getKnownNodes() else Set.empty)

    uris.map { uri =>
      val node = Node.fromUri(uri)
      node.id -> node
    }.toMap
  }

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
      val socket = sender()

      nodeStatus = nodeStatus.copy(discoveryStatus = ServerStatus.Listening(local))
      context become (udpBehavior(socket) orElse businessBehavior(socket))

      timers.startPeriodicTimer(ScanTask, ScanTick, discoveryConfig.scanInterval)
  }

  def udpBehavior(socket: ActorRef): Receive = {
    case Udp.Received(data, remote) =>
      log.debug(s"DiscoveryService received data from $remote")

      val msgReceivedTry = for {
        packet <- Packet.decodePacket(data)
        message <- Packet.extractMessage(packet)
      } yield MessageReceived(message, remote, packet)

      msgReceivedTry match {
        case Success(msg) =>
          log.debug(s"[disc] Received ${msg.message} from $remote")
          receiveMessage(socket, msg)
        case Failure(ex) =>
          log.debug(s"[disc] Unable to decode discovery packet from ${remote}", ex)
      }

    case Udp.Unbind =>
      log.info(s"[disc] DiscoveryService going to Unbind")
      socket ! Udp.Unbind

    case Udp.Unbound =>
      log.info(s"[disc] DiscoveryService unbound")
      nodeStatus = nodeStatus.copy(discoveryStatus = ServerStatus.NotListening)
      context become idleBehavior
  }

  def businessBehavior(socket: ActorRef): Receive = {
    case ScanTick           => scan(socket)
    case GetDiscoveredNodes => sender() ! DiscoveredNodes(this.nodes.values)
  }

  private def scan(socket: ActorRef) {
    this.nodes.values.toSeq
      .sortBy(_.addTimestamp) // take scanMaxNodes most recent nodes
      .takeRight(discoveryConfig.scanMaxNodes)
      .foreach { node => sendPing(socket, node.id, node.remoteAddress) }
  }

  private def receiveMessage(socket: ActorRef, msg: MessageReceived) {
    msg match {
      case MessageReceived(Ping(_version, _from, _to, _timestamp), from, packet) =>
        val to = Endpoint(packet.nodeId, from.getPort, from.getPort)
        sendMessage(socket, Pong(to, packet.mdc, expirationTimestamp), from)

      case MessageReceived(Pong(_to, _token, _timestamp), from, packet) =>
        val pongNode = Node(packet.nodeId, from, System.currentTimeMillis)
        log.debug(s"[disc] Pong from ${pongNode.uri}")

        if (this.nodes.size < discoveryConfig.nodesLimit) {
          this.nodes += (pongNode.id -> pongNode)
          sendMessage(socket, FindNode(ByteString(nodeStatus.nodeId), expirationTimestamp), from)
        } else {
          val (earliestNode, _) = this.nodes.minBy { case (_, node) => node.addTimestamp }
          this.nodes -= earliestNode
          this.nodes += (pongNode.id -> pongNode)
        }

      case MessageReceived(FindNode(_target, _expires), from, packet) =>
        sendMessage(socket, Neighbours(Nil, expirationTimestamp), from)

      case MessageReceived(Neighbours(_nodes, _expires), from, packet) =>
        val toPings = _nodes.filterNot(n => this.nodes.contains(n.nodeId))
          .take(discoveryConfig.nodesLimit - this.nodes.size)

        log.debug(s"[disc] Received neighbours ${_nodes.size}, will ping new known ${toPings.size}")

        toPings foreach { n =>
          sendPing(socket, n.nodeId, n.endpoint.udpAddress)
        }
    }
  }

  private def sendPing(socket: ActorRef, toNodeId: ByteString, toAddr: InetSocketAddress) {
    nodeStatus.discoveryStatus match {
      case ServerStatus.Listening(address) =>
        val tcpPort = nodeStatus.serverStatus match {
          case ServerStatus.Listening(addr) => addr.getPort
          case _                            => 0
        }

        val from = Endpoint(ByteString(address.getAddress.getAddress), address.getPort, tcpPort)
        val to = Endpoint(toNodeId, toAddr.getPort, toAddr.getPort)
        sendMessage(socket, Ping(Version, from, to, expirationTimestamp), toAddr)

        log.debug(s"Sent ping to ${toAddr}")

      case ServerStatus.NotListening =>
        log.warning("[disc] UDP server not running. Not sending ping message.")
    }
  }

  private def sendMessage[M <: Message](socket: ActorRef, message: M, to: InetSocketAddress)(implicit rlpEnc: RLPEncoder[M]) {
    nodeStatus.discoveryStatus match {
      case ServerStatus.Listening(_) =>
        val packet = Packet.encodePacket(message, nodeStatus.key)
        socket ! Udp.Send(packet, to)

        log.debug(s"Send ${message} to $to")

      case ServerStatus.NotListening =>
        log.warning(s"[disc] UDP server not running. Not sending message $message.")
    }
  }

  private def expirationTimestamp = discoveryConfig.messageExpiration.toSeconds + System.currentTimeMillis / 1000
}
